/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.net.async;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Message.Header;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.FrameDecoder.Frame;
import org.apache.cassandra.net.async.FrameDecoder.FrameProcessor;
import org.apache.cassandra.net.async.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.async.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.async.ResourceLimits.Limit;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.jctools.queues.MpscLinkedQueue;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.async.Crc.*;

/**
 * Parses incoming messages as per the 3.0/3.11/4.0 internode messaging protocols.
 */
public class InboundMessageHandler extends ChannelInboundHandlerAdapter
{
    public interface OnHandlerClosed
    {
        void call(InboundMessageHandler handler);
    }

    private static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    private static final Message.Serializer serializer = Message.serializer;

    private boolean isClosed;

    private final FrameDecoder decoder;

    private final ConnectionType type;
    private final Channel channel;
    private final InetAddressAndPort self;
    private final InetAddressAndPort peer;
    private final int version;

    private final int largeThreshold;
    private LargeMessage largeMessage;

    private final long queueCapacity;
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long queueSize = 0L;
    private static final AtomicLongFieldUpdater<InboundMessageHandler> queueSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandler.class, "queueSize");

    private final Limit endpointReserveCapacity;
    private final WaitQueue endpointWaitQueue;

    private final Limit globalReserveCapacity;
    private final WaitQueue globalWaitQueue;

    private final OnHandlerClosed onClosed;
    private final InboundMessageCallbacks callbacks;

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    long receivedCount, receivedBytes;
    int corruptFramesRecovered, corruptFramesUnrecovered;

    InboundMessageHandler(FrameDecoder decoder,

                          ConnectionType type,
                          Channel channel,
                          InetAddressAndPort self,
                          InetAddressAndPort peer,
                          int version,
                          int largeThreshold,

                          long queueCapacity,
                          Limit endpointReserveCapacity,
                          Limit globalReserveCapacity,
                          WaitQueue endpointWaitQueue,
                          WaitQueue globalWaitQueue,

                          OnHandlerClosed onClosed,
                          InboundMessageCallbacks callbacks)
    {
        this.decoder = decoder;

        this.type = type;
        this.channel = channel;
        this.self = self;
        this.peer = peer;
        this.version = version;
        this.largeThreshold = largeThreshold;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = endpointReserveCapacity;
        this.endpointWaitQueue = endpointWaitQueue;
        this.globalReserveCapacity = globalReserveCapacity;
        this.globalWaitQueue = globalWaitQueue;

        this.onClosed = onClosed;
        this.callbacks = callbacks;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx)
    {
        decoder.activate(this::processFrame);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        throw new IllegalStateException("InboundMessageHandler doesn't expect channelRead() to be invoked");
    }

    private boolean processFrame(Frame frame) throws IOException
    {
        if (frame instanceof IntactFrame)
            return processIntactFrame((IntactFrame) frame, endpointReserveCapacity, globalReserveCapacity);

        processCorruptFrame((CorruptFrame) frame);
        return true;
    }

    private boolean processIntactFrame(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        if (frame.isSelfContained)
            return processFrameOfContainedMessages(frame.contents, endpointReserve, globalReserve);
        else if (null == largeMessage)
            return processFirstFrameOfLargeMessage(frame, endpointReserve, globalReserve);
        else
            return processSubsequentFrameOfLargeMessage(frame);
    }

    /*
     * Handling of contained messages (not crossing boundaries of a frame) - both small and 'large', for the inbound
     * definition of 'large' (breaching the size threshold for what we are willing to process on event-loop vs.
     * off event-loop).
     */

    private boolean processFrameOfContainedMessages(SharedBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        while (bytes.isReadable())
            if (!processOneContainedMessage(bytes, endpointReserve, globalReserve))
                return false;
        return true;
    }

    private boolean processOneContainedMessage(SharedBytes bytes, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        ByteBuffer buf = bytes.get();

        long currentTimeNanos = ApproximateTime.nanoTime();
        Header header = serializer.extractHeader(buf, peer, currentTimeNanos, version);
        int size = serializer.inferMessageSize(buf, buf.position(), buf.limit(), version);

        if (ApproximateTime.isAfterNanoTime(currentTimeNanos, header.expiresAtNanos))
        {
            callbacks.onArrivedExpired(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
            receivedCount++;
            receivedBytes += size;
            bytes.skipBytes(size);
            return true;
        }

        if (!acquireCapacity(endpointReserve, globalReserve, size, header.expiresAtNanos))
            return false;

        callbacks.onArrived(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
        receivedCount++;
        receivedBytes += size;

        if (size <= largeThreshold)
            processSmallMessage(bytes, size, header);
        else
            processLargeMessage(bytes, size, header);

        return true;
    }

    private void processSmallMessage(SharedBytes bytes, int size, Header header) throws IOException
    {
        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        final int end = buf.limit();
        buf.limit(begin + size); // cap to expected message size

        Message<?> message = null;
        try (DataInputBuffer in = new DataInputBuffer(buf, false))
        {
            message = serializer.deserialize(in, header, version);
        }
        catch (UnknownTableException | UnknownColumnException e)
        {
            noSpamLogger.info("{} {} caught while reading a small message", id(), e.getClass().getSimpleName(), e);
            callbacks.onFailedDeserialize(size, header, e);
        }
        catch (Throwable t)
        {
            callbacks.onFailedDeserialize(size, header, t);
            throw t;
        }
        finally
        {
            if (null == message)
                releaseCapacity(size);

            buf.position(begin + size);
            buf.limit(end);
        }

        if (null != message)
            dispatch(new ProcessSmallMessage(message, size));
    }

    private void processLargeMessage(SharedBytes bytes, int size, Header header)
    {
        new LargeMessage(size, header, bytes.sliceAndConsume(size).atomic()).schedule();
    }

    /*
     * Handling of multi-frame large messages
     */

    private boolean processFirstFrameOfLargeMessage(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws IOException
    {
        SharedBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();

        long currentTimeNanos = ApproximateTime.nanoTime();
        Header header = serializer.extractHeader(buf, peer, currentTimeNanos, version);
        int size = serializer.inferMessageSize(buf, buf.position(), buf.limit(), version);

        boolean expired = ApproximateTime.isAfterNanoTime(currentTimeNanos, header.expiresAtNanos);

        if (!expired && !acquireCapacity(endpointReserve, globalReserve, size, header.expiresAtNanos))
            return false;

        if (expired)
            callbacks.onArrivedExpired(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
        else
            callbacks.onArrived(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);

        receivedBytes += buf.remaining();
        largeMessage = new LargeMessage(size, header, expired);
        largeMessage.supply(frame);
        return true;
    }

    private boolean processSubsequentFrameOfLargeMessage(Frame frame)
    {
        receivedBytes += frame.frameSize;
        if (largeMessage.supply(frame))
        {
            receivedCount++;
            largeMessage = null;
        }
        return true;
    }

    private void processCorruptFrame(CorruptFrame frame) throws InvalidCrc
    {
        if (!frame.isRecoverable())
        {
            corruptFramesUnrecovered++;
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else if (frame.isSelfContained)
        {
            receivedBytes += frame.frameSize;
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading messages (corrupted self-contained frame)", id());
        }
        else if (null == largeMessage) // first frame of a large message
        {
            receivedBytes += frame.frameSize;
            corruptFramesUnrecovered++;
            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages (corrupted first frame of a large message)", id());
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else // subsequent frame of a large message
        {
            processSubsequentFrameOfLargeMessage(frame);
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", id());
        }
    }

    private boolean onEndpointReserveCapacityRegained(Limit endpointReserve) throws IOException
    {
        assert channel.eventLoop().inEventLoop();
        ticket = null;
        return !isClosed && processUpToOneMessage(endpointReserve, globalReserveCapacity);
    }

    private boolean onGlobalReserveCapacityRegained(Limit globalReserve) throws IOException
    {
        assert channel.eventLoop().inEventLoop();
        ticket = null;
        return !isClosed && processUpToOneMessage(endpointReserveCapacity, globalReserve);
    }

    /*
     * Return true if the handler should be reactivated.
     */
    private boolean processUpToOneMessage(Limit endpointReserve, Limit globalReserve) throws IOException
    {
        UpToOneMessageFrameProcessor processor = new UpToOneMessageFrameProcessor(endpointReserve, globalReserve);
        decoder.processBacklog(processor);
        return processor.isActive;
    }

    private class UpToOneMessageFrameProcessor implements FrameProcessor
    {
        private final Limit endpointReserve;
        private final Limit globalReserve;

        boolean isActive = true;
        boolean firstFrame = true;

        private UpToOneMessageFrameProcessor(Limit endpointReserve, Limit globalReserve)
        {
            this.endpointReserve = endpointReserve;
            this.globalReserve = globalReserve;
        }

        @Override
        public boolean process(Frame frame) throws IOException
        {
            if (firstFrame)
            {
                if (!(frame instanceof IntactFrame))
                    throw new IllegalStateException("First backlog frame must be intact");
                firstFrame = false;
                return processFirstFrame((IntactFrame) frame);
            }
            else
            {
                return processSubsequentFrame(frame);
            }
        }

        private boolean processFirstFrame(IntactFrame frame) throws IOException
        {
            if (frame.isSelfContained)
            {
                isActive = processOneContainedMessage(frame.contents, endpointReserve, globalReserve);
                return false; // stop after one message
            }
            else
            {
                isActive = processFirstFrameOfLargeMessage(frame, endpointReserve, globalReserve);
                return isActive; // continue unless fallen behind coprocessor or ran out of reserve capacity again
            }
        }

        private boolean processSubsequentFrame(Frame frame) throws IOException
        {
            if (frame instanceof IntactFrame)
                processSubsequentFrameOfLargeMessage(frame);
            else
                processCorruptFrame((CorruptFrame) frame); // TODO: can almost be folded into ^

            return largeMessage != null; // continue until done with the large message
        }
    }

    private void resume() throws IOException
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed)
            decoder.reactivate();
    }

    private EventLoop eventLoop()
    {
        return channel.eventLoop();
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes, long expiresAtNanos)
    {
        long currentQueueSize = queueSize;

        /*
         * acquireCapacity() is only ever called on the event loop, and as such queueSize is only ever increased
         * on the event loop. If there is enough capacity, we can safely addAndGet() and immediately return.
         */
        if (currentQueueSize + bytes <= queueCapacity)
        {
            queueSizeUpdater.addAndGet(this, bytes);
            return true;
        }

        long allocatedExcess = min(currentQueueSize + bytes - queueCapacity, bytes);

        switch (ResourceLimits.tryAllocate(endpointReserve, globalReserve, allocatedExcess))
        {
            case INSUFFICIENT_ENDPOINT:
                ticket = endpointWaitQueue.register(this, bytes, expiresAtNanos);
                return false;
            case INSUFFICIENT_GLOBAL:
                ticket = globalWaitQueue.register(this, bytes, expiresAtNanos);
                return false;
        }

        long newQueueSize = queueSizeUpdater.addAndGet(this, bytes);
        long actualExcess = max(0, min(newQueueSize - queueCapacity, bytes));

        if (actualExcess != allocatedExcess) // can be smaller if a release happened since
            ResourceLimits.release(endpointReserve, globalReserve, allocatedExcess - actualExcess);

        return true;
    }

    @VisibleForTesting
    protected void releaseCapacity(int bytes)
    {
        long oldQueueSize = queueSizeUpdater.getAndAdd(this, -bytes);
        if (oldQueueSize > queueCapacity)
        {
            long excess = min(oldQueueSize - queueCapacity, bytes);
            ResourceLimits.release(endpointReserveCapacity, globalReserveCapacity, excess);

            endpointWaitQueue.signal();
            globalWaitQueue.signal();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        exceptionCaught(cause);
    }

    private void exceptionCaught(Throwable cause)
    {
        decoder.discard();

        JVMStabilityInspector.inspectThrowable(cause, false);

        if (cause instanceof Message.InvalidLegacyProtocolMagic)
            logger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages - closing the connection", id());
        else
            logger.error("{} unexpected exception caught while processing inbound messages; terminating connection", id(), cause);

        channel.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
    {
        close();
    }

    /*
     * Clean up after ourselves
     */
    private void close()
    {
        isClosed = true;

        if (null != largeMessage)
        {
            largeMessage.abort();
            largeMessage = null;
        }

        if (null != ticket)
        {
            ticket.invalidate();
            ticket = null;
        }

        onClosed.call(this);
    }

    @VisibleForTesting
    public ConnectionType type()
    {
        return type;
    }

    String id()
    {
        return peer + "->" + self + '-' + type + '-' + channel.id().asShortText();
    }

    private class LargeMessage
    {
        private final int size;
        private final Header header;

        private final List<SharedBytes> buffers = new ArrayList<>();
        private int received;
        private boolean isSkipping;

        private LargeMessage(int size, Header header, boolean isSkipping)
        {
            this.size = size;
            this.header = header;
            this.isSkipping = isSkipping;
        }

        private LargeMessage(int size, Header header, SharedBytes bytes)
        {
            this(size, header, false);
            buffers.add(bytes);
        }

        private void schedule()
        {
            dispatch(new ProcessLargeMessage(this));
        }

        private void abort()
        {
            if (!isSkipping)
            {
                releaseCapacity(size);
                releaseBuffers();
                isSkipping = true;
            }
        }

        /**
         * Return true if this was the last frame of the large message.
         */
        private boolean supply(Frame frame)
        {
            received += frame.frameSize;

            if (frame instanceof IntactFrame)
                onIntactFrame((IntactFrame) frame);
            else
                onCorruptFrame((CorruptFrame) frame);

            return size == received;
        }

        private void onIntactFrame(IntactFrame frame)
        {
            /*
             * Verify that the message is still fresh and is worth deserializing; if not, release the buffers,
             * release capacity, and switch to skipping.
             */
            long currentTimeNanos = ApproximateTime.nanoTime();
            if (!isSkipping && ApproximateTime.isAfterNanoTime(currentTimeNanos, header.expiresAtNanos))
            {
                try
                {
                    callbacks.onArrivedExpired(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
                }
                finally
                {
                    releaseCapacity(size);
                    releaseBuffers();
                }
                isSkipping = true;
            }

            if (isSkipping)
            {
                frame.contents.skipBytes(frame.frameSize);
            }
            else
            {
                buffers.add(frame.contents.sliceAndConsume(frame.frameSize).atomic());
                if (received == size)
                    schedule();
            }
        }

        private void onCorruptFrame(CorruptFrame frame)
        {
            if (isSkipping)
                return;

            try
            {
                callbacks.onFailedDeserialize(size, header, new InvalidCrc(frame.readCRC, frame.computedCRC));
            }
            finally
            {
                releaseCapacity(size);
                releaseBuffers();
            }
            isSkipping = true;
        }

        private Message deserialize()
        {
            try (ChunkedInputPlus input = ChunkedInputPlus.of(buffers))
            {
                return serializer.deserialize(input, header, version);
            }
            catch (UnknownTableException | UnknownColumnException e)
            {
                noSpamLogger.info("{} {} caught while reading a large message", e.getClass().getSimpleName(), id(), e);
                callbacks.onFailedDeserialize(size, header, e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t, false);
                logger.error("{} unexpected exception caught while reading a large message", id(), t);
                callbacks.onFailedDeserialize(size, header, t);
                channel.pipeline().context(InboundMessageHandler.this).fireExceptionCaught(t);
            }
            finally
            {
                buffers.clear(); // closing the input will have ensured that the buffers were released no matter what
            }

            return null;
        }

        private void releaseBuffers()
        {
            buffers.forEach(SharedBytes::release);
            buffers.clear();
        }
    }

    /*
     * Submit a {@link ProcessMessage} task to the appropriate Stage for the Verb.
     */
    private void dispatch(ProcessMessage task)
    {
        Header header = task.header();

        TraceState state = Tracing.instance.initializeFromMessage(header);
        if (state != null) state.trace("{} message received from {}", header.verb, header.from);

        callbacks.onDispatched(task.size, header);
        StageManager.getStage(header.verb.stage).execute(task, ExecutorLocals.create(state));
    }

    /*
     * Actually handle the message. Executes on the appropriate Stage for the Verb.
     */
    private abstract class ProcessMessage implements Runnable
    {
        protected final int size;

        ProcessMessage(int size)
        {
            this.size = size;
        }

        @Override
        public void run()
        {
            Header header = header();
            long currentTimeNanos = ApproximateTime.nanoTime();
            boolean expired = ApproximateTime.isAfterNanoTime(currentTimeNanos, header.expiresAtNanos);

            try
            {
                callbacks.onExecuting(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);

                if (!expired)
                {
                    Message message = provideMessage();
                    if (null != message && MessagingService.instance().messageSink.allowInbound(message))
                    {
                        callbacks.onProcessing(size, message);
                        process(message);
                    }
                }
            }
            finally
            {
                releaseResources();

                if (expired)
                    callbacks.onExpired(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
                else
                    callbacks.onProcessed(size, header);
            }
        }

        @SuppressWarnings("unchecked")
        private void process(Message message)
        {
            try
            {
                header().verb.handler().doVerb(message);
            }
            catch (TombstoneOverwhelmingException | IndexNotAvailableException e)
            {
                maybeSendFailureResponse(e);
                noSpamLogger.error(e.getMessage());
            }
            catch (IOException e)
            {
                maybeSendFailureResponse(e);
                throw new RuntimeException(e);
            }
            catch (Throwable t)
            {
                maybeSendFailureResponse(t);
                throw t;
            }
        }

        private void maybeSendFailureResponse(Throwable t)
        {
            Header header = header();

            if (header.callBackOnFailure())
            {
                RequestFailureReason reason = RequestFailureReason.forException(t);
                Message response = Message.failureResponse(header.id, header.expiresAtNanos, reason);
                MessagingService.instance().sendResponse(response, header.from);
            }
        }

        abstract Header header();
        abstract Message provideMessage();
        abstract void releaseResources();
    }

    private class ProcessSmallMessage extends ProcessMessage
    {
        private final Message message;

        ProcessSmallMessage(Message message, int size)
        {
            super(size);
            this.message = message;
        }

        @Override
        Header header()
        {
            return message.header;
        }

        @Override
        Message provideMessage()
        {
            return message;
        }

        @Override
        void releaseResources()
        {
            releaseCapacity(size);
        }
    }

    private class ProcessLargeMessage extends ProcessMessage
    {
        private final LargeMessage message;

        ProcessLargeMessage(LargeMessage message)
        {
            super(message.size);
            this.message = message;
        }

        @Override
        Header header()
        {
            return message.header;
        }

        @Override
        Message provideMessage()
        {
            return message.deserialize();
        }

        @Override
        void releaseResources()
        {
            releaseCapacity(size);
            message.releaseBuffers(); // releases buffers if they haven't been yet (by deserialize() call)
        }
    }

    /*
     * Backpressure handling
     */

    public static final class WaitQueue
    {
        enum Kind { ENDPOINT_CAPACITY, GLOBAL_CAPACITY }

        /*
         * Callback scheduler states
         */
        private static final int NOT_RUNNING = 0;
        @SuppressWarnings("unused")
        private static final int RUNNING     = 1;
        private static final int RUN_AGAIN   = 2;

        private volatile int scheduled;
        private static final AtomicIntegerFieldUpdater<WaitQueue> scheduledUpdater =
            AtomicIntegerFieldUpdater.newUpdater(WaitQueue.class, "scheduled");

        private final Kind kind;
        private final Limit reserveCapacity;

        private final MpscLinkedQueue<Ticket> queue = MpscLinkedQueue.newMpscLinkedQueue();

        private WaitQueue(Kind kind, Limit reserveCapacity)
        {
            this.kind = kind;
            this.reserveCapacity = reserveCapacity;
        }

        public static WaitQueue endpoint(Limit endpointReserveCapacity)
        {
            return new WaitQueue(Kind.ENDPOINT_CAPACITY, endpointReserveCapacity);
        }

        public static WaitQueue global(Limit globalReserveCapacity)
        {
            return new WaitQueue(Kind.GLOBAL_CAPACITY, globalReserveCapacity);
        }

        private Ticket register(InboundMessageHandler handler, int bytesRequested, long expiresAtNanos)
        {
            Ticket ticket = new Ticket(this, handler, bytesRequested, expiresAtNanos);
            queue.add(ticket);
            signal(); // TODO: *conditionally* signal upon registering
            return ticket;
        }

        void signal()
        {
            if (queue.isEmpty())
                return;

            if (NOT_RUNNING == scheduledUpdater.getAndUpdate(this, i -> Integer.min(RUN_AGAIN, i + 1)))
            {
                do
                {
                    schedule();
                }
                while (RUN_AGAIN == scheduledUpdater.getAndDecrement(this));
            }
        }

        /*
         * TODO: traverse the entire queue to unblock handlers that have expired tickets, and also remove any closed handlers
         */
        private void schedule()
        {
            Map<EventLoop, ResumeProcessing> tasks = null;

            long currentTimeNanos = ApproximateTime.nanoTime();

            Ticket t;
            while ((t = queue.peek()) != null)
            {
                if (!t.call()) // invalidated
                {
                    queue.poll();
                    continue;
                }

                boolean isLive = t.isLive(currentTimeNanos);
                if (isLive && !reserveCapacity.tryAllocate(t.bytesRequested))
                {
                    t.reset();
                    break;
                }

                if (null == tasks)
                    tasks = new IdentityHashMap<>();

                queue.poll();
                tasks.computeIfAbsent(t.handler.eventLoop(), e -> new ResumeProcessing()).add(t, isLive);
            }

            if (null != tasks)
                tasks.forEach(EventLoop::execute);
        }

        class ResumeProcessing implements Runnable
        {
            List<Ticket> tickets = new ArrayList<>();
            long capacity = 0L;

            private void add(Ticket ticket, boolean isLive)
            {
                tickets.add(ticket);

                if (isLive)
                    capacity += ticket.bytesRequested;
            }

            public void run()
            {
                Limit limit = new ResourceLimits.Basic(capacity);
                try
                {
                    for (Ticket ticket : tickets)
                        ticket.reactivate(limit);
                }
                finally
                {
                    /*
                     * Free up any unused global capacity, if any. Will be non-zero if one or more handlers were closed
                     * when we attempted to run their callback or used more of their personal allowance; or if the first
                     * message in the unprocessed stream has expired in the narrow time window.
                     */
                    long remaining = limit.remaining();
                    if (remaining > 0)
                    {
                        reserveCapacity.release(remaining);
                        signal();
                    }
                }
            }
        }

        static final class Ticket
        {
            private static final int WAITING     = 0;
            private static final int CALLED      = 1;
            private static final int INVALIDATED = 2;

            private volatile int state;
            private static final AtomicIntegerFieldUpdater<Ticket> stateUpdater =
                AtomicIntegerFieldUpdater.newUpdater(Ticket.class, "state");

            private final WaitQueue waitQueue;
            private final InboundMessageHandler handler;
            private final int bytesRequested;
            private final long expiresAtNanos;

            private Ticket(WaitQueue waitQueue, InboundMessageHandler handler, int bytesRequested, long expiresAtNanos)
            {
                this.waitQueue = waitQueue;
                this.handler = handler;
                this.bytesRequested = bytesRequested;
                this.expiresAtNanos = expiresAtNanos;
            }

            private void reactivate(Limit capacity)
            {
                try
                {
                    boolean isActive = waitQueue.kind == Kind.ENDPOINT_CAPACITY
                                     ? handler.onEndpointReserveCapacityRegained(capacity)
                                     : handler.onGlobalReserveCapacityRegained(capacity);

                    if (isActive)
                        handler.resume();
                }
                catch (Throwable t)
                {
                    try
                    {
                        handler.exceptionCaught(t);
                    }
                    catch (Throwable e)
                    {
                        logger.error("{} exception caught while invoking InboundMessageHandler's exceptionCaught()", handler.id(), e);
                    }
                }
            }

            boolean isInvalidated()
            {
                return state == INVALIDATED;
            }

            boolean isLive(long currentTimeNanos)
            {
                return !ApproximateTime.isAfterNanoTime(currentTimeNanos, expiresAtNanos);
            }

            void invalidate()
            {
                if (stateUpdater.compareAndSet(this, WAITING, INVALIDATED))
                    waitQueue.signal();
            }

            private boolean call()
            {
                return stateUpdater.compareAndSet(this, WAITING, CALLED);
            }

            private void reset()
            {
                state = WAITING;
            }
        }
    }
}
