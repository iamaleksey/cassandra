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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Message.Header;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.FrameDecoder.Frame;
import org.apache.cassandra.net.async.FrameDecoder.FrameProcessor;
import org.apache.cassandra.net.async.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.async.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.async.ResourceLimits.Limit;
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
public final class InboundMessageHandler extends ChannelInboundHandlerAdapter
{
    public interface MessageProcessor
    {
        void process(Message<?> message, int messageSize, InboundMessageCallbacks callbacks);
    }

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
    private final ExecutorService largeExecutor;
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
    private final MessageProcessor processor;

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
                          ExecutorService largeExecutor,

                          long queueCapacity,
                          Limit endpointReserveCapacity,
                          Limit globalReserveCapacity,
                          WaitQueue endpointWaitQueue,
                          WaitQueue globalWaitQueue,

                          OnHandlerClosed onClosed,
                          InboundMessageCallbacks callbacks,
                          Function<InboundMessageCallbacks, InboundMessageCallbacks> callbacksTransformer, // for testing purposes only
                          MessageProcessor processor)
    {
        this.decoder = decoder;

        this.type = type;
        this.channel = channel;
        this.self = self;
        this.peer = peer;
        this.version = version;

        this.largeThreshold = largeThreshold;
        this.largeExecutor = largeExecutor;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = endpointReserveCapacity;
        this.endpointWaitQueue = endpointWaitQueue;
        this.globalReserveCapacity = globalReserveCapacity;
        this.globalWaitQueue = globalWaitQueue;

        this.onClosed = onClosed;
        this.callbacks = callbacksTransformer.apply(wrapToReleaseCapacity(callbacks));
        this.processor = processor;
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

        callbacks.onArrived(header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
        receivedCount++;
        receivedBytes += size;

        if (size <= largeThreshold)
            processContainedSmallMessage(bytes, size, header);
        else
            processContainedLargeMessage(bytes, size, header);

        return true;
    }

    private void processContainedSmallMessage(SharedBytes bytes, int size, Header header)
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
        catch (IOException e)
        {
            // TODO: is there really anything particularly special about an IOException vs other kind of exception?
            logger.error("{} unexpected IOException caught while reading a small message", id(), e);
            callbacks.onFailedDeserialize(size, header, e);
        }
        catch (Throwable t)
        {
            callbacks.onFailedDeserialize(size, header, t);
            throw t;
        }
        finally
        {
            buf.position(begin + size);
            buf.limit(end);

            if (null == message)
                releaseCapacity(size);
        }

        if (null != message)
            processor.process(message, size, callbacks);
    }

    private void processContainedLargeMessage(SharedBytes bytes, int size, Header header)
    {
        new LargeMessage(size, header, bytes.sliceAndConsume(size).atomic()).scheduleCoprocessor();
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

        if (ApproximateTime.isAfterNanoTime(currentTimeNanos, header.expiresAtNanos))
        {
            callbacks.onArrivedExpired(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
            receivedBytes += buf.remaining();
            largeMessage = new LargeMessage(size, header, true);
            largeMessage.supply(frame);
            return true;
        }

        if (!acquireCapacity(endpointReserve, globalReserve, size, header.expiresAtNanos))
            return false;

        callbacks.onArrived(header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
        receivedBytes += buf.remaining();
        largeMessage = new LargeMessage(size, header, false);
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

        int frameSize = frame.frameSize;

        if (frame.isSelfContained)
        {
            receivedBytes += frameSize;
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading messages (corrupted self-contained frame)", id());
        }
        else if (null == largeMessage) // first frame of a large message
        {
            receivedBytes += frameSize;
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

    /*
     * Wrap parent message callbacks to ensure capacity is released onProcessed() and onExpired()
     */
    private InboundMessageCallbacks wrapToReleaseCapacity(InboundMessageCallbacks callbacks)
    {
        return new InboundMessageCallbacks()
        {
            public void onArrived(Header header, long timeElapsed, TimeUnit unit)
            {
                callbacks.onArrived(header, timeElapsed, unit);
            }

            @Override
            public void onProcessed(int messageSize)
            {
                releaseCapacity(messageSize);
                callbacks.onProcessed(messageSize);
            }

            @Override
            public void onExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                releaseCapacity(messageSize);
                callbacks.onExpired(messageSize, header, timeElapsed, unit);
            }

            @Override
            public void onArrivedExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                callbacks.onArrivedExpired(messageSize, header, timeElapsed, unit);
            }

            @Override
            public void onFailedDeserialize(int messageSize, Header header, Throwable t)
            {
                callbacks.onFailedDeserialize(messageSize, header, t);
            }
        };
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

    private void releaseCapacity(int bytes)
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

        JVMStabilityInspector.inspectThrowable(cause);

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

        private List<SharedBytes> buffers;
        private int bytesReceived;
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
            buffers = Collections.singletonList(bytes);
        }

        /**
         * Return true if this was the last frame of the large message.
         * TODO: can be cleaned up further
         *
         */
        private boolean supply(Frame frame)
        {
            bytesReceived += frame.frameSize;

            if (frame instanceof IntactFrame)
                onIntactFrame((IntactFrame) frame);
            else
                onCorruptFrame((CorruptFrame) frame);

            return size == bytesReceived;
        }

        // TODO: clean up further?
        private void onIntactFrame(IntactFrame frame)
        {
            /*
             * Verify that the message is still fresh and is worth deserializing; if not, release the buffers,
             * release capacity, and switch to skipping.
             */
            long currentTimeNanos = ApproximateTime.nanoTime();
            if (!isSkipping && ApproximateTime.isAfterNanoTime(currentTimeNanos, header.expiresAtNanos))
            {
                releaseBuffers();
                isSkipping = true;

                try
                {
                    callbacks.onArrivedExpired(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
                }
                finally
                {
                    releaseCapacity(size);
                }
            }

            if (isSkipping)
                frame.contents.skipBytes(frame.frameSize);
            else
                add(frame.contents.sliceAndConsume(frame.frameSize).atomic());

            if (bytesReceived == size && !isSkipping)
                scheduleCoprocessor();
        }

        // TODO: clean up further?
        private void onCorruptFrame(CorruptFrame frame)
        {
            if (!isSkipping)
            {
                releaseBuffers();
                isSkipping = true;

                try
                {
                    callbacks.onFailedDeserialize(size, header, new InvalidCrc(frame.readCRC, frame.computedCRC));
                }
                finally
                {
                    releaseCapacity(size);
                }
            }
        }

        private void releaseBuffers()
        {
            if (buffers != null)
            {
                buffers.forEach(SharedBytes::release);
                buffers = null;
            }
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

        private void add(SharedBytes buffer)
        {
            if (null == buffers)
                buffers = new ArrayList<>();
            buffers.add(buffer);
        }

        private void scheduleCoprocessor()
        {
            largeExecutor.execute(new LargeCoprocessor(this));
        }
    }

    /**
     * This will execute on a thread that isn't a netty event loop.
     */
    private final class LargeCoprocessor implements Runnable
    {
        private final LargeMessage msg;

        private LargeCoprocessor(LargeMessage msg)
        {
            this.msg = msg;
        }

        public void run()
        {
            String threadName, priorThreadName = null;
            try
            {
                priorThreadName = Thread.currentThread().getName();
                threadName = "Messaging-IN-" + peer + "->" + self + '-' + type + '-' + msg.header.id;
                Thread.currentThread().setName(threadName);

                processLargeMessage();
            }
            finally
            {
                if (null != priorThreadName)
                    Thread.currentThread().setName(priorThreadName);
            }
        }

        private void processLargeMessage()
        {
            Message<?> message = null;
            Header header = msg.header;
            long currentTimeNanos = ApproximateTime.nanoTime();

            try (ChunkedInputPlus input = ChunkedInputPlus.of(msg.buffers))
            {
                if (ApproximateTime.isAfterNanoTime(currentTimeNanos, header.expiresAtNanos))
                    callbacks.onArrivedExpired(msg.size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
                else
                    message = serializer.deserialize(input, header, version);
            }
            catch (UnknownTableException | UnknownColumnException e)
            {
                noSpamLogger.info("{} {} caught while reading a large message", e.getClass().getSimpleName(), id(), e);
                callbacks.onFailedDeserialize(msg.size, header, e);
            }
            catch (IOException e)
            {
                logger.error("{} unexpected IOException caught while reading a large message", id(), e);
                callbacks.onFailedDeserialize(msg.size, header, e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t, false);
                logger.error("{} unexpected exception caught while reading a large message", id(), t);
                callbacks.onFailedDeserialize(msg.size, header, t);
                channel.pipeline().context(InboundMessageHandler.this).fireExceptionCaught(t);
            }
            finally
            {
                if (null == message)
                    releaseCapacity(msg.size);
            }

            if (null != message)
                processor.process(message, msg.size, callbacks);
        }
    }

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

            long nanoTime = ApproximateTime.nanoTime();

            Ticket t;
            while ((t = queue.peek()) != null)
            {
                if (!t.call()) // invalidated
                {
                    queue.poll();
                    continue;
                }

                boolean isLive = t.isLive(nanoTime);
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
