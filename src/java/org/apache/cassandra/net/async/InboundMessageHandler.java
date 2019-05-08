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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.exceptions.UnknownColumnException;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Message.Header;
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
    volatile long queueSize = 0L;
    private static final AtomicLongFieldUpdater<InboundMessageHandler> queueSizeUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandler.class, "queueSize");

    private final Limit endpointReserveCapacity;
    private final WaitQueue endpointWaitQueue;

    private final Limit globalReserveCapacity;
    private final WaitQueue globalWaitQueue;

    private final OnHandlerClosed onClosed;
    private final InboundMessageCallbacks callbacks;
    private final Consumer<Message<?>> consumer;

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    /*
     * Counters
     */
    long corruptFramesRecovered, corruptFramesUnrecovered;
    long receivedCount, receivedBytes;
    long throttledCount, throttledNanos;

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
                          InboundMessageCallbacks callbacks,
                          Consumer<Message<?>> consumer)
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
        this.consumer = consumer;
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

        if (!acquireCapacity(endpointReserve, globalReserve, size, currentTimeNanos, header.expiresAtNanos))
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
            Message<?> m = serializer.deserialize(in, header, version);
            if (in.available() > 0)
                throw new InvalidSerializedSizeException(size, size - in.available());
            message = m;
        }
        catch (UnknownTableException | UnknownColumnException e)
        {
            callbacks.onFailedDeserialize(size, header, e);
            noSpamLogger.info("{} incompatible schema encountered while deserializing a message", id(), e);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t, false);
            callbacks.onFailedDeserialize(size, header, t);
            logger.error("{} unexpected exception caught while deserializing a message", id(), t);
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

        if (!expired && !acquireCapacity(endpointReserve, globalReserve, size, currentTimeNanos, header.expiresAtNanos))
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

    private void onEndpointReserveCapacityRegained(Limit endpointReserve, long elapsedNanos)
    {
        try
        {
            onReserveCapacityRegained(endpointReserve, globalReserveCapacity, elapsedNanos);
        }
        catch (Throwable t)
        {
            exceptionCaught(t);
        }
    }

    private void onGlobalReserveCapacityRegained(Limit globalReserve, long elapsedNanos)
    {
        try
        {
            onReserveCapacityRegained(endpointReserveCapacity, globalReserve, elapsedNanos);
        }
        catch (Throwable t)
        {
            exceptionCaught(t);
        }
    }

    private void onReserveCapacityRegained(Limit endpointReserve, Limit globalReserve, long elapsedNanos) throws IOException
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed)
        {
            ticket = null;
            throttledNanos += elapsedNanos;
            if (processUpToOneMessage(endpointReserve, globalReserve))
                decoder.reactivate();
        }
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

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes, long currentTimeNanos, long expiresAtNanos)
    {
        ResourceLimits.Outcome outcome = acquireCapacity(endpointReserve, globalReserve, bytes);

        if (outcome == ResourceLimits.Outcome.INSUFFICIENT_ENDPOINT)
            ticket = endpointWaitQueue.register(this, bytes, currentTimeNanos, expiresAtNanos);
        else if (outcome == ResourceLimits.Outcome.INSUFFICIENT_GLOBAL)
            ticket = globalWaitQueue.register(this, bytes, currentTimeNanos, expiresAtNanos);

        if (outcome != ResourceLimits.Outcome.SUCCESS)
            throttledCount++;

        return outcome == ResourceLimits.Outcome.SUCCESS;
    }

    private ResourceLimits.Outcome acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes)
    {
        long currentQueueSize = queueSize;

        /*
         * acquireCapacity() is only ever called on the event loop, and as such queueSize is only ever increased
         * on the event loop. If there is enough capacity, we can safely addAndGet() and immediately return.
         */
        if (currentQueueSize + bytes <= queueCapacity)
        {
            queueSizeUpdater.addAndGet(this, bytes);
            return ResourceLimits.Outcome.SUCCESS;
        }

        // we know we don't have enough local queue capacity for the entire message, so we need to borrow some from reserve capacity
        long allocatedExcess = min(currentQueueSize + bytes - queueCapacity, bytes);

        if (!globalReserve.tryAllocate(allocatedExcess))
            return ResourceLimits.Outcome.INSUFFICIENT_GLOBAL;

        if (!endpointReserve.tryAllocate(allocatedExcess))
        {
            globalReserve.release(allocatedExcess);
            globalWaitQueue.signal();
            return ResourceLimits.Outcome.INSUFFICIENT_GLOBAL;
        }

        long newQueueSize = queueSizeUpdater.addAndGet(this, bytes);
        long actualExcess = max(0, min(newQueueSize - queueCapacity, bytes));

        if (actualExcess != allocatedExcess) // can be smaller if a release happened since
        {
            ResourceLimits.release(endpointReserve, globalReserve, allocatedExcess - actualExcess);

            endpointWaitQueue.signal();
            globalWaitQueue.signal();
        }

        return ResourceLimits.Outcome.SUCCESS;
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

    /**
     * Invoked to release capacity for a message that has been fully, successfully processed.
     *
     * Normally no different from invoking {@link #releaseCapacity(int)}, but is necessary for the verifier
     * to be able to delay capacity release for backpressure testing.
     */
    @VisibleForTesting
    protected void releaseProcessedCapacity(int size, Header header)
    {
        releaseCapacity(size);
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

    private EventLoop eventLoop()
    {
        return channel.eventLoop();
    }

    String id(boolean includeReal)
    {
        if (!includeReal)
            return id();

        return SocketFactory.channelId(peer, (InetSocketAddress) channel.remoteAddress(),
                                       self, (InetSocketAddress) channel.localAddress(),
                                       type, channel.id().asShortText());
    }

    String id()
    {
        return SocketFactory.channelId(peer, self, type, channel.id().asShortText());
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
                Message<?> m = serializer.deserialize(input, header, version);
                int remainder = input.remainder();
                if (remainder > 0)
                    throw new InvalidSerializedSizeException(size, size - remainder);
                return m;
            }
            catch (UnknownTableException | UnknownColumnException e)
            {
                callbacks.onFailedDeserialize(size, header, e);
                noSpamLogger.info("{} incompatible schema encountered while deserializing a message", id(), e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t, false);
                callbacks.onFailedDeserialize(size, header, t);
                logger.error("{} unexpected exception caught while deserializing a message", id(), t);
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

            boolean processed = false;
            try
            {
                callbacks.onExecuting(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);

                if (expired)
                {
                    callbacks.onExpired(size, header, currentTimeNanos - header.createdAtNanos, NANOSECONDS);
                    return;
                }

                Message message = provideMessage();
                if (null != message)
                {
                    consumer.accept(message);
                    processed = true;
                    callbacks.onProcessed(size, header);
                }
            }
            finally
            {
                if (processed)
                    releaseProcessedCapacity(size, header);
                else
                    releaseCapacity(size);

                releaseResources();

                callbacks.onExecuted(size, header, ApproximateTime.nanoTime() - currentTimeNanos, NANOSECONDS);
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

        private final ManyToOneConcurrentLinkedQueue<Ticket> queue = new ManyToOneConcurrentLinkedQueue<>();

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

        private Ticket register(InboundMessageHandler handler, int bytesRequested, long registeredAtNanos, long expiresAtNanos)
        {
            Ticket ticket = new Ticket(this, handler, bytesRequested, registeredAtNanos, expiresAtNanos);
            Ticket previous = queue.relaxedPeekLastAndOffer(ticket);
            if (null == previous || !previous.isWaiting())
                signal(); // only signal the queue if this handler is first to register
            return ticket;
        }

        void signal()
        {
            if (queue.relaxedIsEmpty())
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
                    queue.remove();
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

                queue.remove();
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
                        ticket.reactivateHandler(limit);
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
            private final long reigsteredAtNanos;
            private final long expiresAtNanos;

            private Ticket(WaitQueue waitQueue, InboundMessageHandler handler, int bytesRequested, long registeredAtNanos, long expiresAtNanos)
            {
                this.waitQueue = waitQueue;
                this.handler = handler;
                this.bytesRequested = bytesRequested;
                this.reigsteredAtNanos = registeredAtNanos;
                this.expiresAtNanos = expiresAtNanos;
            }

            private void reactivateHandler(Limit capacity)
            {
                long elapsedNanos = ApproximateTime.nanoTime() - reigsteredAtNanos;
                try
                {
                    if (waitQueue.kind == Kind.ENDPOINT_CAPACITY)
                        handler.onEndpointReserveCapacityRegained(capacity, elapsedNanos);
                    else
                        handler.onGlobalReserveCapacityRegained(capacity, elapsedNanos);
                }
                catch (Throwable t)
                {
                    logger.error("{} exception caught while reactivating a handler", handler.id(), t);
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

            private boolean isWaiting()
            {
                return state == WAITING;
            }
        }
    }
}
