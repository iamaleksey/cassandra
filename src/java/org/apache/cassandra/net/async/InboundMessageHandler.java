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
import org.apache.cassandra.net.Message.InvalidLegacyProtocolMagic;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.FrameDecoder.Frame;
import org.apache.cassandra.net.async.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.async.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.async.ResourceLimits.Limit;
import org.apache.cassandra.net.async.ResourceLimits.Outcome;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.jctools.queues.MpscLinkedQueue;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.cassandra.net.async.Crc.*;

/**
 * Parses incoming messages as per the 3.0/3.11/4.0 internode messaging protocols.
 */
public final class InboundMessageHandler extends ChannelInboundHandlerAdapter
{
    public interface MessageProcessor
    {
        void process(Message<?> message, int messageSize, MessageCallbacks callbacks);
    }

    public interface OnHandlerClosed
    {
        void call(InboundMessageHandler handler);
    }

    private static final Logger logger = LoggerFactory.getLogger(InboundMessageHandler.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.SECONDS);

    private static final Message.Serializer serializer = Message.serializer;

    private boolean isClosed;
    private boolean isBlocked;

    private final FrameDecoder decoder;

    private final ConnectionType type;
    private final Channel channel;
    private final InetAddressAndPort peer;
    private final int version;

    private final int largeThreshold;
    private final ExecutorService largeExecutor;

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
    private final MessageCallbacks callbacks;
    private final MessageProcessor processor;

    private int largeBytesRemaining; // remainig bytes we need to supply to the coprocessor to deserialize the in-flight large message
    private int skipBytesRemaining;  // remaining bytes we need to skip to get over the expired message

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    long receivedCount, receivedBytes;
    int corruptFramesRecovered, corruptFramesUnrecovered;

    private LargeCoprocessor largeCoprocessor;

    private volatile int largeUnconsumedBytes; // unconsumed bytes in all ByteBufs queued up in all coprocessors
    private static final AtomicIntegerFieldUpdater<InboundMessageHandler> largeUnconsumedBytesUpdater =
        AtomicIntegerFieldUpdater.newUpdater(InboundMessageHandler.class, "largeUnconsumedBytes");

    InboundMessageHandler(FrameDecoder decoder,

                          ConnectionType type,
                          Channel channel,
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
                          MessageCallbacks callbacks,
                          Function<MessageCallbacks, MessageCallbacks> callbacksTransformer, // for testing purposes only
                          MessageProcessor processor)
    {
        this.decoder = decoder;

        this.type = type;
        this.channel = channel;
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
        decoder.resume(this::readFrame);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        throw new IllegalStateException("InboundMessageHandler doesn't expect channelRead() to be invoked");
    }

    private boolean readFrame(Frame frame) throws IOException
    {
        return readFrame(frame, endpointReserveCapacity, globalReserveCapacity, Integer.MAX_VALUE);
    }

    private boolean readFrame(Frame frame, Limit endpointReserve, Limit globalReserve, int count) throws IOException
    {
        return frame instanceof IntactFrame
             ? readIntactFrame((IntactFrame) frame, endpointReserve, globalReserve, count)
             : readCorruptFrame((CorruptFrame) frame);
    }

    private boolean readIntactFrame(IntactFrame frame, Limit endpointReserve, Limit globalReserve, int count) throws InvalidLegacyProtocolMagic
    {
        SharedBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();
        int readableBytes = buf.remaining();

        if (frame.isSelfContained)
        {
            return processMessages(true, frame.contents, endpointReserve, globalReserve, count);
        }
        else if (largeBytesRemaining == 0 && skipBytesRemaining == 0)
        {
            return processMessages(false, frame.contents, endpointReserve, globalReserve, count);
        }
        else if (largeBytesRemaining > 0)
        {
            receivedBytes += readableBytes;
            largeBytesRemaining -= readableBytes;

            boolean isKeepingUp = largeCoprocessor.supply(bytes.sliceAndConsume(readableBytes).atomic());
            if (largeBytesRemaining == 0)
            {
                receivedCount++;
                stopCoprocessor();
            }

            return isKeepingUp;
        }
        else if (skipBytesRemaining > 0)
        {
            receivedBytes += readableBytes;
            skipBytesRemaining -= readableBytes;

            bytes.skipBytes(readableBytes);
            if (skipBytesRemaining == 0)
                receivedCount++;

            return true;
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    private boolean readCorruptFrame(CorruptFrame frame) throws InvalidCrc
    {
        if (!frame.isRecoverable())
        {
            corruptFramesUnrecovered++;
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }

        int frameSize = frame.frameSize;
        receivedBytes += frameSize;

        if (frame.isSelfContained)
        {
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading messages from {} (corrupted self-contained frame)", peer);
        }
        else if (largeBytesRemaining == 0 && skipBytesRemaining == 0)
        {
            corruptFramesUnrecovered++;
            noSpamLogger.error("Invalid, unrecoverable CRC mismatch detected while reading messages from {} (corrupted first frame of a message)", peer);
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else if (largeBytesRemaining > 0)
        {
            stopCoprocessor();
            skipBytesRemaining = largeBytesRemaining - frameSize;
            largeBytesRemaining = 0;
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading a large message from {}", peer);
        }
        else if (skipBytesRemaining > 0)
        {
            skipBytesRemaining -= frameSize;
            if (skipBytesRemaining == 0)
                receivedCount++;
            noSpamLogger.warn("Invalid, recoverable CRC mismatch detected while reading a message from {}", peer);
        }
        else
        {
            throw new IllegalStateException();
        }

        corruptFramesRecovered++;

        return true;
    }

    private boolean processMessages(boolean contained, SharedBytes bytes, Limit endpointReserve, Limit globalReserve, int count)
    throws InvalidLegacyProtocolMagic
    {
        while (!isBlocked && bytes.isReadable() && count-- > 0)
            isBlocked = !processMessage(bytes, contained, endpointReserve, globalReserve);

        return !isBlocked && count > 0;
    }

    private boolean processMessage(SharedBytes bytes, boolean contained, Limit endpointReserve, Limit globalReserve)
    throws InvalidLegacyProtocolMagic
    {
        ByteBuffer buf = bytes.get();
        int size = serializer.messageSize(buf, buf.position(), buf.limit(), version);

        long currentTimeNanos = ApproximateTime.nanoTime();
        long id = serializer.getId(buf, version);
        long createdAtNanos = serializer.getCreatedAtNanos(buf, peer, version);
        long expiresAtNanos = serializer.getExpiresAtNanos(buf, createdAtNanos, version);

        if (expiresAtNanos < currentTimeNanos)
        {
            callbacks.onArrivedExpired(size, id, serializer.getVerb(buf, version), currentTimeNanos - createdAtNanos, TimeUnit.NANOSECONDS);

            int skipped = contained ? size : buf.remaining();
            receivedBytes += skipped;
            bytes.skipBytes(skipped);

            if (contained)
                receivedCount++;
            else
                skipBytesRemaining = size - skipped;

            return true;
        }

        switch (acquireCapacity(endpointReserve, globalReserve, size))
        {
            case INSUFFICIENT_ENDPOINT:
                ticket = endpointWaitQueue.registerAndSignal(this, size, expiresAtNanos);
                return false;
            case INSUFFICIENT_GLOBAL:
                ticket = globalWaitQueue.registerAndSignal(this, size, expiresAtNanos);
                return false;
        }

        boolean callBackOnFailure = serializer.getCallBackOnFailure(buf, version);

        if (contained && size <= largeThreshold)
            return processMessageOnEventLoop(bytes, size, id, expiresAtNanos, callBackOnFailure);
        else if (size <= buf.remaining())
            return processMessageOffEventLoopContained(bytes, size, id, expiresAtNanos, callBackOnFailure);
        else
            return processMessageOffEventLoopUncontained(bytes, size, id, expiresAtNanos, callBackOnFailure);
    }

    private boolean processMessageOnEventLoop(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        receivedCount++;
        receivedBytes += size;

        ByteBuffer buf = bytes.get();
        final int begin = buf.position();
        final int end = buf.limit();
        buf.limit(begin + size); // cap to expected message size

        Message<?> message = null;
        try (DataInputBuffer in = new DataInputBuffer(buf, false))
        {
            message = serializer.deserialize(in, peer, version);
        }
        catch (UnknownTableException | UnknownColumnException e)
        {
            noSpamLogger.info("{} caught while reading a small message from {}", e.getClass().getSimpleName(), peer, e);
            callbacks.onFailedDeserialize(size, id, expiresAtNanos, callBackOnFailure, e);
        }
        catch (IOException e)
        {
            logger.error("Unexpected IOException caught while reading a small message from {}", peer, e);
            callbacks.onFailedDeserialize(size, id, expiresAtNanos, callBackOnFailure, e);
        }
        catch (Throwable t)
        {
            callbacks.onFailedDeserialize(size, id, expiresAtNanos, callBackOnFailure, t);
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

        return true;
    }

    private boolean processMessageOffEventLoopContained(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        receivedCount++;
        receivedBytes += size;

        LargeCoprocessor coprocessor = new LargeCoprocessor(size, id, expiresAtNanos, callBackOnFailure);
        boolean isKeepingUp = coprocessor.supplyAndRequestClosure(bytes.sliceAndConsume(size).atomic());
        largeExecutor.submit(coprocessor);
        return isKeepingUp;
    }

    private boolean processMessageOffEventLoopUncontained(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        int readableBytes = bytes.readableBytes();
        receivedBytes += readableBytes;

        startCoprocessor(size, id, expiresAtNanos, callBackOnFailure);
        boolean isKeepingUp = largeCoprocessor.supply(bytes.sliceAndConsume(readableBytes).atomic());
        largeBytesRemaining = size - readableBytes;
        return isKeepingUp;
    }

    /*
     * Wrap parent message callbacks to ensure capacity is released onProcessed() and onExpired()
     */
    private MessageCallbacks wrapToReleaseCapacity(MessageCallbacks callbacks)
    {
        return new MessageCallbacks()
        {
            @Override
            public void onProcessed(int messageSize)
            {
                releaseCapacity(messageSize);
                callbacks.onProcessed(messageSize);
            }

            @Override
            public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
            {
                releaseCapacity(messageSize);
                callbacks.onExpired(messageSize, id, verb, timeElapsed, unit);
            }

            @Override
            public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
            {
                callbacks.onArrivedExpired(messageSize, id, verb, timeElapsed, unit);
            }

            @Override
            public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
            {
                callbacks.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
            }
        };
    }

    private void onCoprocessorCaughtUp()
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed)
        {
            isBlocked = false;
            decoder.resume(this::readFrame);
        }
    }

    private void onEndpointReserveCapacityRegained(Limit endpointReserve)
    {
        ticket = null;
        onReserveCapacityRegained(endpointReserve, globalReserveCapacity);
    }

    private void onGlobalReserveCapacityRegained(Limit globalReserve)
    {
        ticket = null;
        onReserveCapacityRegained(endpointReserveCapacity, globalReserve);
    }

    private void onReserveCapacityRegained(Limit endpointReserve, Limit globalReserve)
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed)
        {
            isBlocked = false;
            decoder.resume(frame -> readFrame(frame, endpointReserve, globalReserve, 1));
        }
    }

    private void resumeIfNotBlocked()
    {
        assert channel.eventLoop().inEventLoop();

        if (!isClosed && !isBlocked)
            decoder.resume(this::readFrame);
    }

    private EventLoop eventLoop()
    {
        return channel.eventLoop();
    }

    private Outcome acquireCapacity(Limit endpointReserve, Limit globalReserve, int bytes)
    {
        long currentQueueSize = queueSize;

        /*
         * acquireCapacity() is only ever called on the event loop, and as such queueSize is only ever increased
         * on the event loop. If there is enough capacity, we can safely addAndGet() and immediately return.
         */
        if (currentQueueSize + bytes <= queueCapacity)
        {
            queueSizeUpdater.addAndGet(this, bytes);
            return Outcome.SUCCESS;
        }

        long allocatedExcess = min(currentQueueSize + bytes - queueCapacity, bytes);
        Outcome outcome = ResourceLimits.tryAllocate(endpointReserve, globalReserve, allocatedExcess);
        if (outcome != Outcome.SUCCESS)
            return outcome;

        long newQueueSize = queueSizeUpdater.addAndGet(this, bytes);
        long actualExcess = max(0, min(newQueueSize - queueCapacity, bytes));

        if (actualExcess != allocatedExcess) // can be smaller if a release happened since
            ResourceLimits.release(endpointReserve, globalReserve, allocatedExcess - actualExcess);

        return Outcome.SUCCESS;
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
        decoder.stop();

        JVMStabilityInspector.inspectThrowable(cause);

        if (cause instanceof Message.InvalidLegacyProtocolMagic)
            logger.error("Invalid, unrecoverable CRC mismatch detected while reading messages from {} - closing the connection", peer);
        else
            logger.error("Unexpected exception caught while processing inbound messages from {}; terminating connection", peer, cause);

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

        if (null != largeCoprocessor)
            stopCoprocessor();

        if (null != ticket)
        {
            ticket.invalidate();
            ticket = null;
        }

        onClosed.call(this);
    }

    private void startCoprocessor(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        largeCoprocessor = new LargeCoprocessor(messageSize, id, expiresAtNanos, callBackOnFailure);
        largeExecutor.submit(largeCoprocessor);
    }

    private void stopCoprocessor()
    {
        largeCoprocessor.stop();
        largeCoprocessor = null;
    }

    @VisibleForTesting
    public ConnectionType type()
    {
        return type;
    }

    /**
     * This will execute on a thread that isn't a netty event loop.
     */
    private final class LargeCoprocessor implements Runnable
    {
        private final int messageSize;
        private final long id;
        private final long expiresAtNanos;
        private final boolean callBackOnFailure;

        private final AsyncMessagingInputPlus input;

        private final int maxUnconsumedBytes;

        private LargeCoprocessor(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure)
        {
            this.messageSize = messageSize;
            this.id = id;
            this.expiresAtNanos = expiresAtNanos;
            this.callBackOnFailure = callBackOnFailure;

            this.input = new AsyncMessagingInputPlus(this::onBufConsumed);
            /*
             * Allow up to 2x large message threshold bytes of ByteBufs in coprocessors' queues before pausing reads
             * from the channel. Signal the handler to resume reading from the channel once we've consumed enough
             * bytes from the queues to drop below this threshold again.
             */
            maxUnconsumedBytes = largeThreshold * 2;
        }

        public void run()
        {
            String threadName, priorThreadName = null;
            try
            {
                priorThreadName = Thread.currentThread().getName();
                threadName = "Messaging-IN-" + peer + "->" + FBUtilities.getBroadcastAddressAndPort() + '-' + type + '-' + id;
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
            try
            {
                message = serializer.deserialize(input, peer, version);
            }
            catch (UnknownTableException | UnknownColumnException e)
            {
                noSpamLogger.info("{} caught while reading a large message from {}", e.getClass().getSimpleName(), peer, e);
                callbacks.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, e);
            }
            catch (IOException e)
            {
                logger.error("Unexpected IOException caught while reading a large message from {}", peer, e);
                callbacks.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("Unexpected exception caught while reading a large message from {}", peer, t);
                callbacks.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
                channel.pipeline().context(InboundMessageHandler.this).fireExceptionCaught(t);
            }
            finally
            {
                if (null == message)
                    releaseCapacity(messageSize);

                input.close();
            }

            if (null != message)
                processor.process(message, messageSize, callbacks);
        }

        void stop()
        {
            input.requestClosure();
        }

        /*
         * Returns true if coprocessor is keeping up and can accept more input, false if it's fallen behind.
         */
        boolean supply(SharedBytes bytes)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, bytes.readableBytes());
            input.supply(bytes);
            return unconsumed <= maxUnconsumedBytes;
        }

        boolean supplyAndRequestClosure(SharedBytes bytes)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, bytes.readableBytes());
            input.supplyAndRequestClosure(bytes);
            return unconsumed <= maxUnconsumedBytes;
        }

        private void onBufConsumed(int size)
        {
            int unconsumed = largeUnconsumedBytesUpdater.addAndGet(InboundMessageHandler.this, -size);
            int prevUnconsumed = unconsumed + size;

            if (unconsumed <= maxUnconsumedBytes && prevUnconsumed > maxUnconsumedBytes)
                channel.eventLoop().submit(InboundMessageHandler.this::onCoprocessorCaughtUp);
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

        private WaitQueue(Limit reserveCapacity, Kind kind)
        {
            this.reserveCapacity = reserveCapacity;
            this.kind = kind;
        }

        public static WaitQueue endpoint(Limit endpointReserveCapacity)
        {
            return new WaitQueue(endpointReserveCapacity, Kind.ENDPOINT_CAPACITY);
        }

        public static WaitQueue global(Limit globalReserveCapacity)
        {
            return new WaitQueue(globalReserveCapacity, Kind.GLOBAL_CAPACITY);
        }

        private Ticket registerAndSignal(InboundMessageHandler handler, int bytesRequested, long expiresAtNanos)
        {
            Ticket ticket = new Ticket(this, handler, bytesRequested, expiresAtNanos);
            queue.add(ticket);
            signal();
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

        private void schedule()
        {
            Map<EventLoop, ResumeProcessing> tasks = null;

            long nanoTime = ApproximateTime.nanoTime();

            Ticket t;
            while ((t = queue.peek()) != null)
            {
                if (!t.call()) // invalidated
                {
                    if (t != queue.poll())
                        throw new IllegalStateException("Queue peek() != poll()");

                    continue;
                }

                if (t.isLive(nanoTime) && !reserveCapacity.tryAllocate(t.bytesRequested))
                {
                    t.reset();
                    break;
                }

                if (null == tasks)
                    tasks = new IdentityHashMap<>();

                if (t != queue.poll())
                    throw new IllegalStateException("Queue peek() != poll()");

                tasks.computeIfAbsent(t.handler.eventLoop(), e -> new ResumeProcessing()).add(t);
            }

            if (null != tasks)
                tasks.forEach(EventLoop::execute);
        }

        class ResumeProcessing implements Runnable
        {
            List<Ticket> tickets = new ArrayList<>();

            private void add(Ticket ticket)
            {
                tickets.add(ticket);
            }

            public void run()
            {
                long capacity = 0L;
                for (Ticket ticket : tickets)
                    capacity += ticket.bytesRequested;

                Limit limit = new ResourceLimits.Basic(capacity);
                try
                {
                    for (Ticket ticket : tickets)
                        ticket.processOneMessage(limit);
                }
                finally
                {
                    /*
                     * Free up any unused global capacity, if any. Will be non-zero if one or more handlers were closed
                     * when we attempted to run their callback or used more of their personal allowance; or if the first
                     * message in the unprocessed stream has expired in the narrow time window.
                     */
                    reserveCapacity.release(limit.remaining());
                }

                tickets.forEach(Ticket::resumeNormalProcessing);
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

            private void processOneMessage(Limit capacity)
            {
                try
                {
                    if (waitQueue.kind == Kind.ENDPOINT_CAPACITY)
                        handler.onEndpointReserveCapacityRegained(capacity);
                    else
                        handler.onGlobalReserveCapacityRegained(capacity);
                }
                catch (Throwable t)
                {
                    handleException(t);
                }
            }

            private void resumeNormalProcessing()
            {
                try
                {
                    handler.resumeIfNotBlocked();
                }
                catch (Throwable t)
                {
                    handleException(t);
                }
            }

            private void handleException(Throwable t)
            {
                try
                {
                    handler.exceptionCaught(t);
                }
                catch (Throwable e)
                {
                    // no-op
                }
            }

            boolean isInvalidated()
            {
                return state == INVALIDATED;
            }

            boolean isLive(long currentTimeNanos)
            {
                return currentTimeNanos <= expiresAtNanos;
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
