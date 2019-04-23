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
import org.apache.cassandra.net.async.FrameDecoder.FrameProcessor;
import org.apache.cassandra.net.async.FrameDecoder.IntactFrame;
import org.apache.cassandra.net.async.FrameDecoder.CorruptFrame;
import org.apache.cassandra.net.async.ResourceLimits.Limit;
import org.apache.cassandra.net.async.ResourceLimits.Outcome;
import org.apache.cassandra.utils.ApproximateTime;
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

    private final FrameDecoder decoder;

    private final ConnectionType type;
    private final Channel channel;
    private final InetAddressAndPort self;
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

    private int largeBytesRemaining; // remaining bytes of an in-flight multi-frame large message
    private LargeCoprocessor largeCoprocessor;
    private boolean isSkippingLargeMessage;

    // wait queue handle, non-null if we overrun endpoint or global capacity and request to be resumed once it's released
    private WaitQueue.Ticket ticket = null;

    long receivedCount, receivedBytes;
    int corruptFramesRecovered, corruptFramesUnrecovered;


    private volatile int largeUnconsumedBytes; // unconsumed bytes in all ByteBufs queued up in all coprocessors
    private static final AtomicIntegerFieldUpdater<InboundMessageHandler> largeUnconsumedBytesUpdater =
        AtomicIntegerFieldUpdater.newUpdater(InboundMessageHandler.class, "largeUnconsumedBytes");

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
                          MessageCallbacks callbacks,
                          Function<MessageCallbacks, MessageCallbacks> callbacksTransformer, // for testing purposes only
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

    private boolean processIntactFrame(IntactFrame frame, Limit endpointReserve, Limit globalReserve) throws InvalidLegacyProtocolMagic
    {
        if (frame.isSelfContained)
            return processFrameOfContainedMessages(frame.contents, endpointReserve, globalReserve);
        else if (largeBytesRemaining == 0)
            return processFirstFrameOfLargeMessage(frame, endpointReserve, globalReserve);
        else
            return processSubsequentFrameOfLargeMessage(frame);
    }

    /*
     * Handling of contained messages (not crossing boundaries of a frame) - both small and 'large', for the inbound
     * definition of 'large' (breaching the size threshold for what we are willing to process on event-loop vs.
     * off event-loop).
     */

    private boolean processFrameOfContainedMessages(SharedBytes bytes, Limit endpointReserve, Limit globalReserve)
    throws InvalidLegacyProtocolMagic
    {
        while (bytes.isReadable())
            if (!processOneContainedMessage(bytes, endpointReserve, globalReserve))
                return false;
        return true;
    }

    private boolean processOneContainedMessage(SharedBytes bytes, Limit endpointReserve, Limit globalReserve)
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
            receivedCount++;
            receivedBytes += size;
            bytes.skipBytes(size);
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

        receivedCount++;
        receivedBytes += size;

        boolean callBackOnFailure = serializer.getCallBackOnFailure(buf, version);

        return size < largeThreshold
             ? processContainedSmallMessage(bytes, size, id, expiresAtNanos, callBackOnFailure)
             : processContainedLargeMessage(bytes, size, id, expiresAtNanos, callBackOnFailure);
    }

    private boolean processContainedSmallMessage(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
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
            noSpamLogger.info("{} {} caught while reading a small message", id(), e.getClass().getSimpleName(), e);
            callbacks.onFailedDeserialize(size, id, expiresAtNanos, callBackOnFailure, e);
        }
        catch (IOException e)
        {
            logger.error("{} unexpected IOException caught while reading a small message", id(), e);
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

    private boolean processContainedLargeMessage(SharedBytes bytes, int size, long id, long expiresAtNanos, boolean callBackOnFailure)
    {
        LargeCoprocessor coprocessor = new LargeCoprocessor(size, id, expiresAtNanos, callBackOnFailure);
        boolean isKeepingUp = coprocessor.supplyAndRequestClosure(bytes.sliceAndConsume(size).atomic());
        largeExecutor.execute(coprocessor);
        return isKeepingUp;
    }

    /*
     * Handling of multi-frame large messages
     */

    private boolean processFirstFrameOfLargeMessage(IntactFrame frame, Limit endpointReserve, Limit globalReserve)
    throws InvalidLegacyProtocolMagic
    {
        SharedBytes bytes = frame.contents;
        ByteBuffer buf = bytes.get();
        int size = serializer.messageSize(buf, buf.position(), buf.limit(), version);

        long currentTimeNanos = ApproximateTime.nanoTime();
        long id = serializer.getId(buf, version);
        long createdAtNanos = serializer.getCreatedAtNanos(buf, peer, version);
        long expiresAtNanos = serializer.getExpiresAtNanos(buf, createdAtNanos, version);

        if (expiresAtNanos < currentTimeNanos)
        {
            callbacks.onArrivedExpired(size, id, serializer.getVerb(buf, version), currentTimeNanos - createdAtNanos, TimeUnit.NANOSECONDS);
            int skipped = buf.remaining();
            receivedBytes += skipped;
            bytes.skipBytes(skipped);
            largeBytesRemaining = size - skipped;
            isSkippingLargeMessage = true;
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

        int readableBytes = buf.remaining();
        receivedBytes += readableBytes;

        startCoprocessor(size, id, expiresAtNanos, callBackOnFailure);
        boolean isKeepingUp = largeCoprocessor.supply(bytes.sliceAndConsume(readableBytes).atomic());
        largeBytesRemaining = size - readableBytes;
        return isKeepingUp;
    }

    private boolean processSubsequentFrameOfLargeMessage(IntactFrame frame)
    {
        SharedBytes bytes = frame.contents;
        int frameSize = bytes.readableBytes();

        largeBytesRemaining -= frameSize;
        boolean isLastFrame = largeBytesRemaining == 0;
        if (isLastFrame)
            receivedCount++;
        receivedBytes += frameSize;

        if (isSkippingLargeMessage)
        {
            bytes.skipBytes(frameSize);
            if (isLastFrame)
                isSkippingLargeMessage = false;
            return true;
        }
        else
        {
            boolean isKeepingUp = largeCoprocessor.supply(bytes.sliceAndConsume(frameSize).atomic());
            if (isLastFrame)
                stopCoprocessor();
            return isKeepingUp;
        }
    }

    private void processCorruptSubsequentFrameOfLargeMessage(CorruptFrame frame)
    {
        largeBytesRemaining -= frame.frameSize;
        boolean isLastFrame = largeBytesRemaining == 0;
        if (isLastFrame)
            receivedCount++;

        if (!isSkippingLargeMessage)
            stopCoprocessor();
        isSkippingLargeMessage = !isLastFrame;
    }

    private void processCorruptFrame(CorruptFrame frame) throws InvalidCrc
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
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading messages (corrupted self-contained frame)", id());
        }
        else if (largeBytesRemaining == 0) // first frame of a large message
        {
            corruptFramesUnrecovered++;
            noSpamLogger.error("{} invalid, unrecoverable CRC mismatch detected while reading messages (corrupted first frame of a large message)", id());
            throw new InvalidCrc(frame.readCRC, frame.computedCRC);
        }
        else // subsequent frame of a large message
        {
            processCorruptSubsequentFrameOfLargeMessage(frame);
            corruptFramesRecovered++;
            noSpamLogger.warn("{} invalid, recoverable CRC mismatch detected while reading a large message", id());
        }
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

        try
        {
            if (!isClosed)
                decoder.reactivate();
        }
        catch (Throwable t)
        {
            exceptionCaught(t);
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
            {
                isActive = processSubsequentFrameOfLargeMessage((IntactFrame) frame);
                return isActive && largeBytesRemaining > 0; // continue unless blocked or done with the large message
            }
            else
            {
                processCorruptFrame((CorruptFrame) frame);
                return largeBytesRemaining > 0;
            }
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
        decoder.deactivate();

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
        largeExecutor.execute(largeCoprocessor);
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

    String id()
    {
        return peer + "->" + self + '-' + type + '-' + channel.id().asShortText();
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
                threadName = "Messaging-IN-" + peer + "->" + self + '-' + type + '-' + id;
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
                noSpamLogger.info("{} {} caught while reading a large message", e.getClass().getSimpleName(), id(), e);
                callbacks.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, e);
            }
            catch (IOException e)
            {
                logger.error("{} unexpected IOException caught while reading a large message", id(), e);
                callbacks.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, e);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("{} unexpected exception caught while reading a large message", id(), t);
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
                channel.eventLoop().execute(InboundMessageHandler.this::onCoprocessorCaughtUp);
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

        // TODO: why do we signal here?
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
                    queue.poll();
                    continue;
                }

                if (t.isLive(nanoTime) && !reserveCapacity.tryAllocate(t.bytesRequested))
                {
                    t.reset();
                    break;
                }

                if (null == tasks)
                    tasks = new IdentityHashMap<>();

                queue.poll();

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
                // FIXME: exclude capacity for expired tickets
                long capacity = 0L;
                for (Ticket ticket : tickets)
                    capacity += ticket.bytesRequested;

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
                    reserveCapacity.release(limit.remaining());
                    // TODO: should we re-signal if limit.remaining() > 0?
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
