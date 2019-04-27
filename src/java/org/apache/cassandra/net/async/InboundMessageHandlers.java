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

import java.util.Collection;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.google.common.base.Functions;

import io.netty.channel.Channel;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeInboundMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Message.Header;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.InboundMessageHandler.MessageProcessor;
import org.apache.cassandra.utils.ApproximateTime;

public final class InboundMessageHandlers
{
    private final InetAddressAndPort self;
    private final InetAddressAndPort peer;

    private final int queueCapacity;
    private final ResourceLimits.Limit endpointReserveCapacity;
    private final ResourceLimits.Limit globalReserveCapacity;

    private final InboundMessageHandler.WaitQueue endpointWaitQueue;
    private final InboundMessageHandler.WaitQueue globalWaitQueue;

    private final InternodeInboundMetrics metrics;

    // allow tests to wrap or replace InboundMessageCallbacks used by individual handlers
    private final Function<InboundMessageCallbacks, InboundMessageCallbacks> callbacksTransformer;

    private final InboundCounters urgentCounters = new InboundCounters();
    private final InboundCounters smallCounters  = new InboundCounters();
    private final InboundCounters largeCounters  = new InboundCounters();
    private final InboundCounters legacyCounters = new InboundCounters();

    private final InboundMessageCallbacks urgentCallbacks;
    private final InboundMessageCallbacks smallCallbacks;
    private final InboundMessageCallbacks largeCallbacks;
    private final InboundMessageCallbacks legacyCallbacks;

    private final MessageProcessor urgentProcessor;
    private final MessageProcessor smallProcessor;
    private final MessageProcessor largeProcessor;
    private final MessageProcessor legacyProcessor;

    private final Collection<InboundMessageHandler> handlers = new CopyOnWriteArrayList<>();

    public InboundMessageHandlers(InetAddressAndPort self,
                                  InetAddressAndPort peer,
                                  int queueCapacity,
                                  long endpointReserveCapacity,
                                  ResourceLimits.Limit globalReserveCapacity,
                                  InboundMessageHandler.WaitQueue globalWaitQueue,
                                  MessageProcessor messageProcessor)
    {
        this(self, peer, queueCapacity, endpointReserveCapacity, globalReserveCapacity, globalWaitQueue, messageProcessor, Functions.identity());
    }

    public InboundMessageHandlers(InetAddressAndPort self,
                                  InetAddressAndPort peer,
                                  int queueCapacity,
                                  long endpointReserveCapacity,
                                  ResourceLimits.Limit globalReserveCapacity,
                                  InboundMessageHandler.WaitQueue globalWaitQueue,
                                  MessageProcessor messageProcessor,
                                  Function<InboundMessageCallbacks, InboundMessageCallbacks> callbacksTransformer)
    {
        this.self = self;
        this.peer = peer;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = new ResourceLimits.Concurrent(endpointReserveCapacity);
        this.globalReserveCapacity = globalReserveCapacity;

        this.endpointWaitQueue = InboundMessageHandler.WaitQueue.endpoint(this.endpointReserveCapacity);
        this.globalWaitQueue = globalWaitQueue;

        this.callbacksTransformer = callbacksTransformer;

        urgentCallbacks = makeMessageCallbacks(peer, urgentCounters);
        smallCallbacks  = makeMessageCallbacks(peer, smallCounters);
        largeCallbacks  = makeMessageCallbacks(peer, largeCounters);
        legacyCallbacks = makeMessageCallbacks(peer, legacyCounters);

        urgentProcessor = wrapProcessorForMetrics(messageProcessor, urgentCounters);
        smallProcessor  = wrapProcessorForMetrics(messageProcessor, smallCounters);
        largeProcessor  = wrapProcessorForMetrics(messageProcessor, largeCounters);
        legacyProcessor = wrapProcessorForMetrics(messageProcessor, legacyCounters);

        metrics = new InternodeInboundMetrics(peer, this);
    }

    InboundMessageHandler createHandler(FrameDecoder frameDecoder, ExecutorService synchronousWorkExecutor, ConnectionType type, Channel channel, int version)
    {
        InboundMessageHandler handler =
            new InboundMessageHandler(frameDecoder,

                                      type,
                                      channel,
                                      self,
                                      peer,
                                      version,

                                      OutboundConnections.LARGE_MESSAGE_THRESHOLD,
                                      synchronousWorkExecutor,

                                      queueCapacity,
                                      endpointReserveCapacity,
                                      globalReserveCapacity,
                                      endpointWaitQueue,
                                      globalWaitQueue,

                                      this::onHandlerClosed,
                                      callbacksFor(type),
                                      callbacksTransformer,
                                      processorFor(type));
        handlers.add(handler);
        return handler;
    }

    public void releaseMetrics()
    {
        metrics.release();
    }

    private void onHandlerClosed(InboundMessageHandler handler)
    {
        handlers.remove(handler);
        absorbCounters(handler);
    }

    /*
     * Wrap provided MessageProcessor to allow pending message metrics to be maintained.
     */

    private MessageProcessor processorFor(ConnectionType type)
    {
        switch (type)
        {
            case URGENT_MESSAGES: return urgentProcessor;
            case  SMALL_MESSAGES: return smallProcessor;
            case  LARGE_MESSAGES: return largeProcessor;
            case LEGACY_MESSAGES: return legacyProcessor;
        }

        throw new IllegalArgumentException();
    }

    private static MessageProcessor wrapProcessorForMetrics(MessageProcessor processor, InboundCounters counters)
    {
        return (message, messageSize, callbacks) ->
        {
            counters.addPending(messageSize);
            processor.process(message, messageSize, callbacks);
        };
    }

    /*
     * Message callbacks
     */

    private InboundMessageCallbacks callbacksFor(ConnectionType type)
    {
        switch (type)
        {
            case URGENT_MESSAGES: return urgentCallbacks;
            case  SMALL_MESSAGES: return smallCallbacks;
            case  LARGE_MESSAGES: return largeCallbacks;
            case LEGACY_MESSAGES: return legacyCallbacks;
        }

        throw new IllegalArgumentException();
    }

    private static InboundMessageCallbacks makeMessageCallbacks(InetAddressAndPort peer, InboundCounters counters)
    {
        final MessagingMetrics.Updater latencyUpdater = MessagingService.instance().metrics.getForPeer(peer);
        return new InboundMessageCallbacks()
        {
            @Override
            public void onArrived(Header header, long timeElapsed, TimeUnit unit)
            {
                // do not log latency if we are within error bars of zero
                if (timeElapsed > ApproximateTime.almostNowPrecision(unit))
                    latencyUpdater.addTimeTaken(timeElapsed, unit);
            }

            @Override
            public void onProcessed(int messageSize)
            {
                counters.removePending(messageSize);
                counters.addProcessed(messageSize);
            }

            @Override
            public void onExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                counters.removePending(messageSize);
                counters.addExpired(messageSize);

                MessagingService.instance().droppedMessages.incrementWithLatency(header.verb, timeElapsed, unit);
            }

            @Override
            public void onArrivedExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit)
            {
                counters.addExpired(messageSize);

                MessagingService.instance().droppedMessages.incrementWithLatency(header.verb, timeElapsed, unit);
            }

            @Override
            public void onFailedDeserialize(int messageSize, Header header, Throwable t)
            {
                counters.addError(messageSize);

                /*
                 * If an exception is caught during deser, return a failure response immediately instead of waiting for the callback
                 * on the other end to expire.
                 */
                if (header.callBackOnFailure())
                {
                    Message response = Message.failureResponse(header.id, header.expiresAtNanos, RequestFailureReason.forException(t));
                    MessagingService.instance().sendOneWay(response, peer);
                }
            }
        };
    }

    /*
     * Aggregated counters
     */

    InboundCounters countersFor(ConnectionType type)
    {
        switch (type)
        {
            case URGENT_MESSAGES: return urgentCounters;
            case  SMALL_MESSAGES: return smallCounters;
            case  LARGE_MESSAGES: return largeCounters;
            case LEGACY_MESSAGES: return legacyCounters;
        }

        throw new IllegalArgumentException();
    }

    public long receivedCount()
    {
        return sumHandlers(h -> h.receivedCount) + closedReceivedCount;
    }

    public long receivedBytes()
    {
        return sumHandlers(h -> h.receivedBytes) + closedReceivedBytes;
    }

    public int corruptFramesRecovered()
    {
        return (int) sumHandlers(h -> h.corruptFramesRecovered) + closedCorruptFramesRecovered;
    }

    public int corruptFramesUnrecovered()
    {
        return (int) sumHandlers(h -> h.corruptFramesUnrecovered) + closedCorruptFramesUnrecovered;
    }

    public long errorCount()
    {
        return sumCounters(InboundCounters::errorCount);
    }

    public long errorBytes()
    {
        return sumCounters(InboundCounters::errorBytes);
    }

    public long expiredCount()
    {
        return sumCounters(InboundCounters::expiredCount);
    }

    public long expiredBytes()
    {
        return sumCounters(InboundCounters::expiredBytes);
    }

    public long processedCount()
    {
        return sumCounters(InboundCounters::processedCount);
    }

    public long processedBytes()
    {
        return sumCounters(InboundCounters::processedBytes);
    }

    public long pendingCount()
    {
        return sumCounters(InboundCounters::pendingCount);
    }

    public long pendingBytes()
    {
        return sumCounters(InboundCounters::pendingBytes);
    }

    private volatile long closedReceivedCount;
    private volatile long closedReceivedBytes;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedReceivedCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedReceivedCount");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> closedReceivedBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedReceivedBytes");

    private volatile int closedCorruptFramesRecovered;
    private volatile int closedCorruptFramesUnrecovered;

    private static final AtomicIntegerFieldUpdater<InboundMessageHandlers> closedCorruptFramesRecoveredUpdater =
        AtomicIntegerFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedCorruptFramesRecovered");
    private static final AtomicIntegerFieldUpdater<InboundMessageHandlers> closedCorruptFramesUnrecoveredUpdater =
        AtomicIntegerFieldUpdater.newUpdater(InboundMessageHandlers.class, "closedCorruptFramesUnrecovered");

    private void absorbCounters(InboundMessageHandler handler)
    {
        closedReceivedCountUpdater.addAndGet(this, handler.receivedCount);
        closedReceivedBytesUpdater.addAndGet(this, handler.receivedBytes);

        closedCorruptFramesRecoveredUpdater.addAndGet(this, handler.corruptFramesRecovered);
        closedCorruptFramesUnrecoveredUpdater.addAndGet(this, handler.corruptFramesUnrecovered);
    }

    private long sumHandlers(ToLongFunction<InboundMessageHandler> counter)
    {
        long sum = 0L;
        for (InboundMessageHandler h : handlers)
            sum += counter.applyAsLong(h);
        return sum;
    }

    private long sumCounters(ToLongFunction<InboundCounters> mapping)
    {
        return mapping.applyAsLong(urgentCounters)
             + mapping.applyAsLong(smallCounters)
             + mapping.applyAsLong(largeCounters)
             + mapping.applyAsLong(legacyCounters);
    }
}
