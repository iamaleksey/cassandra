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
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.InboundMessageHandler.MessageProcessor;

public final class InboundMessageHandlers implements MessageCallbacks
{
    private final InetAddressAndPort peer;

    private final int queueCapacity;
    private final ResourceLimits.Limit endpointReserveCapacity;
    private final ResourceLimits.Limit globalReserveCapacity;

    private final InboundMessageHandler.WaitQueue endpointWaitQueue;
    private final InboundMessageHandler.WaitQueue globalWaitQueue;

    private final Collection<InboundMessageHandler> handlers;
    private final InternodeInboundMetrics metrics;

    private final MessageProcessor messageProcessor;
    private final MessageCallbacks messageCallbacks;

    public InboundMessageHandlers(InetAddressAndPort peer,
                                  int queueCapacity,
                                  long endpointReserveCapacity,
                                  ResourceLimits.Limit globalReserveCapacity,
                                  InboundMessageHandler.WaitQueue globalWaitQueue,
                                  MessageProcessor messageProcessor)
    {
        this(peer, queueCapacity, endpointReserveCapacity, globalReserveCapacity, globalWaitQueue, messageProcessor, Functions.identity());
    }

    public InboundMessageHandlers(InetAddressAndPort peer,
                                  int queueCapacity,
                                  long endpointReserveCapacity,
                                  ResourceLimits.Limit globalReserveCapacity,
                                  InboundMessageHandler.WaitQueue globalWaitQueue,
                                  MessageProcessor messageProcessor,
                                  Function<MessageCallbacks, MessageCallbacks> messageCallbacks)
    {
        this.peer = peer;
        this.messageProcessor = messageProcessor;

        this.queueCapacity = queueCapacity;
        this.endpointReserveCapacity = new ResourceLimits.Concurrent(endpointReserveCapacity);
        this.globalReserveCapacity = globalReserveCapacity;
        this.messageCallbacks = messageCallbacks.apply(this);

        this.endpointWaitQueue = new InboundMessageHandler.WaitQueue(this.endpointReserveCapacity);
        this.globalWaitQueue = globalWaitQueue;

        this.handlers = new CopyOnWriteArrayList<>();
        this.metrics = new InternodeInboundMetrics(peer, this);
    }

    InboundMessageHandler createHandler(InboundMessageHandler.ReadSwitch readSwitch, ExecutorService synchronousWorkExecutor, Channel channel, int version)
    {
        InboundMessageHandler handler =
            new InboundMessageHandler(readSwitch,

                                      channel,
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
                                      messageCallbacks,
                                      this::process);
        handlers.add(handler);
        return handler;
    }

    public void releaseMetrics()
    {
        metrics.release();
    }

    /*
     * Wrap provided MessageProcessor to allow pending message metrics to be maintained.
     */
    private void process(Message<?> message, int messageSize, MessageCallbacks callbacks)
    {
        pendingCountUpdater.incrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, messageSize);

        messageProcessor.process(message, messageSize, callbacks);
    }

    private void onHandlerClosed(InboundMessageHandler handler)
    {
        handlers.remove(handler);
        absorbCounters(handler);
    }

    /*
     * Message callbacks
     */

    @Override
    public void onProcessed(int messageSize)
    {
        pendingCountUpdater.decrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, -messageSize);

        processedCountUpdater.incrementAndGet(this);
        processedBytesUpdater.addAndGet(this, messageSize);
    }

    @Override
    public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
    {
        pendingCountUpdater.decrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, -messageSize);

        expiredCountUpdater.incrementAndGet(this);
        expiredBytesUpdater.addAndGet(this, messageSize);

        MessagingService.instance().droppedMessages.incrementWithLatency(verb, timeElapsed, unit);
    }

    @Override
    public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
    {
        expiredCountUpdater.incrementAndGet(this);
        expiredBytesUpdater.addAndGet(this, messageSize);

        MessagingService.instance().droppedMessages.incrementWithLatency(verb, timeElapsed, unit);
    }

    @Override
    public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
    {
        errorCountUpdater.incrementAndGet(this);
        errorBytesUpdater.addAndGet(this, messageSize);

        /*
         * If an exception is caught during deser, return a failure response immediately instead of waiting for the callback
         * on the other end to expire.
         */
        if (callBackOnFailure)
        {
            Message response = Message.failureResponse(id, expiresAtNanos, RequestFailureReason.forException(t));
            MessagingService.instance().sendOneWay(response, peer);
        }
    }

    /*
     * Aggregated counters
     */

    public long receivedCount()
    {
        return sum(h -> h.receivedCount) + closedReceivedCount;
    }

    public long receivedBytes()
    {
        return sum(h -> h.receivedBytes) + closedReceivedBytes;
    }

    public int corruptFramesRecovered()
    {
        return (int) sum(h -> h.corruptFramesRecovered) + closedCorruptFramesRecovered;
    }

    public int corruptFramesUnrecovered()
    {
        return (int) sum(h -> h.corruptFramesUnrecovered) + closedCorruptFramesUnrecovered;
    }

    public long errorCount()
    {
        return errorCount;
    }

    public long errorBytes()
    {
        return errorBytes;
    }

    public long expiredCount()
    {
        return expiredCount;
    }

    public long expiredBytes()
    {
        return expiredBytes;
    }

    public long processedCount()
    {
        return processedCount;
    }

    public long processedBytes()
    {
        return processedBytes;
    }

    public long pendingCount()
    {
        return pendingCount;
    }

    public long pendingBytes()
    {
        return pendingBytes;
    }

    private volatile long errorCount;
    private volatile long errorBytes;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> errorCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "errorCount");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> errorBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "errorBytes");

    private volatile long expiredCount;
    private volatile long expiredBytes;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> expiredCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "expiredCount");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> expiredBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "expiredBytes");

    private volatile long processedCount;
    private volatile long processedBytes;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> processedCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "processedCount");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> processedBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "processedBytes");

    private volatile long pendingCount;
    private volatile long pendingBytes;

    private static final AtomicLongFieldUpdater<InboundMessageHandlers> pendingCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "pendingCount");
    private static final AtomicLongFieldUpdater<InboundMessageHandlers> pendingBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundMessageHandlers.class, "pendingBytes");

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

    private long sum(ToLongFunction<InboundMessageHandler> counter)
    {
        long sum = 0L;
        for (InboundMessageHandler h : handlers)
            sum += counter.applyAsLong(h);
        return sum;
    }
}
