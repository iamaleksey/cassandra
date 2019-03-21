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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.ToLongFunction;

import io.netty.channel.Channel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeInboundMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.InboundCallbacks.OnMessageExpired;
import org.apache.cassandra.net.async.InboundCallbacks.MessageProcessor;
import org.apache.cassandra.net.async.InboundCallbacks.OnMessageProcessed;

public final class InboundMessageHandlers
{
    private final InetAddressAndPort peer;

    private final int queueCapacity;
    private final ResourceLimits.Limit endpointReserveCapacity;
    private final ResourceLimits.Limit globalReserveCapacity;

    private final InboundMessageHandler.WaitQueue endpointWaitQueue;
    private final InboundMessageHandler.WaitQueue globalWaitQueue;

    private final OnMessageExpired onExpired;
    private final MessageProcessor processor;

    private final Collection<InboundMessageHandler> handlers;
    private final InternodeInboundMetrics metrics;

    // TODO: specify some of this in InboundConnectionSettings
    public InboundMessageHandlers(InetAddressAndPort peer,
                                  ResourceLimits.Limit globalReserveCapacity,
                                  InboundMessageHandler.WaitQueue globalWaitQueue,
                                  OnMessageExpired onExpired,
                                  MessageProcessor processor)
    {
        this.peer = peer;
        this.queueCapacity = DatabaseDescriptor.getInternodeApplicationReceiveQueueCapacityInBytes();
        this.endpointReserveCapacity = new ResourceLimits.Concurrent(DatabaseDescriptor.getInternodeApplicationReserveReceiveQueueEndpointCapacityInBytes());
        this.globalReserveCapacity = globalReserveCapacity;
        this.endpointWaitQueue = new InboundMessageHandler.WaitQueue(endpointReserveCapacity);
        this.globalWaitQueue = globalWaitQueue;
        this.onExpired = onExpired;
        this.processor = processor;

        this.handlers = new CopyOnWriteArrayList<>();
        this.metrics = new InternodeInboundMetrics(peer, this);
    }

    InboundMessageHandler createHandler(InboundMessageHandler.Button button, Channel channel, int version)
    {
        InboundMessageHandler handler =
            new InboundMessageHandler(button,

                                      channel,
                                      peer,
                                      version,

                                      OutboundConnections.LARGE_MESSAGE_THRESHOLD,
                                      NettyFactory.instance.synchronousWorkExecutor,

                                      queueCapacity,
                                      endpointReserveCapacity,
                                      globalReserveCapacity,
                                      endpointWaitQueue,
                                      globalWaitQueue,

                                      this::onMessageError,
                                      this::onMessageExpired,
                                      this::onMessageArrivedExpired,
                                      this::onMessageProcessed,
                                      this::onHandlerClosed,

                                      this::process);
        handlers.add(handler);
        return handler;
    }

    public void releaseMetrics()
    {
        metrics.release();
    }

    /*
     * Callbacks
     */

    private void onMessageError(Throwable t, int messageSize)
    {
        errorCountUpdater.incrementAndGet(this);
        errorBytesUpdater.addAndGet(this, messageSize);
    }

    private void onMessageExpired(Verb verb, int messageSize, long timeElapsed, TimeUnit unit)
    {
        pendingCountUpdater.decrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, -messageSize);

        expiredCountUpdater.incrementAndGet(this);
        expiredBytesUpdater.addAndGet(this, messageSize);

        onExpired.call(verb, messageSize, timeElapsed, unit);
    }

    /*
     * Message was already expired when we started deserializing it, so it was never fully deserialized and enqueued
     * for processing. Don't increment pending count/bytes in this case.
     */
    private void onMessageArrivedExpired(Verb verb, int messageSize, long timeElapsed, TimeUnit unit)
    {
        expiredCountUpdater.incrementAndGet(this);
        expiredBytesUpdater.addAndGet(this, messageSize);

        onExpired.call(verb, messageSize, timeElapsed, unit);
    }

    private void process(Message<?> message, int messageSize, OnMessageProcessed onProcessed, OnMessageExpired onExpired)
    {
        pendingCountUpdater.incrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, messageSize);

        processor.process(message, messageSize, onProcessed, onExpired);
    }

    private void onMessageProcessed(int messageSize)
    {
        pendingCountUpdater.decrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, -messageSize);

        processedCountUpdater.incrementAndGet(this);
        processedBytesUpdater.addAndGet(this, messageSize);
    }

    private void onHandlerClosed(InboundMessageHandler handler)
    {
        handlers.remove(handler);
        absorbCounters(handler);
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