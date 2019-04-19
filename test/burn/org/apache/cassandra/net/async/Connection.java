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

import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.InboundMessageHandler.MessageProcessor;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

class Connection implements MessageCallbacks, MessageProcessor
{
    final InetAddressAndPort sender;
    final InetAddressAndPort recipient;
    final InboundMessageHandlers inboundHandlers;
    final BytesInFlightController controller;
    final OutboundConnection outbound;
    final MessageGenerator sendGenerator;
    final MessageGenerator receiveGenerator;
    final long minId;
    final long maxId;
    final int version;
    final long onEventLoopThreshold;

    private final AtomicLong nextSendId = new AtomicLong();
    private volatile long minReceiveId;
    private final ConcurrentSkipListMap<Long, Long> inFlight = new ConcurrentSkipListMap<>();
    private final WaitQueue waitQueue = new WaitQueue();

    Connection(InetAddressAndPort sender, InetAddressAndPort recipient, InboundMessageHandlers inboundHandlers, OutboundConnection outbound, MessageGenerator generator, long minId, long maxId, int version)
    {
        this.sender = sender;
        this.recipient = recipient;
        this.outbound = outbound;
        this.controller = new BytesInFlightController(1 << 20);
        this.sendGenerator = generator.copy();
        this.receiveGenerator = generator.copy();
        this.minId = minId;
        this.maxId = maxId;
        this.inboundHandlers = inboundHandlers;
        this.minReceiveId = minId;
        this.version = version;
        this.nextSendId.set(minId);
        this.onEventLoopThreshold = version < current_version ? LARGE_MESSAGE_THRESHOLD
                                                              : OutboundConnection.LargeMessageDelivery.DEFAULT_BUFFER_SIZE;
    }

    void sendOne() throws InterruptedException
    {
        long id = nextSendId.getAndUpdate(i -> i == maxId ? minId : i + 1);
        try
        {
            Message<?> msg;
            synchronized (sendGenerator)
            {
                msg = sendGenerator.generate(id).withId(id);
            }
            controller.send(msg.serializedSize(current_version));
            inFlight.put(id, (long) msg.serializedSize(version));
            outbound.enqueue(msg);
        }
        catch (ClosedChannelException e)
        {
            // TODO: make this a tested, not illegal, state
            throw new IllegalStateException(e);
        }
    }

    public void process(Message<?> message, int messageSize, MessageCallbacks callbacks)
    {
        controller.process(messageSize, callbacks);
        Message<?> canon;
        synchronized (receiveGenerator)
        {
            canon = receiveGenerator.generate(message.id);
        }
        if (!Arrays.equals((byte[])canon.payload, (byte[]) message.payload)) // assertArrayEquals is EXTREMELY inefficient
            Assert.assertArrayEquals((byte[])canon.payload, (byte[]) message.payload);
        checkReceived(messageSize, message.id, true);
    }

    public void onProcessed(int messageSize)
    {
        throw new IllegalStateException();
    }

    public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
    {
        onFailed(messageSize, id);
    }

    public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
    {
        onFailed(messageSize, id);
    }

    public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
    {
        onFailed(messageSize, id);
    }

    private void onFailed(int messageSize, long id)
    {
        controller.fail(messageSize);
        Message<?> canon;
        synchronized (receiveGenerator)
        {
            canon = receiveGenerator.generate(id);
        }
        Assert.assertEquals(canon.serializedSize(current_version), messageSize);
        checkReceived(messageSize, id, false);
    }

    InboundCounters inboundCounters()
    {
        return inboundHandlers.countersFor(outbound.type());
    }

    private long difference(long newest, long oldest)
    {
        return newest > oldest ? newest - oldest : (newest - minId) + (maxId - oldest);
    }

    private void checkReceived(int messageSize, long id, boolean success)
    {
//        System.out.println("C " + id);
        Map.Entry<Long, Long> oldest;
        oldest = inFlight.ceilingEntry(minReceiveId);
        if (oldest == null)
            oldest = inFlight.ceilingEntry(minId);
        this.minReceiveId = oldest.getKey();

        Assert.assertEquals((long)inFlight.remove(id), (long)messageSize);
        waitQueue.signalAll();

        if (oldest.getValue() <= onEventLoopThreshold)
        {
            // if the oldest value we are waiting for was processed on the event loop, then we must be that message
            // as we process messages in order on the event loop
            if (oldest.getKey() != id)
            {
                synchronized (this)
                {
                    Assert.fail(minId + " " + minReceiveId + " " + oldest.getKey() + " " + maxId + " " +  inFlight.toString());
                }
            }
        }
        // TODO: special case <= onEventLoopThreshold?
        // TODO: must discount those that have failed before sending
        else if (difference(id, oldest.getKey()) > 16)
        {
            long waitUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100L);
            while (true)
            {
                WaitQueue.Signal signal = waitQueue.register();
                if (!inFlight.containsKey(oldest.getKey()))
                {
                    signal.cancel();
                    return;
                }
                if (!signal.awaitUntilUninterruptibly(waitUntil) && inFlight.containsKey(oldest.getKey()))
                {
                    synchronized (this)
                    {
                        Assert.fail(minId + " " + minReceiveId + " " + oldest.getKey() + " " + maxId + " " +  inFlight.toString());
                    }
                }
            }
        }
    }
}
