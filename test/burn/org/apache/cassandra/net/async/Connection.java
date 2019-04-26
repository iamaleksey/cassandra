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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.InboundMessageHandler.MessageProcessor;
import org.apache.cassandra.utils.ApproximateTime;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.utils.ExecutorUtils.runWithThreadName;

class Connection implements MessageCallbacks, MessageProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    final InetAddressAndPort sender;
    final InetAddressAndPort recipient;
    final InboundMessageHandlers inboundHandlers;
    final BytesInFlightController controller;
    final OutboundConnection outbound;
    final Verifier verifier;
    final MessageGenerator sendGenerator;
    final String linkId;
    final long minId;
    final long maxId;

    private final AtomicLong nextSendId = new AtomicLong();

    Connection(InetAddressAndPort sender, InetAddressAndPort recipient, InboundMessageHandlers inboundHandlers, OutboundConnection outbound, MessageGenerator generator, long minId, long maxId)
    {
        this.sender = sender;
        this.recipient = recipient;
        this.outbound = outbound;
        this.controller = new BytesInFlightController(1 << 20);
        this.sendGenerator = generator.copy();
        this.minId = minId;
        this.maxId = maxId;
        this.inboundHandlers = inboundHandlers;
        this.nextSendId.set(minId);
        this.linkId = sender.toString(false) + "->" + recipient.toString(false) + "-" + outbound.type();
        this.verifier = new Verifier(outbound.type());
    }

    void start(Runnable onFailure, Executor executor, long deadlineNanos)
    {
        executor.execute(runWithThreadName(() -> verifier.run(onFailure, deadlineNanos), "Verify-" + linkId));
        executor.execute(runWithThreadName(() -> send(onFailure, deadlineNanos), "Generate-" + linkId));
    }

    private void send(Runnable onFailure, long deadlineNanos)
    {
        try
        {
            while (ApproximateTime.nanoTime() < deadlineNanos && !Thread.currentThread().isInterrupted())
                sendOne();
        }
        catch (Throwable t)
        {
            if (t instanceof InterruptedException)
                return;
            logger.error("Unexpected exception", t);
            onFailure.run();
        }
    }

    private void sendOne() throws InterruptedException
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
            Verifier.EnqueueMessageEvent e = verifier.enqueue(msg);
            outbound.enqueue(msg);
            e.complete(verifier);
        }
        catch (ClosedChannelException e)
        {
            // TODO: make this a tested, not illegal, state
            throw new IllegalStateException(e);
        }
    }

    public void onSerialize(long id, int messagingVersion)
    {
        verifier.serialize(id, messagingVersion);
    }

    public void onDeserialize(long id, int messagingVersion)
    {
        verifier.deserialize(id, messagingVersion);
    }

    public void process(Message<?> message, int messageSize, MessageCallbacks callbacks)
    {
        controller.process(messageSize, callbacks);
        verifier.process(message, messageSize);
    }

    public void onArrived(long id, long timeElapsed, TimeUnit units)
    {
        verifier.arrive(id);
    }

    public void onProcessed(int messageSize)
    {
        throw new IllegalStateException();
    }

    public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit timeUnit)
    {
        controller.fail(messageSize);
        verifier.processExpired(id, messageSize, timeElapsed, timeUnit);
    }

    public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit timeUnit)
    {
        controller.fail(messageSize);
        verifier.expireOnArrival(id, messageSize, timeElapsed, timeUnit);
    }

    public void onSentExpired(int messageSize, long id, long timeElapsed, TimeUnit timeUnit)
    {
        controller.fail(messageSize);
        verifier.expireOnSend(id, messageSize, timeElapsed, timeUnit);
    }

    public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
    {
        controller.fail(messageSize);
    }

    InboundCounters inboundCounters()
    {
        return inboundHandlers.countersFor(outbound.type());
    }

}

