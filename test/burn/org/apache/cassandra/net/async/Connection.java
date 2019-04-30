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
import org.apache.cassandra.utils.ApproximateTime;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.utils.ExecutorUtils.runWithThreadName;

class Connection implements InboundMessageCallbacks, OutboundMessageCallbacks
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

    Connection(InetAddressAndPort sender, InetAddressAndPort recipient, ConnectionType type,
               InboundMessageHandlers inboundHandlers,
               OutboundConnectionSettings outboundTemplate, ResourceLimits.EndpointAndGlobal reserveCapacityInBytes,
               MessageGenerator generator,
               long minId, long maxId)
    {
        this.sender = sender;
        this.recipient = recipient;
        this.controller = new BytesInFlightController(1 << 20);
        this.sendGenerator = generator.copy();
        this.minId = minId;
        this.maxId = maxId;
        this.inboundHandlers = inboundHandlers;
        this.nextSendId.set(minId);
        this.linkId = sender.toString(false) + "->" + recipient.toString(false) + "-" + type;
        this.verifier = new Verifier(controller, type);
        this.outbound = new OutboundConnection(type,
                                               outboundTemplate.toEndpoint(recipient)
                                                               .withFrom(sender)
                                                               .withCallbacks(this),
                                               reserveCapacityInBytes);
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
            Verifier.EnqueueMessageEvent e = verifier.onEnqueue(msg);
            outbound.enqueue(msg);
            e.complete(verifier);
        }
        catch (ClosedChannelException e)
        {
            // TODO: make this a tested, not illegal, state
            throw new IllegalStateException(e);
        }
    }

    void interrupt()
    {
        Verifier.InterruptEvent interrupt = verifier.interrupt();
        outbound.interrupt().addListener(future -> interrupt.complete(verifier));
    }

    public void onSerialize(long id, int messagingVersion)
    {
        verifier.onSerialize(id, messagingVersion);
    }

    public void onDeserialize(long id, int messagingVersion)
    {
        verifier.onDeserialize(id, messagingVersion);
    }

    public void onArrived(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
    {
        verifier.onArrived(header.id);
    }

    public void onDispatched(int messageSize, Message.Header header)
    {
    }

    public void onExecuting(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
    {
    }

    public boolean shouldProcess(int messageSize, Message message)
    {
        verifier.onProcessed(message, messageSize);
        return false;
    }

    public void onProcessingException(int messageSize, Message.Header header, Throwable t)
    {
    }

    public void onProcessed(int messageSize, Message.Header header)
    {
    }

    public void onExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit timeUnit)
    {
        controller.fail(messageSize);
        verifier.onProcessedExpired(header.id, messageSize, timeElapsed, timeUnit);
    }

    public void onArrivedExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit timeUnit)
    {
        controller.fail(messageSize);
        verifier.onArrivedExpired(header.id, messageSize, timeElapsed, timeUnit);
    }

    public void onFailedDeserialize(int messageSize, Message.Header header, Throwable t)
    {
        controller.fail(messageSize);
        verifier.onFailedDeserialize(header.id);
    }

    InboundCounters inboundCounters()
    {
        return inboundHandlers.countersFor(outbound.type());
    }

    public void onSendFrame(int messageCount, int payloadSizeInBytes)
    {
        verifier.onSendFrame(messageCount, payloadSizeInBytes);
    }

    public void onSentFrame(int messageCount, int payloadSizeInBytes)
    {
        verifier.onSentFrame(messageCount, payloadSizeInBytes);
    }

    public void onFailedFrame(int messageCount, int payloadSizeInBytes)
    {
        controller.fail(payloadSizeInBytes);
        verifier.onFailedFrame(messageCount, payloadSizeInBytes);
    }

    public void onOverloaded(Message<?> message)
    {
        controller.fail(message.serializedSize(current_version));
        verifier.onOverloaded(message.id());
    }

    public void onExpired(Message<?> message)
    {
        controller.fail(message.serializedSize(current_version));
        verifier.onExpiredBeforeSend(message.id(), message.serializedSize(current_version), ApproximateTime.nanoTime() - message.createdAtNanos(), TimeUnit.NANOSECONDS);
    }

    public void onFailedSerialize(Message<?> message)
    {
        controller.fail(message.serializedSize(current_version));
        verifier.onFailedSerialize(message.id());
    }

    public void onClosed(Message<?> message)
    {
        controller.fail(message.serializedSize(current_version));
        verifier.onFailedClosing(message.id());
    }
}

