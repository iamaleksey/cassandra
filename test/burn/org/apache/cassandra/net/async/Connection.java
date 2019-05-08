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
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.async.Verifier.Destiny;
import org.apache.cassandra.utils.ApproximateTime;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.utils.ExecutorUtils.runWithThreadName;

public class Connection implements InboundMessageCallbacks, OutboundMessageCallbacks, OutboundDebugCallbacks
{
    public static class IntentionalIOException extends IOException {}
    public static class IntentionalRuntimeException extends RuntimeException {}

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    final InetAddressAndPort sender;
    final InetAddressAndPort recipient;
    final InboundMessageHandlers inboundHandlers;
    final BytesInFlightController controller;
    final OutboundConnection outbound;
    final OutboundConnectionSettings outboundTemplate;
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
        this.outboundTemplate = outboundTemplate.toEndpoint(recipient)
                                                .withFrom(sender)
                                                .withCallbacks(this)
                                                .withDebugCallbacks(this);
        this.outbound = new OutboundConnection(type, this.outboundTemplate, reserveCapacityInBytes);
    }

    void startVerifier(Runnable onFailure, Executor executor, long deadlineNanos)
    {
        executor.execute(runWithThreadName(() -> verifier.run(onFailure, deadlineNanos), "Verify-" + linkId));
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

    void sendOne() throws InterruptedException
    {
        long id = nextSendId.getAndUpdate(i -> i == maxId ? minId : i + 1);
        try
        {
            Destiny destiny = Destiny.SUCCEED;
            byte realDestiny = 0;
            Message<?> msg;
            synchronized (sendGenerator)
            {
                if (0 == sendGenerator.uniformInt(1 << 10))
                {
                    // abnormal destiny
                    realDestiny = (byte) (1 + sendGenerator.uniformInt(6));
                    destiny = realDestiny <= 3 ? Destiny.FAIL_TO_SERIALIZE : Destiny.FAIL_TO_DESERIALIZE;
                }
                msg = sendGenerator.generate(id, realDestiny).withId(id);
            }

            controller.send(msg.serializedSize(current_version));
            Verifier.EnqueueMessageEvent e = verifier.onEnqueue(msg, destiny);
            outbound.enqueue(msg);
            e.complete(verifier);
        }
        catch (ClosedChannelException e)
        {
            // TODO: make this a tested, not illegal, state
            throw new IllegalStateException(e);
        }
    }

    void reconnect(OutboundConnectionSettings template)
    {
        outbound.reconnectWithNewTemplate(template);
    }

    void serialize(long id, byte[] payload, DataOutputPlus out, int messagingVersion) throws IOException
    {
        verifier.onSerialize(id, messagingVersion);
        int length = payload.length;
        switch (MessageGenerator.getInfo(payload))
        {
            case 1:
                switch ((int) (id & 1))
                {
                    case 0: throw new IntentionalIOException();
                    case 1: throw new IntentionalRuntimeException();
//                    case 2: throw new AssertionError("INTENTIONAL FAILURE");
                }
                break;
            case 2:
                length -= (int)id % payload.length;
                break;
            case 3:
                length += (int)id & 65535;
                break;
        }

        MessageGenerator.write(payload, Math.min(length, payload.length), out, messagingVersion);
        while ((length -= payload.length) > 0)
            out.write(payload, 0, Math.min(length, payload.length));
    }

    byte[] deserialize(MessageGenerator.Header header, DataInputPlus in, int messagingVersion) throws IOException
    {
        verifier.onDeserialize(header.id, messagingVersion);
        int length = header.length;
//        switch (header.info)
//        {
//            case 4:
//                switch ((int) (header.id & 1))
//                {
//                    case 0: throw new IntentionalExceptions.IntentionalIOException();
//                    case 1: throw new IntentionalExceptions.IntentionalRuntimeException();
////                    case 2: throw new AssertionError("INTENTIONAL FAILURE");
//                }
//                break;
//            case 5: {
//                length -= (int)header.id % header.length;
//                break;
//            }
//            case 6: {
//                length += (int)header.id & 65535;
//                break;
//            }
//        }
        byte[] result = header.read(in, Math.min(header.length, length), messagingVersion);
//        if (length > header.length)
//        {
//            length -= header.length;
//            while (length >= 8)
//            {
//                in.readLong();
//                length -= 8;
//            }
//            while (length-- > 0)
//                in.readByte();
//        }
        return result;
    }

    public void onArrived(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
    {
        verifier.onArrived(header.id, messageSize);
    }

    public void onDispatched(int messageSize, Message.Header header) {}
    public void onExecuting(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit) {}
    public void onProcessed(int messageSize, Message.Header header) {}
    public void onExecuted(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit) {}

    public void process(Message message)
    {
        verifier.process(message);
    }

    public void onExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit timeUnit)
    {
        controller.fail(messageSize);
        verifier.onProcessExpired(header.id, messageSize, timeElapsed, timeUnit);
    }

    public void onArrivedExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit timeUnit)
    {
        controller.fail(messageSize);
        verifier.onArrivedExpired(header.id, messageSize, timeElapsed, timeUnit);
    }

    public void onFailedDeserialize(int messageSize, Message.Header header, Throwable t)
    {
        controller.fail(messageSize);
        verifier.onFailedDeserialize(header.id, messageSize);
    }

    InboundCounters inboundCounters()
    {
        return inboundHandlers.countersFor(outbound.type());
    }

    public void onSendSmallFrame(int messageCount, int payloadSizeInBytes)
    {
        verifier.onSendFrame(messageCount, payloadSizeInBytes);
    }

    public void onSentSmallFrame(int messageCount, int payloadSizeInBytes)
    {
        verifier.onSentFrame(messageCount, payloadSizeInBytes);
    }

    public void onFailedSmallFrame(int messageCount, int payloadSizeInBytes)
    {
        controller.fail(payloadSizeInBytes);
        verifier.onFailedFrame(messageCount, payloadSizeInBytes);
    }

    public void onConnect(int messagingVersion, OutboundConnectionSettings settings)
    {
        verifier.onConnect(messagingVersion, settings);
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

    public void onFailedSerialize(Message<?> message, int messagingVersion)
    {
        controller.fail(message.serializedSize(messagingVersion));
        verifier.onFailedSerialize(message.id());
    }

    public void onDiscardOnClose(Message<?> message)
    {
        controller.fail(message.serializedSize(current_version));
        verifier.onFailedClosing(message.id());
    }

}

