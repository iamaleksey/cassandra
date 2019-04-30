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

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.async.Verifier.ExpiredMessageEvent.ExpirationType;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.ConnectionType.LARGE_MESSAGES;
import static org.apache.cassandra.net.async.OutboundConnection.LargeMessageDelivery.DEFAULT_BUFFER_SIZE;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
import static org.apache.cassandra.net.async.Verifier.EventType.ARRIVE;
import static org.apache.cassandra.net.async.Verifier.EventType.DESERIALIZE;
import static org.apache.cassandra.net.async.Verifier.EventType.ENQUEUE;
import static org.apache.cassandra.net.async.Verifier.EventType.FAILED_CLOSING;
import static org.apache.cassandra.net.async.Verifier.EventType.FAILED_DESERIALIZE;
import static org.apache.cassandra.net.async.Verifier.EventType.FAILED_EXPIRED;
import static org.apache.cassandra.net.async.Verifier.EventType.FAILED_FRAME;
import static org.apache.cassandra.net.async.Verifier.EventType.FAILED_OVERLOADED;
import static org.apache.cassandra.net.async.Verifier.EventType.FAILED_SERIALIZE;
import static org.apache.cassandra.net.async.Verifier.EventType.PROCESS;
import static org.apache.cassandra.net.async.Verifier.EventType.SEND_FRAME;
import static org.apache.cassandra.net.async.Verifier.EventType.SENT_FRAME;
import static org.apache.cassandra.net.async.Verifier.EventType.SERIALIZE;

/**
 * This class is a single-threaded verifier monitoring a single link, with events supplied by inbound and outbound threads
 *
 * By making verification single threaded, it is easier to reason about (and complex enough as is), but also permits
 * a dedicated thread to monitor timeliness of events, e.g. elapsed time between a given SEND and its corresponding RECEIVE
 */
@SuppressWarnings("WeakerAccess")
public class Verifier
{
    private static final Logger logger = LoggerFactory.getLogger(Verifier.class);

    public enum EventType
    {
        ENQUEUE,
        SERIALIZE,
        SEND_FRAME,
        SENT_FRAME,
        ARRIVE,
        DESERIALIZE,
        PROCESS,
        FAILED_EXPIRED,
        FAILED_OVERLOADED,
        FAILED_SERIALIZE,
        FAILED_DESERIALIZE,
        FAILED_CLOSING,
        FAILED_FRAME,
        CONTROLLER_UPDATE
    }

    public static class Event
    {
        final EventType type;
        Event(EventType type)
        {
            this.type = type;
        }
    }

    static class SimpleEvent extends Event
    {
        final long at;
        SimpleEvent(EventType type, long at)
        {
            super(type);
            this.at = at;
        }
    }

    static class BoundedEvent extends Event
    {
        final long start;
        volatile long end;
        BoundedEvent(EventType type, Verifier verifier)
        {
            super(type);
            this.start = verifier.sequenceId.getAndIncrement();
        }
        public void complete(Verifier verifier)
        {
            end = verifier.sequenceId.getAndIncrement();
            verifier.events.put(end, this);
        }
    }

    static class SimpleMessageEvent extends SimpleEvent
    {
        final long messageId;
        SimpleMessageEvent(EventType type, long at, long messageId)
        {
            super(type, at);
            this.messageId = messageId;
        }
    }

    static class BoundedMessageEvent extends BoundedEvent
    {
        final long messageId;
        BoundedMessageEvent(EventType type, Verifier verifier, long messageId)
        {
            super(type, verifier);
            this.messageId = messageId;
        }
    }

    static class EnqueueMessageEvent extends BoundedMessageEvent
    {
        final Message<?> message;
        EnqueueMessageEvent(EventType type, Verifier verifier, Message<?> message)
        {
            super(type, verifier, message.id());
            this.message = message;
        }
    }

    static class SerializeMessageEvent extends SimpleMessageEvent
    {
        final int messagingVersion;
        SerializeMessageEvent(EventType type, long at, long messageId, int messagingVersion)
        {
            super(type, at, messageId);
            this.messagingVersion = messagingVersion;
        }
    }

    static class ExpiredMessageEvent extends SimpleMessageEvent
    {
        enum ExpirationType {ON_SENT, ON_ARRIVED, ON_PROCESSED }
        final int size;
        final long timeElapsed;
        final TimeUnit timeUnit;
        final ExpirationType expirationType;
        ExpiredMessageEvent(long at, long messageId, int size, long timeElapsed, TimeUnit timeUnit, ExpirationType expirationType)
        {
            super(FAILED_EXPIRED, at, messageId);
            this.size = size;
            this.timeElapsed = timeElapsed;
            this.timeUnit = timeUnit;
            this.expirationType = expirationType;
        }
    }

    static class FrameEvent extends SimpleEvent
    {
        final int messageCount;
        final int payloadSizeInBytes;
        FrameEvent(EventType type, long at, int messageCount, int payloadSizeInBytes)
        {
            super(type, at);
            this.messageCount = messageCount;
            this.payloadSizeInBytes = payloadSizeInBytes;
        }
    }

    static class ProcessMessageEvent extends SimpleMessageEvent
    {
        final int size;
        final Message<?> message;
        ProcessMessageEvent(long at, Message<?> message, int size)
        {
            super(PROCESS, at, message.id());
            this.message = message;
            this.size = size;
        }
    }

    static class ControllerEvent extends BoundedEvent
    {
        final long minimumBytesInFlight;
        final long maximumBytesInFlight;
        ControllerEvent(Verifier verifier, long minimumBytesInFlight, long maximumBytesInFlight)
        {
            super(EventType.CONTROLLER_UPDATE, verifier);
            this.minimumBytesInFlight = minimumBytesInFlight;
            this.maximumBytesInFlight = maximumBytesInFlight;
        }
    }

    EnqueueMessageEvent onEnqueue(Message<?> message)
    {
        EnqueueMessageEvent enqueue = new EnqueueMessageEvent(ENQUEUE, this, message);
        events.put(enqueue.start, enqueue);
        return enqueue;
    }
    private static EnqueueMessageEvent onEnqueue(Event e) { return (EnqueueMessageEvent)e; }

    void onSerialize(long messageId, int messagingVersion)
    {
        long at = nextId();
        events.put(at, new SerializeMessageEvent(SERIALIZE, at, messageId, messagingVersion));
    }
    private static SerializeMessageEvent onSerialize(Event e) { return (SerializeMessageEvent)e; }

    void onArrived(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(ARRIVE, at, messageId));
    }
    private static SimpleMessageEvent onArrived(Event e) { return (SimpleMessageEvent)e; }

    void onDeserialize(long messageId, int messagingVersion)
    {
        long at = nextId();
        events.put(at, new SerializeMessageEvent(DESERIALIZE, at, messageId, messagingVersion));
    }
    private static SimpleMessageEvent onDeserialize(Event e) { return (SimpleMessageEvent)e; }

    void onProcessed(Message<?> message, int size)
    {
        long at = nextId();
        events.put(at, new ProcessMessageEvent(at, message, size));
    }
    private static ProcessMessageEvent onProcessed(Event e) { return (ProcessMessageEvent)e; }

    void onExpiredBeforeSend(long messageId, int size, long timeElapsed, TimeUnit timeUnit)
    {
        onExpired(messageId, size, timeElapsed, timeUnit, ExpirationType.ON_SENT);
    }
    void onArrivedExpired(long messageId, int size, long timeElapsed, TimeUnit timeUnit)
    {
        onExpired(messageId, size, timeElapsed, timeUnit, ExpirationType.ON_ARRIVED);
    }
    void onProcessedExpired(long messageId, int size, long timeElapsed, TimeUnit timeUnit)
    {
        onExpired(messageId, size, timeElapsed, timeUnit, ExpirationType.ON_PROCESSED);
    }
    private void onExpired(long messageId, int size, long timeElapsed, TimeUnit timeUnit, ExpirationType expirationType)
    {
        long at = nextId();
        events.put(at, new ExpiredMessageEvent(at, messageId, size, timeElapsed, timeUnit, expirationType));
    }
    private static ExpiredMessageEvent onExpired(Event e) { return (ExpiredMessageEvent)e; }

    void onOverloaded(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(FAILED_OVERLOADED, at, messageId));
    }
    private static SimpleMessageEvent onOverloaded(Event e) { return (SimpleMessageEvent)e; }

    void onFailedSerialize(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(FAILED_SERIALIZE, at, messageId));
    }
    private static SimpleMessageEvent onFailedSerialize(Event e) { return (SimpleMessageEvent)e; }
    void onFailedDeserialize(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(FAILED_DESERIALIZE, at, messageId));
    }
    private static SimpleMessageEvent onFailedDeserialize(Event e) { return (SimpleMessageEvent)e; }

    void onFailedClosing(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(FAILED_CLOSING, at, messageId));
    }
    private static SimpleMessageEvent onFailedClosing(Event e) { return (SimpleMessageEvent)e; }

    void onSendFrame(int messageCount, int payloadSizeInBytes)
    {
        long at = nextId();
        events.put(at, new FrameEvent(SEND_FRAME, at, messageCount, payloadSizeInBytes));
    }

    void onSentFrame(int messageCount, int payloadSizeInBytes)
    {
        long at = nextId();
        events.put(at, new FrameEvent(SENT_FRAME, at, messageCount, payloadSizeInBytes));
    }

    void onFailedFrame(int messageCount, int payloadSizeInBytes)
    {
        long at = nextId();
        events.put(at, new FrameEvent(FAILED_DESERIALIZE, at, messageCount, payloadSizeInBytes));
    }



    private final BytesInFlightController controller;
    private final AtomicLong sequenceId = new AtomicLong();
    private final EventSequence events = new EventSequence();
    private final ConnectionType outboundType;

    Verifier(BytesInFlightController controller, ConnectionType outboundType)
    {
        this.controller = controller;
        this.outboundType = outboundType;
    }

    private long nextId()
    {
        return sequenceId.getAndIncrement();
    }

    private void fail(String message, Object ... params)
    {
        logger.error("{}", String.format(message, params));
    }

    // TODO: - verify timeliness of all messages, not just those arriving out of order
    //         (integrate bytes in flight controller info into timeliness decisions)
    //       - indicate a message is expected to fail serialization (or deserialization)
    final LongObjectOpenHashMap<MessageState> messages = new LongObjectOpenHashMap<>();

    // messages start here, but may enter in a haphazard (non-sequential) fashion;
    // ENQUEUE_START, ENQUEUE_END both take place here, with the latter imposing bounds on the out-of-order appearance of messages.
    // note that ENQUEUE_END - being concurrent - may not appear before the message's lifespan has completely ended.
    final Queue<MessageState> enqueueing = new Queue<>();

    // Strict message order will then be determined at serialization time, since this happens on a single thread.
    // The order in which messages arrive here determines the order they will arrive on the other node.
    // must follow either ENQUEUE_START or ENQUEUE_END
    final Queue<MessageState> serializing = new Queue<>();

    // Messages sent on the small connection will all be sent in frames; this is a concurrent operation,
    // so only the sendingFrame MUST be encountered before any future events -
    // large connections skip this step and goes straight to arriving
    // we consult the queues in reverse order in arriving, as it is acceptable to find our frame in any of these queues
    final FramesInFlight framesInFlight = new FramesInFlight(); // unknown if the messages will arrive, accept either
    final Queue<Frame> reuseFrames = new Queue<>();

    // for large messages OR < VERSION_40, arriving can occur BEFORE serializing completes successfully
    // OR a frame is fully serialized
    final Queue<MessageState> arriving = new Queue<>();

    final Queue<MessageState> deserializingOnEventLoop = new Queue<>(),
                              deserializingOffEventLoop = new Queue<>();

    final Queue<MessageState> processingOutOfOrder = new Queue<>();

    long canonicalBytesInFlight = 0;
    long nextId = 0;
    long now;

    public void run(Runnable onFailure, long deadlineNanos)
    {
        try
        {
            while ((now = ApproximateTime.nanoTime()) < deadlineNanos)
            {
                Event next = events.await(nextId, 100L, TimeUnit.MILLISECONDS);
                if (next == null)
                {
                    // decide if we have any messages waiting too long to proceed
                    while (!processingOutOfOrder.isEmpty())
                    {
                        MessageState m = processingOutOfOrder.get(0);
                        if (now - m.lastUpdateNanos > TimeUnit.SECONDS.toNanos(10L))
                        {
                            fail("Unreasonably long period spent waiting for out-of-order deser/delivery of received message %d", m.message.id());
                            messages.remove(m.message.id());
                            processingOutOfOrder.remove(0);
                        }
                        else break;
                    }
                    continue;
                }
                events.clear(nextId); // TODO: simplify collection if we end up using it exclusively as a queue, as we are now

                switch (next.type)
                {
                    case ENQUEUE:
                    {
                        MessageState m;
                        EnqueueMessageEvent e = onEnqueue(next);
                        assert nextId == e.start || nextId == e.end;
                        assert e.message != null;
                        if (nextId == e.start)
                        {
                            canonicalBytesInFlight += e.message.serializedSize(current_version);
                            m = new MessageState(e.message, e.start);
                            messages.put(e.messageId, m);
                            enqueueing.add(m);
                            m.update(next.type);
                        }
                        else
                        {
                            // warning: enqueueEnd can occur at any time in the future, since it's a different thread;
                            //          it could be arbitrarily paused, long enough even for the messsage to be fully processed
                            m = messages.get(e.messageId);
                            if (m != null)
                                m.enqueueEnd = e.end;
                        }
                        break;
                    }
                    case FAILED_OVERLOADED:
                    {
                        // TODO: verify that we could have exceeded our memory limits
                        SimpleMessageEvent e = onOverloaded(next);
                        assert nextId == e.at;
                        MessageState m = remove(e.messageId, enqueueing, messages);
                        if (ENQUEUE != m.state)
                            fail("Invalid state at overload of %d: expected message in %s, found %s", m.message.id(), ENQUEUE, m.state);
                        break;
                    }
                    case FAILED_CLOSING:
                    {
                        // TODO: verify if this is acceptable due to e.g. inbound refusing to process for long enough
                        SimpleMessageEvent e = onFailedClosing(next);
                        assert nextId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        enqueueing.remove(m);
                        if (ENQUEUE == m.state) enqueueing.remove(m);
                        else fail("Invalid state at close of %d: expected message in %s, found %s", m.message.id(), ENQUEUE, m.state);
                        fail("Invalid discard of %d: connection was closing for too long", m.message.id());
                        break;
                    }
                    case SERIALIZE:
                    {
                        // serialize happens serially, so we can compress the asynchronicity of the above enqueue
                        // into a linear sequence of events we expect to occur on arrival
                        SerializeMessageEvent e = onSerialize(next);
                        assert nextId == e.at;
                        MessageState m = messages.get(e.messageId);
                        assert m.state == ENQUEUE;
                        m.serialize = e.at;
                        m.messagingVersion = e.messagingVersion;
                        if (e.messagingVersion != current_version)
                            controller.adjust(m.message.serializedSize(current_version), m.message.serializedSize(e.messagingVersion));

                        m.processOnEventLoop = willProcessOnEventLoop(outboundType, m.message, e.messagingVersion);
                        m.expiresAtNanos = expiresAtNanos(m.message, e.messagingVersion);
                        int mi = enqueueing.indexOf(m);
                        for (int i = 0 ; i < mi ; ++i)
                        {
                            MessageState pm = enqueueing.get(i);
                            if (pm.enqueueEnd != 0)
                            {
                                fail("Invalid order of events: %s enqueued strictly before %s, but serialized after",
                                     pm, m);
                            }
                        }
                        enqueueing.remove(mi);
                        serializing.add(m);
                        m.update(next.type);
                        break;
                    }
                    case FAILED_SERIALIZE:
                    {
                        // TODO: verify this failure to serialize was intended by test
                        SimpleMessageEvent e = onFailedSerialize(next);
                        assert nextId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        enqueueing.remove(m);
                        break;
                    }
                    case SEND_FRAME:
                    {
                        FrameEvent e = (FrameEvent) next;
                        assert nextId == e.at;
                        int size = 0;
                        Frame frame = reuseFrames.poll();
                        if (frame == null) frame = new Frame();
                        MessageState first = serializing.get(0);
                        int messagingVersion = first.messagingVersion;
                        for (int i = 0 ; i < e.messageCount ; ++i)
                        {
                            MessageState m = serializing.get(i);
                            size += m.message.serializedSize(m.messagingVersion);
                            if (m.messagingVersion != messagingVersion)
                            {
                                fail("Invalid sequence of events: %s encoded to same frame as %s",
                                     m, first);
                            }

                            frame.add(m);
                            m.update(e.type);
                        }
                        frame.payloadSizeInBytes = e.payloadSizeInBytes;
                        frame.messageCount = e.messageCount;
                        frame.messagingVersion = messagingVersion;
                        framesInFlight.add(frame);
                        serializing.removeFirst(e.messageCount);
                        if (e.payloadSizeInBytes != size)
                            fail("Invalid frame payload size with %s: expected %d, actual %d", first,  size, e.payloadSizeInBytes);
                        break;
                    }
                    case SENT_FRAME:
                    {
                        framesInFlight.supplySendStatus(Frame.Status.SUCCESS);
                        break;
                    }
                    case FAILED_FRAME:
                    {
                        // TODO: verify that this was expected
                        Frame frame = framesInFlight.supplySendStatus(Frame.Status.FAILED);
                        if (frame != null && frame.messagingVersion >= VERSION_40)
                        {
                            // the contents cannot be delivered without the whole frame arriving, so clear the contents now
                            clear(frame, messages);
                            framesInFlight.remove(frame);
                            reuseFrames.add(frame.reset());
                        }
                        break;
                    }
                    case ARRIVE:
                    {
                        SimpleMessageEvent e = onArrived(next);
                        assert nextId == e.at;
                        MessageState m = messages.get(e.messageId);
                        m.arrive = e.at;
                        if (outboundType == LARGE_MESSAGES)
                        {
                            assert m.state == SERIALIZE;
                            int mi = serializing.indexOf(m);
                            for (int i = 0; i < mi; ++i)
                                fail("Invalid order of events: %s serialized to large stream strictly before %s, but arrived after", serializing.get(i), m);
                            serializing.remove(mi);
                            arriving.add(m);
                        }
                        else
                        {
                            if (m.state != SEND_FRAME)
                            {
                                fail("Invalid order of events: %s arrived before being sent in a frame", m);
                                break;
                            }

                            int fi = -1, mi = -1;
                            while (fi + 1 < framesInFlight.size() && mi < 0)
                                mi = framesInFlight.get(++fi).indexOf(m);

                            if (fi == framesInFlight.size())
                            {
                                fail("Invalid state: %s, but no frame in flight was found to contain it", m);
                                break;
                            }

                            if (fi > 0)
                            {
                                // we have skipped over some frames, meaning these have either failed (and we know it)
                                // or we have not yet heard about them and they have presumably failed, or something
                                // has gone wrong
                                for (int i = 0 ; i < fi ; ++i)
                                {
                                    Frame skip = framesInFlight.get(i);
                                    skip.receiveStatus = Frame.Status.FAILED;
                                    if (skip.sendStatus == Frame.Status.SUCCESS)
                                        fail("Successfully sent frame %s was not delivered", skip);
                                    clear(skip, messages);
                                    reuseFrames.add(skip.reset());
                                }
                                framesInFlight.removeFirst(fi);
                            }

                            Frame frame = framesInFlight.get(0);
                            for (int i = 0; i < mi; ++i)
                                fail("Invalid order of events: %s serialized strictly before %s, but arrived after", frame.get(i), m);

                            frame.remove(mi);
                            if (frame.isEmpty())
                            {
                                framesInFlight.poll();
                                reuseFrames.add(frame.reset());
                            }
                            arriving.add(m);
                        }
                        m.update(next.type);
                        break;
                    }
                    case DESERIALIZE:
                    {
                        // deserialize may happen in parallel for large messages, but in sequence for small messages
                        // we currently require that this event be issued before any possible error is thrown
                        SimpleMessageEvent e = onDeserialize(next);
                        assert nextId == e.at;
                        MessageState m = messages.get(e.messageId);
                        assert m.state == ARRIVE;
                        m.deserialize = e.at;
                        // deserialize may be off-loaded, so we can only impose meaningful ordering constraints
                        // on those messages we know to have been processed on the event loop
                        int mi = arriving.indexOf(m);
                        if (m.processOnEventLoop)
                        {
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = arriving.get(i);
                                if (pm.processOnEventLoop)
                                {
                                    fail("Invalid order of events: %d (%d, %d) arrived strictly before %d (%d, %d), but deserialized after",
                                         pm.message.id(), pm.arrive, pm.deserialize, m.message.id(), m.arrive, m.deserialize);
                                }
                            }
                            deserializingOnEventLoop.add(m);
                        }
                        else
                        {
                            deserializingOffEventLoop.add(m);
                        }
                        arriving.remove(mi);
                        m.update(next.type);
                        break;
                    }
                    case FAILED_DESERIALIZE:
                    {
                        // TODO: verify this failure to deserialize was intended by test
                        SimpleMessageEvent e = onFailedDeserialize(next);
                        assert nextId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        assert m.state == DESERIALIZE;
                        (m.processOnEventLoop ? deserializingOnEventLoop : deserializingOffEventLoop).remove(m);
                        break;
                    }
                    case PROCESS:
                    {
                        ProcessMessageEvent e = onProcessed(next);
                        assert nextId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        if (m == null)
                            break;

                        assert m.state == DESERIALIZE;
                        canonicalBytesInFlight -= m.message.serializedSize(m.messagingVersion);
                        if (!Arrays.equals((byte[]) e.message.payload, (byte[]) m.message.payload))
                        {
                            fail("Invalid message payload for %d: %s supplied by processor, but %s implied by original message and messaging version",
                                 e.messageId, Arrays.toString((byte[]) e.message.payload), Arrays.toString((byte[]) m.message.payload));
                        }
                        if (e.size != m.message.serializedSize(m.messagingVersion))
                        {
                            fail("Invalid serialized size for %d: %d supplied by processor, but %d implied by original message and messaging version",
                                 e.messageId, e.size, m.message.serializedSize(m.messagingVersion));
                        }
                        if (m.processOutOfOrder)
                        {
                            assert !m.processOnEventLoop; // will have already been reported small (processOnEventLoop) messages
                            processingOutOfOrder.remove(m);
                        }
                        else if (m.processOnEventLoop)
                        {
                            // we can expect that processing happens sequentially in this case, more specifically
                            // we can actually expect that this event will occur _immediately_ after the deserialize event
                            // so that we have exactly one mess
                            // c
                            int mi = deserializingOnEventLoop.indexOf(m);
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = deserializingOnEventLoop.get(i);
                                fail("Invalid order of events: %s deserialized strictly before %s, but processed after",
                                     pm, m);
                            }
                            clearFirst(mi, deserializingOnEventLoop, messages);
                            deserializingOnEventLoop.poll();
                        }
                        else
                        {
                            int mi = deserializingOffEventLoop.indexOf(m);
                            // process may be off-loaded, so we can only impose meaningful ordering constraints
                            // on those messages we know to have been processed on the event loop
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = deserializingOffEventLoop.get(i);
                                pm.processOutOfOrder = true;
                                processingOutOfOrder.add(pm);
                            }
                            deserializingOffEventLoop.removeFirst(mi + 1);
                        }
                        // this message has been fully validated
                        break;
                    }
                    case FAILED_EXPIRED:
                    {
                        ExpiredMessageEvent e = onExpired(next);
                        assert nextId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        switch (e.expirationType)
                        {
                            case ON_SENT:
                                if (m.state != ENQUEUE)
                                    fail("Invalid expiry on sent of %s", m);
                                break;
                            case ON_ARRIVED:
                                if (outboundType == LARGE_MESSAGES ? m.state != SERIALIZE
                                                                   : m.state != SEND_FRAME && m.state != SENT_FRAME && m.state != FAILED_FRAME)
                                    fail("Invalid expiry on arrival of %s", m);
                                break;
                            case ON_PROCESSED:
                                if (m.state != ARRIVE)
                                    fail("Invalid expiry on processed of %s", m);
                                break;
                        }

                        now = System.nanoTime();
                        if (m.expiresAtNanos > now)
                        {
                            // we fix the conversion AlmostSameTime for an entire run, which should suffice to guarantee these comparisons
                            fail("Invalid expiry of %d: expiry should occur in %dms; event believes %dms have elapsed, and %dms have actually elapsed", m.message.id(),
                                 NANOSECONDS.toMillis(m.expiresAtNanos - m.message.createdAtNanos()),
                                 e.timeUnit.toMillis(e.timeElapsed),
                                 NANOSECONDS.toMillis(now - m.message.createdAtNanos()));
                        }

                        switch (m.state)
                        {
                            case ENQUEUE: enqueueing.remove(m); break;
                            case DESERIALIZE: (m.processOnEventLoop ? deserializingOnEventLoop : deserializingOffEventLoop).remove(m); break;
                            case SERIALIZE: serializing.remove(m); break;
                        }
                        break;
                    }
                    case CONTROLLER_UPDATE:
                    {
                        break;
                    }
                }
                ++nextId;
            }
        }
        catch (InterruptedException e)
        {
        }
        catch (Throwable t)
        {
            logger.error("Unexpected error:", t);
            onFailure.run();
        }
    }

    private static class Frame extends Queue<MessageState>
    {
        enum Status { SUCCESS, FAILED, UNKNOWN }
        Status sendStatus = Status.UNKNOWN, receiveStatus = Status.UNKNOWN;
        int messagingVersion;
        int messageCount;
        int payloadSizeInBytes;

        Frame reset()
        {
            sendStatus = receiveStatus = Status.UNKNOWN;
            messagingVersion = messageCount = payloadSizeInBytes = 0;
            return this;
        }

        public String toString()
        {
            return String.format("{count:%d, size:%d, version:%d, send:%s, receive:%s}",
                                 messageCount, payloadSizeInBytes, messagingVersion, sendStatus, receiveStatus);
        }
    }

    private static MessageState remove(long messageId, Queue<MessageState> queue, LongObjectOpenHashMap<MessageState> lookup)
    {
        MessageState m = lookup.remove(messageId);
        queue.remove(m);
        return m;
    }

    private static void clearFirst(int count, Queue<MessageState> queue, LongObjectOpenHashMap<MessageState> lookup)
    {
        if (count > 0)
        {
            for (int i = 0 ; i < count ; ++i)
                lookup.remove(queue.get(i).message.id());
            queue.removeFirst(count);
        }
    }

    private static void clear(Queue<MessageState> queue, LongObjectOpenHashMap<MessageState> lookup)
    {
        if (!queue.isEmpty())
            clearFirst(queue.size(), queue, lookup);
    }

    private static class EventSequence
    {
        static final int CHUNK_SIZE = 1 << 10;
        static class Chunk extends AtomicReferenceArray<Event>
        {
            final long sequenceId;
            int removed = 0;
            Chunk(long sequenceId)
            {
                super(CHUNK_SIZE);
                this.sequenceId = sequenceId;
            }
            Event get(long sequenceId)
            {
                return get((int)(sequenceId - this.sequenceId));
            }
            void set(long sequenceId, Event event)
            {
                set((int)(sequenceId - this.sequenceId), event);
            }
        }

        // we use a concurrent skip list to permit efficient searching, even if we always append
        final ConcurrentSkipListMap<Long, Chunk> chunkList = new ConcurrentSkipListMap<>();
        final WaitQueue writerWaiting = new WaitQueue();

        volatile Chunk writerChunk = new Chunk(0);
        Chunk readerChunk = writerChunk;

        long readerWaitingFor;
        volatile Thread readerWaiting;

        EventSequence()
        {
            chunkList.put(0L, writerChunk);
        }

        public void put(long sequenceId, Event event)
        {
            long chunkSequenceId = sequenceId & -CHUNK_SIZE;
            Chunk chunk = writerChunk;
            if (chunk.sequenceId != chunkSequenceId)
            {
                try
                {
                    chunk = ensureChunk(chunkSequenceId);
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            }

            chunk.set(sequenceId, event);

            Thread wake = readerWaiting;
            long wakeIf = readerWaitingFor; // we are guarded by the above volatile read
            if (wake != null && wakeIf == sequenceId)
                LockSupport.unpark(wake);
        }

        Chunk ensureChunk(long chunkSequenceId) throws InterruptedException
        {
            Chunk chunk = chunkList.get(chunkSequenceId);
            if (chunk == null)
            {
                Map.Entry<Long, Chunk> e;
                while ( null != (e = chunkList.firstEntry()) && chunkSequenceId - e.getKey() > 1 << 12)
                {
                    WaitQueue.Signal signal = writerWaiting.register();
                    if (null != (e = chunkList.firstEntry()) && chunkSequenceId - e.getKey() > 1 << 12)
                        signal.await();
                    else
                        signal.cancel();
                }
                chunk = chunkList.get(chunkSequenceId);
                if (chunk == null)
                {
                    synchronized (this)
                    {
                        chunk = chunkList.get(chunkSequenceId);
                        if (chunk == null)
                            chunkList.put(chunkSequenceId, chunk = new Chunk(chunkSequenceId));
                    }
                }
            }
            return chunk;
        }

        Chunk readerChunk(long readerId) throws InterruptedException
        {
            long chunkSequenceId = readerId & -CHUNK_SIZE;
            if (readerChunk.sequenceId != chunkSequenceId)
                readerChunk = ensureChunk(chunkSequenceId);
            return readerChunk;
        }

//        public Event peek()
//        {
//            return readerChunk(rea).get(readerId);
//        }
//
//        public Event poll()
//        {
//            Chunk chunk = readerChunk();
//            Event event = chunk.get(readerId);
//            if (event != null)
//                ++readerId;
//            return event;
//        }
//
        public Event await(long id, long timeout, TimeUnit unit) throws InterruptedException
        {
            return await(id, System.nanoTime() + unit.toNanos(timeout));
        }

        public Event await(long id, long deadlineNanos) throws InterruptedException
        {
            Chunk chunk = readerChunk(id);
            Event result = chunk.get(id);
            if (result != null)
                return result;

            readerWaitingFor = id;
            readerWaiting = Thread.currentThread();
            while (null == (result = chunk.get(id)))
            {
                long waitNanos = deadlineNanos - System.nanoTime();
                if (waitNanos <= 0)
                    return null;
                LockSupport.parkNanos(waitNanos);
                if (Thread.interrupted())
                    throw new InterruptedException();
            }
            readerWaitingFor = -1;
            readerWaiting = null;
            return result;
        }

        public Event find(long sequenceId)
        {
            long chunkSequenceId = sequenceId & -CHUNK_SIZE;
            Chunk chunk = readerChunk;
            if (chunk.sequenceId != chunkSequenceId)
            {
                chunk = writerChunk;
                if (chunk.sequenceId != chunkSequenceId)
                    chunk = chunkList.get(chunkSequenceId);
            }
            return chunk.get(sequenceId);
        }

        public void clear(long sequenceId)
        {
            long chunkSequenceId = sequenceId & -CHUNK_SIZE;
            Chunk chunk = chunkList.get(chunkSequenceId);
            if (++chunk.removed == CHUNK_SIZE)
            {
                chunkList.remove(chunkSequenceId);
                writerWaiting.signalAll();
            }
        }
    }

    private static class MessageState
    {
        final Message<?> message; // temporary, only maintained until serialization to calculate actualSize
        int messagingVersion;
        // set initially to message.expiresAtNanos, but if at serialization time we use
        // an older messaging version we may not be able to serialize expiration
        long expiresAtNanos;
        long enqueueStart, enqueueEnd, serialize, arrive, deserialize;
        boolean processOnEventLoop, processOutOfOrder;
        EventType state;
        long lastUpdateNanos;

        MessageState(Message<?> message, long enqueueStart)
        {
            this.enqueueStart = enqueueStart;
            this.message = message;
            this.expiresAtNanos = message.expiresAtNanos();
        }

        void update(EventType state)
        {
            this.state = state;
            this.lastUpdateNanos = ApproximateTime.nanoTime();
        }

        public String toString()
        {
            return String.format("{id:%d, ver:%d, state:%s, enqueue:[%d,%d], ser:%d, arr:%d, deser:%d}",
                                 message.id(), messagingVersion, state, enqueueStart, enqueueEnd, serialize, arrive, deserialize);
        }
    }

    static class Queue<T>
    {
        private Object[] items = new Object[10];
        private int begin, end;

        int size()
        {
            return end - begin;
        }

        T get(int i)
        {
            return (T) items[i + begin];
        }

        int indexOf(T item)
        {
            for (int i = begin ; i < end ; ++i)
            {
                if (item == items[i])
                    return i - begin;
            }
            return -1;
        }

        void remove(T item)
        {
            int i = indexOf(item);
            if (i >= 0)
                remove(i);
        }

        void remove(int i)
        {
            i += begin;
            assert i < end;

            if (i == begin || i + 1 == end)
            {
                items[i] = null;
                if (begin + 1 == end) begin = end = 0;
                else if (i == begin) ++begin;
                else --end;
            }
            else if (i - begin < end - i)
            {
                System.arraycopy(items, begin, items, begin + 1, i - begin);
                items[begin++] = null;
            }
            else
            {
                System.arraycopy(items, i + 1, items, i, (end - 1) - i);
                items[--end] = null;
            }
        }

        void add(T item)
        {
            if (end == items.length)
            {
                Object[] src = items;
                Object[] trg;
                if (end - begin < src.length / 2) trg = src;
                else trg = new Object[src.length * 2];
                System.arraycopy(src, begin, trg, 0, end - begin);
                end -= begin;
                begin = 0;
                items = trg;
            }
            items[end++] = item;
        }

        void clear()
        {
            Arrays.fill(items, begin, end, null);
            begin = end = 0;
        }

        void removeFirst(int count)
        {
            Arrays.fill(items, begin, begin + count, null);
            begin += count;
            if (begin == end)
                begin = end = 0;
        }

        T poll()
        {
            if (begin == end)
                return null;
            T result = (T) items[begin];
            items[begin++] = null;
            if (begin == end)
                begin = end = 0;
            return result;
        }

        T peek()
        {
            return (T) items[begin];
        }

        boolean isEmpty()
        {
            return begin == end;
        }
    }



    class FramesInFlight extends Queue<Frame>
    {
        private int framesWithStatus;

        Frame supplySendStatus(Frame.Status status)
        {
            Frame frame = null;
            if (framesWithStatus >= 0)
            {
                frame = get(framesWithStatus);
                assert frame.sendStatus == Frame.Status.UNKNOWN;
                frame.sendStatus = status;
            }
            ++framesWithStatus;
            return frame;
        }

        void remove(int i)
        {
            if (i > framesWithStatus)
                throw new IllegalArgumentException();
            super.remove(i);
        }

        void clear()
        {
            framesWithStatus -= size();
            super.clear();
        }

        void removeFirst(int count)
        {
            framesWithStatus -= count;
            super.removeFirst(count);
        }

        Frame poll()
        {
            --framesWithStatus;
            return super.poll();
        }
    }

    private static boolean willProcessOnEventLoop(ConnectionType type, Message<?> message, int messagingVersion)
    {
        int size = message.serializedSize(messagingVersion);
        if (type == ConnectionType.SMALL_MESSAGES && messagingVersion >= VERSION_40)
            return size <= LARGE_MESSAGE_THRESHOLD;
        else if (messagingVersion >= VERSION_40)
            return size <= DEFAULT_BUFFER_SIZE;
        else
            return size <= LARGE_MESSAGE_THRESHOLD;
    }

    private static long expiresAtNanos(Message<?> message, int messagingVersion)
    {
        return messagingVersion < VERSION_40 ? message.verb().expiresAtNanos(message.createdAtNanos())
                                             : message.expiresAtNanos();
    }

}
