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
 *
 * TODO: timeliness of events
 * TODO: periodically stop all activity to/from a given endpoint, until it stops (and verify queues all empty, counters all accurate)
 * TODO: integrate with proxy that corrupts frames
 * TODO: test _OutboundConnection_ close
 * TODO: verify that we do not process very expired messages
 */
@SuppressWarnings("WeakerAccess")
public class Verifier
{
    private static final Logger logger = LoggerFactory.getLogger(Verifier.class);

    public enum Destiny
    {
        SUCCEED,
        FAIL_TO_SERIALIZE,
        FAIL_TO_DESERIALIZE,
    }

    enum EventType
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

        CONNECT,
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
        BoundedEvent(EventType type, long start)
        {
            super(type);
            this.start = start;
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
        BoundedMessageEvent(EventType type, long start, long messageId)
        {
            super(type, start);
            this.messageId = messageId;
        }
    }

    static class EnqueueMessageEvent extends BoundedMessageEvent
    {
        final Message<?> message;
        final Destiny destiny;
        EnqueueMessageEvent(EventType type, long start, Message<?> message, Destiny destiny)
        {
            super(type, start, message.id());
            this.message = message;
            this.destiny = destiny;
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

    static class SimpleMessageEventWithSize extends SimpleMessageEvent
    {
        final int messageSize;
        SimpleMessageEventWithSize(EventType type, long at, long messageId, int messageSize)
        {
            super(type, at, messageId);
            this.messageSize = messageSize;
        }
    }

    static class ExpiredMessageEvent extends SimpleMessageEvent
    {
        enum ExpirationType {ON_SENT, ON_ARRIVED, ON_PROCESSED }
        final int messageSize;
        final long timeElapsed;
        final TimeUnit timeUnit;
        final ExpirationType expirationType;
        ExpiredMessageEvent(long at, long messageId, int messageSize, long timeElapsed, TimeUnit timeUnit, ExpirationType expirationType)
        {
            super(FAILED_EXPIRED, at, messageId);
            this.messageSize = messageSize;
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
        final Message<?> message;
        ProcessMessageEvent(long at, Message<?> message)
        {
            super(PROCESS, at, message.id());
            this.message = message;
        }
    }

    EnqueueMessageEvent onEnqueue(Message<?> message, Destiny destiny)
    {
        EnqueueMessageEvent enqueue = new EnqueueMessageEvent(ENQUEUE, nextId(), message, destiny);
        events.put(enqueue.start, enqueue);
        return enqueue;
    }
    void onOverloaded(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(FAILED_OVERLOADED, at, messageId));
    }
    void onFailedClosing(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(FAILED_CLOSING, at, messageId));
    }
    void onSerialize(long messageId, int messagingVersion)
    {
        long at = nextId();
        events.put(at, new SerializeMessageEvent(SERIALIZE, at, messageId, messagingVersion));
    }
    void onFailedSerialize(long messageId)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEvent(FAILED_SERIALIZE, at, messageId));
    }
    void onExpiredBeforeSend(long messageId, int messageSize, long timeElapsed, TimeUnit timeUnit)
    {
        onExpired(messageId, messageSize, timeElapsed, timeUnit, ExpirationType.ON_SENT);
    }
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
        events.put(at, new FrameEvent(FAILED_FRAME, at, messageCount, payloadSizeInBytes));
    }
    void onArrived(long messageId, int messageSize)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEventWithSize(ARRIVE, at, messageId, messageSize));
    }
    void onArrivedExpired(long messageId, int messageSize, long timeElapsed, TimeUnit timeUnit)
    {
        onExpired(messageId, messageSize, timeElapsed, timeUnit, ExpirationType.ON_ARRIVED);
    }
    void onDeserialize(long messageId, int messagingVersion)
    {
        long at = nextId();
        events.put(at, new SerializeMessageEvent(DESERIALIZE, at, messageId, messagingVersion));
    }
    void onFailedDeserialize(long messageId, int messageSize)
    {
        long at = nextId();
        events.put(at, new SimpleMessageEventWithSize(FAILED_DESERIALIZE, at, messageId, messageSize));
    }
    void process(Message<?> message)
    {
        long at = nextId();
        events.put(at, new ProcessMessageEvent(at, message));
    }
    void onProcessExpired(long messageId, int messageSize, long timeElapsed, TimeUnit timeUnit)
    {
        onExpired(messageId, messageSize, timeElapsed, timeUnit, ExpirationType.ON_PROCESSED);
    }
    private void onExpired(long messageId, int messageSize, long timeElapsed, TimeUnit timeUnit, ExpirationType expirationType)
    {
        long at = nextId();
        events.put(at, new ExpiredMessageEvent(at, messageId, messageSize, timeElapsed, timeUnit, expirationType));
    }



    static class ConnectEvent extends SimpleEvent
    {
        final int messagingVersion;
        final OutboundConnectionSettings settings;
        ConnectEvent(long at, int messagingVersion, OutboundConnectionSettings settings)
        {
            super(EventType.CONNECT, at);
            this.messagingVersion = messagingVersion;
            this.settings = settings;
        }
    }

    static class ControllerEvent extends BoundedEvent
    {
        final long minimumBytesInFlight;
        final long maximumBytesInFlight;
        ControllerEvent(long start, long minimumBytesInFlight, long maximumBytesInFlight)
        {
            super(EventType.CONTROLLER_UPDATE, start);
            this.minimumBytesInFlight = minimumBytesInFlight;
            this.maximumBytesInFlight = maximumBytesInFlight;
        }
    }

    void onConnect(int messagingVersion, OutboundConnectionSettings settings)
    {
        ConnectEvent connect = new ConnectEvent(nextId(), messagingVersion, settings);
        events.put(connect.at, connect);
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
        logger.error("Connection: {}", currentConnection);
    }

    private void failinfo(String message, Object ... params)
    {
        logger.error("{}", String.format(message, params));
    }

    private static class MessageState
    {
        final Message<?> message;
        final Destiny destiny;
        int messagingVersion;
        // set initially to message.expiresAtNanos, but if at serialization time we use
        // an older messaging version we may not be able to serialize expiration
        long expiresAtNanos;
        long enqueueStart, enqueueEnd, serialize, arrive, deserialize;
        boolean processOnEventLoop, processOutOfOrder;
        EventType state;
        long lastUpdateNanos;
        ConnectionState sentOn;

        int messageSize()
        {
            return message.serializedSize(messagingVersion);
        }

        MessageState(Message<?> message, Destiny destiny, long enqueueStart)
        {
            this.message = message;
            this.destiny = destiny;
            this.enqueueStart = enqueueStart;
            this.expiresAtNanos = message.expiresAtNanos();
        }

        void update(EventType state)
        {
            this.state = state;
            this.lastUpdateNanos = ApproximateTime.nanoTime();
        }

        public String toString()
        {
            return String.format("{id:%d, ver:%d, state:%s, enqueue:[%d,%d], ser:%d, arr:%d, deser:%d, expires:%d, sentOn: %d}",
                                 message.id(), messagingVersion, state, enqueueStart, enqueueEnd, serialize, arrive, deserialize, ApproximateTime.toCurrentTimeMillis(expiresAtNanos), sentOn == null ? -1 : sentOn.connectionId);
        }
    }

    final LongObjectOpenHashMap<MessageState> messages = new LongObjectOpenHashMap<>();

    // messages start here, but may enter in a haphazard (non-sequential) fashion;
    // ENQUEUE_START, ENQUEUE_END both take place here, with the latter imposing bounds on the out-of-order appearance of messages.
    // note that ENQUEUE_END - being concurrent - may not appear before the message's lifespan has completely ended.
    final Queue<MessageState> enqueueing = new Queue<>();

    static final class ConnectionState
    {
        final long connectionId;
        final int messagingVersion;
        // Strict message order will then be determined at serialization time, since this happens on a single thread.
        // The order in which messages arrive here determines the order they will arrive on the other node.
        // must follow either ENQUEUE_START or ENQUEUE_END
        final Queue<MessageState> serializing = new Queue<>();

        // Messages sent on the small connection will all be sent in frames; this is a concurrent operation,
        // so only the sendingFrame MUST be encountered before any future events -
        // large connections skip this step and goes straight to arriving
        // we consult the queues in reverse order in arriving, as it is acceptable to find our frame in any of these queues
        final FramesInFlight framesInFlight = new FramesInFlight(); // unknown if the messages will arrive, accept either

        // for large messages OR < VERSION_40, arriving can occur BEFORE serializing completes successfully
        // OR a frame is fully serialized
        final Queue<MessageState> arriving = new Queue<>();

        final Queue<MessageState> deserializingOnEventLoop = new Queue<>(),
                                  deserializingOffEventLoop = new Queue<>();

        ConnectionState(long connectionId, int messagingVersion)
        {
            this.connectionId = connectionId;
            this.messagingVersion = messagingVersion;
        }

        public String toString()
        {
            return String.format("{id: %d, ver: %d, ser: %d, inFlight: %s, arriving: %d, deserOn: %d, deserOff: %d}",
                                 connectionId, messagingVersion, serializing.size(), framesInFlight, arriving.size(), deserializingOnEventLoop.size(), deserializingOffEventLoop.size());
        }
    }

    final Queue<Frame> reuseFrames = new Queue<>();
    final Queue<MessageState> processingOutOfOrder = new Queue<>();

    long canonicalBytesInFlight = 0;
    long nextMessageId = 0;
    long now;
    long connectionCounter;
    ConnectionState currentConnection = new ConnectionState(connectionCounter++, current_version);

    public void run(Runnable onFailure, long deadlineNanos)
    {
        try
        {
            while ((now = ApproximateTime.nanoTime()) < deadlineNanos)
            {
                Event next = events.await(nextMessageId, 100L, TimeUnit.MILLISECONDS);
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
                events.clear(nextMessageId); // TODO: simplify collection if we end up using it exclusively as a queue, as we are now

                switch (next.type)
                {
                    case ENQUEUE:
                    {
                        MessageState m;
                        EnqueueMessageEvent e = (EnqueueMessageEvent) next;
                        assert nextMessageId == e.start || nextMessageId == e.end;
                        assert e.message != null;
                        if (nextMessageId == e.start)
                        {
                            canonicalBytesInFlight += e.message.serializedSize(current_version);
                            m = new MessageState(e.message, e.destiny, e.start);
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
                        SimpleMessageEvent e = (SimpleMessageEvent) next;
                        assert nextMessageId == e.at;
                        MessageState m = remove(e.messageId, enqueueing, messages);
                        if (ENQUEUE != m.state)
                            fail("Invalid state at overload of %d: expected message in %s, found %s", m.message.id(), ENQUEUE, m.state);
                        break;
                    }
                    case FAILED_CLOSING:
                    {
                        // TODO: verify if this is acceptable due to e.g. inbound refusing to process for long enough
                        SimpleMessageEvent e = (SimpleMessageEvent) next;
                        assert nextMessageId == e.at;
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
                        SerializeMessageEvent e = (SerializeMessageEvent) next;
                        assert nextMessageId == e.at;
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
                            if (pm.enqueueEnd != 0 && pm.enqueueEnd < m.enqueueStart)
                            {
                                fail("Invalid order of events: %s enqueued strictly before %s, but serialized after",
                                     pm, m);
                            }
                        }
                        enqueueing.remove(mi);
                        m.sentOn = currentConnection;
                        currentConnection.serializing.add(m);
                        m.update(next.type);
                        break;
                    }
                    case FAILED_SERIALIZE:
                    {
                        SimpleMessageEvent e = (SimpleMessageEvent) next;
                        assert nextMessageId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        switch (m.state)
                        {
                            case ENQUEUE:
                                enqueueing.remove(m);
                                break;
                            case SERIALIZE:
                                m.sentOn.serializing.remove(m);
                                break;
                        }
                        if (m.destiny != Destiny.FAIL_TO_SERIALIZE)
                            fail("%s failed to serialize, but its destiny was to %s", m, m.destiny);
                        break;
                    }
                    case SEND_FRAME:
                    {
                        FrameEvent e = (FrameEvent) next;
                        assert nextMessageId == e.at;
                        int size = 0;
                        Frame frame = reuseFrames.poll();
                        if (frame == null) frame = new Frame();
                        MessageState first = currentConnection.serializing.get(0);
                        int messagingVersion = first.messagingVersion;
                        for (int i = 0 ; i < e.messageCount ; ++i)
                        {
                            MessageState m = currentConnection.serializing.get(i);
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
                        currentConnection.framesInFlight.add(frame);
                        currentConnection.serializing.removeFirst(e.messageCount);
                        if (e.payloadSizeInBytes != size)
                            fail("Invalid frame payload size with %s: expected %d, actual %d", first,  size, e.payloadSizeInBytes);
                        break;
                    }
                    case SENT_FRAME:
                    {
                        currentConnection.framesInFlight.supplySendStatus(Frame.Status.SUCCESS);
                        break;
                    }
                    case FAILED_FRAME:
                    {
                        // TODO: is it possible for this to be signalled AFTER our reconnect event? probably, in which case this will be wrong
                        // TODO: verify that this was expected
                        Frame frame = currentConnection.framesInFlight.supplySendStatus(Frame.Status.FAILED);
                        if (frame != null && frame.messagingVersion >= VERSION_40)
                        {
                            // the contents cannot be delivered without the whole frame arriving, so clear the contents now
                            clear(frame, messages);
                            currentConnection.framesInFlight.remove(frame);
                            reuseFrames.add(frame.reset());
                        }
                        break;
                    }
                    case ARRIVE:
                    {
                        SimpleMessageEventWithSize e = (SimpleMessageEventWithSize) next;
                        assert nextMessageId == e.at;
                        MessageState m = messages.get(e.messageId);
                        if (m == null)
                            break; // TODO: this occurs because large messages can have previously failed, but we should perhaps be more robust in accounting for this

                        m.arrive = e.at;
                        if (e.messageSize != m.messageSize())
                            fail("onArrived with invalid size for %s: %d vs %d", m, e.messageSize, m.messageSize());

                        if (outboundType == LARGE_MESSAGES)
                        {
                            if (m.state != SERIALIZE)
                            {
                                fail("Invalid state of %s (expect %s)", m, SERIALIZE);
                                break;
                            }
                            int mi = m.sentOn.serializing.indexOf(m);
                            for (int i = 0; i < mi; ++i)
                                fail("Invalid order of events: %s serialized to large stream strictly before %s, but arrived after", m.sentOn.serializing.get(i), m);
                            m.sentOn.serializing.remove(mi);
                        }
                        else
                        {
                            if (m.state != SEND_FRAME)
                            {
                                fail("Invalid order of events: %s arrived before being sent in a frame", m);
                                break;
                            }

                            int fi = -1, mi = -1;
                            while (fi + 1 < m.sentOn.framesInFlight.size() && mi < 0)
                                mi = m.sentOn.framesInFlight.get(++fi).indexOf(m);

                            if (fi == m.sentOn.framesInFlight.size())
                            {
                                fail("Invalid state: %s, but no frame in flight was found to contain it", m);
                                break;
                            }

                            if (fi > 0)
                            {
                                // we have skipped over some frames, meaning these have either failed (and we know it)
                                // or we have not yet heard about them and they have presumably failed, or something
                                // has gone wrong
                                fail("BEGIN: Successfully sent frames were not delivered");
                                for (int i = 0 ; i < fi ; ++i)
                                {
                                    Frame skip = m.sentOn.framesInFlight.get(i);
                                    skip.receiveStatus = Frame.Status.FAILED;
                                    if (skip.sendStatus == Frame.Status.SUCCESS)
                                    {
                                        failinfo("Frame %s", skip);
                                        for (int j = 0 ; j < skip.size() ; ++j)
                                            failinfo("Containing: %s", skip.get(j));
                                    }
                                    clear(skip, messages);
                                    reuseFrames.add(skip.reset());
                                }
                                m.sentOn.framesInFlight.removeFirst(fi);
                                failinfo("END: Successfully sent frames were not delivered");
                            }

                            Frame frame = m.sentOn.framesInFlight.get(0);
                            for (int i = 0; i < mi; ++i)
                                fail("Invalid order of events: %s serialized strictly before %s, but arrived after", frame.get(i), m);

                            frame.remove(mi);
                            if (frame.isEmpty())
                            {
                                m.sentOn.framesInFlight.poll();
                                reuseFrames.add(frame.reset());
                            }
                        }
                        m.sentOn.arriving.add(m);
                        m.update(next.type);
                        break;
                    }
                    case DESERIALIZE:
                    {
                        // deserialize may happen in parallel for large messages, but in sequence for small messages
                        // we currently require that this event be issued before any possible error is thrown
                        SimpleMessageEvent e = (SimpleMessageEvent) next;
                        assert nextMessageId == e.at;
                        MessageState m = messages.get(e.messageId);
                        if (m.state != ARRIVE)
                        {
                            fail("Invalid state of %s (expect %s)", m, ARRIVE);
                            break;
                        }
                        m.deserialize = e.at;
                        // deserialize may be off-loaded, so we can only impose meaningful ordering constraints
                        // on those messages we know to have been processed on the event loop
                        int mi = m.sentOn.arriving.indexOf(m);
                        if (m.processOnEventLoop)
                        {
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = m.sentOn.arriving.get(i);
                                if (pm.processOnEventLoop)
                                {
                                    fail("Invalid order of events: %d (%d, %d) arrived strictly before %d (%d, %d), but deserialized after",
                                         pm.message.id(), pm.arrive, pm.deserialize, m.message.id(), m.arrive, m.deserialize);
                                }
                            }
                            m.sentOn.deserializingOnEventLoop.add(m);
                        }
                        else
                        {
                            m.sentOn.deserializingOffEventLoop.add(m);
                        }
                        m.sentOn.arriving.remove(mi);
                        m.update(next.type);
                        break;
                    }
                    case FAILED_DESERIALIZE:
                    {
                        SimpleMessageEventWithSize e = (SimpleMessageEventWithSize) next;
                        assert nextMessageId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        if (e.messageSize != m.messageSize())
                            fail("onFailedDeserialize has invalid size for %s: %d vs %d", m, e.messageSize, m.messageSize());
                        if (m.state != DESERIALIZE)
                        {
                            fail("Invalid state of %s (expect %s)", m, DESERIALIZE);
                            break;
                        }
                        (m.processOnEventLoop ? m.sentOn.deserializingOnEventLoop : m.sentOn.deserializingOffEventLoop).remove(m);
                        if (m.destiny != Destiny.FAIL_TO_DESERIALIZE)
                            fail("%s failed to deserialize, but its destiny was to %s", m, m.destiny);
                        break;
                    }
                    case PROCESS:
                    {
                        ProcessMessageEvent e = (ProcessMessageEvent) next;
                        assert nextMessageId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        if (m == null)
                            break;

                        if (m.state != DESERIALIZE)
                        {
                            fail("Invalid state of %s (expect %s)", m, DESERIALIZE);
                            break;
                        }
                        canonicalBytesInFlight -= m.message.serializedSize(m.messagingVersion);
                        if (!Arrays.equals((byte[]) e.message.payload, (byte[]) m.message.payload))
                        {
                            fail("Invalid message payload for %d: %s supplied by processor, but %s implied by original message and messaging version",
                                 e.messageId, Arrays.toString((byte[]) e.message.payload), Arrays.toString((byte[]) m.message.payload));
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
                            int mi = m.sentOn.deserializingOnEventLoop.indexOf(m);
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = m.sentOn.deserializingOnEventLoop.get(i);
                                fail("Invalid order of events: %s deserialized strictly before %s, but processed after",
                                     pm, m);
                            }
                            clearFirst(mi, m.sentOn.deserializingOnEventLoop, messages);
                            m.sentOn.deserializingOnEventLoop.poll();
                        }
                        else
                        {
                            int mi = m.sentOn.deserializingOffEventLoop.indexOf(m);
                            // process may be off-loaded, so we can only impose meaningful ordering constraints
                            // on those messages we know to have been processed on the event loop
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = m.sentOn.deserializingOffEventLoop.get(i);
                                pm.processOutOfOrder = true;
                                processingOutOfOrder.add(pm);
                            }
                            m.sentOn.deserializingOffEventLoop.removeFirst(mi + 1);
                        }
                        // this message has been fully validated
                        break;
                    }
                    case FAILED_EXPIRED:
                    {
                        ExpiredMessageEvent e = (ExpiredMessageEvent) next;
                        assert nextMessageId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        switch (e.expirationType)
                        {
                            case ON_SENT:
                                if (m.state != ENQUEUE)
                                    fail("Invalid state onExpiry ON_SENT of %s", m);
                                break;
                            case ON_ARRIVED:
                                if (m.state != ARRIVE && // can be deferred and return to expire on arrival
                                    (outboundType == LARGE_MESSAGES ? m.state != SERIALIZE
                                                                    : m.state != SEND_FRAME && m.state != SENT_FRAME && m.state != FAILED_FRAME))
                                    fail("Invalid state onExpiry ON_ARRIVED of %s", m);
                                break;
                            case ON_PROCESSED:
                                if (m.state != ARRIVE)
                                    fail("Invalid state onExpiry ON_PROCESSED of %s", m);
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
                            case DESERIALIZE: (m.processOnEventLoop ? m.sentOn.deserializingOnEventLoop : m.sentOn.deserializingOffEventLoop).remove(m); break;
                            case SEND_FRAME: case SENT_FRAME: case FAILED_FRAME:
                                // TODO: this should be robust to re-ordering; should perhaps extract a common method
                                m.sentOn.framesInFlight.get(0).remove(m);
                                if (m.sentOn.framesInFlight.get(0).isEmpty())
                                    m.sentOn.framesInFlight.removeFirst(1);
                                break;
                            case SERIALIZE: m.sentOn.serializing.remove(m); break;
                            case ARRIVE: m.sentOn.arriving.remove(m); break;
                            default: throw new IllegalStateException(m.state.toString());
                        }

                        if (m.messagingVersion != 0 && e.messageSize != m.messageSize())
                            fail("onExpired %s with invalid size for %s: %d vs %d", e.expirationType, m, e.messageSize, m.messageSize());

                        break;
                    }
                    case CONTROLLER_UPDATE:
                    {
                        break;
                    }
                    case CONNECT:
                    {
                        ConnectEvent e = (ConnectEvent) next;
                        currentConnection = new ConnectionState(connectionCounter++, e.messagingVersion);
                        break;
                    }
                    default:
                        throw new IllegalStateException();
                }
                ++nextMessageId;
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
            //noinspection unchecked
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
            //noinspection unchecked
            T result = (T) items[begin];
            items[begin++] = null;
            if (begin == end)
                begin = end = 0;
            return result;
        }

        boolean isEmpty()
        {
            return begin == end;
        }

        public String toString()
        {
            StringBuilder result = new StringBuilder();
            result.append('[');
            toString(result);
            result.append(']');
            return result.toString();
        }

        void toString(StringBuilder out)
        {
            for (int i = 0 ; i < size() ; ++i)
            {
                if (i > 0) out.append(", ");
                out.append(get(i));
            }
        }
    }



    static class FramesInFlight extends Queue<Frame>
    {
        // this may be negative, indicating we have processed a frame whose status we did not know at the time
        // TODO: we should verify the status of these frames by logging the inferred status and verifying it matches
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
            --framesWithStatus;
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

        public String toString()
        {
            StringBuilder result = new StringBuilder();
            result.append("[withStatus=");
            result.append(framesWithStatus);
            result.append(", ");
            toString(result);
            result.append(']');
            return result.toString();
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
