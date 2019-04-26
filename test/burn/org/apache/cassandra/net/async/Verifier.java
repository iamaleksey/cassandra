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
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.OutboundConnection.LargeMessageDelivery.DEFAULT_BUFFER_SIZE;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
import static org.apache.cassandra.net.async.Verifier.EventType.ARRIVE;
import static org.apache.cassandra.net.async.Verifier.EventType.DESERIALIZE;
import static org.apache.cassandra.net.async.Verifier.EventType.ENQUEUE;
import static org.apache.cassandra.net.async.Verifier.EventType.FAIL_EXPIRED;
import static org.apache.cassandra.net.async.Verifier.EventType.PROCESS;
import static org.apache.cassandra.net.async.Verifier.EventType.SERIALIZE;

/**
 * This class is a single-threaded verifier monitoring a single link, with events supplied by inbound and outbound threads
 *
 * By making verification single threaded, it is easier to reason about (and complex enough as is), but also permits
 * a dedicated thread to monitor timeliness of events, e.g. elapsed time between a given SEND and its corresponding RECEIVE
 */
public class Verifier
{
    private static final Logger logger = LoggerFactory.getLogger(Verifier.class);

    public enum EventType
    {
        ENQUEUE,
        SERIALIZE,
        ARRIVE,
        DESERIALIZE,
        PROCESS,
        FAIL_EXPIRED,
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

    private static class MessageState
    {
        final Message<?> message; // temporary, only maintained until serialization to calculate actualSize
        int messagingVersion;
        long enqueueStart, enqueueEnd, serialize, arrive, deserialize;
        boolean processOnEventLoop, processOutOfOrder;
        EventType state;
        long lastUpdateNanos;

        MessageState(Message<?> message, long enqueueStart)
        {
            this.enqueueStart = enqueueStart;
            this.message = message;
        }

        void update(EventType state)
        {
            this.state = state;
            this.lastUpdateNanos = ApproximateTime.nanoTime();
        }
    }

    public static class SimpleEvent extends Event
    {
        final long at;
        SimpleEvent(EventType type, long at)
        {
            super(type);
            this.at = at;
        }
    }

    public static class BoundedEvent extends Event
    {
        final long start;
        volatile long end;
        BoundedEvent(EventType type, Verifier verifier)
        {
            super(type);
            this.start = verifier.sequenceId.getAndIncrement();
        }
        public void complete(Verifier verifier) throws InterruptedException
        {
            end = verifier.sequenceId.getAndIncrement();
            verifier.sequence.put(end, this);
        }
    }

    public static class SimpleMessageEvent extends SimpleEvent
    {
        final long messageId;
        SimpleMessageEvent(EventType type, long at, long messageId)
        {
            super(type, at);
            this.messageId = messageId;
        }
    }

    public static class BoundedMessageEvent extends BoundedEvent
    {
        final long messageId;
        BoundedMessageEvent(EventType type, Verifier verifier, long messageId)
        {
            super(type, verifier);
            this.messageId = messageId;
        }
    }

    public static class EnqueueMessageEvent extends BoundedMessageEvent
    {
        final Message<?> message;
        EnqueueMessageEvent(EventType type, Verifier verifier, Message<?> message)
        {
            super(type, verifier, message.id());
            this.message = message;
        }
    }

    public static class SerializeMessageEvent extends SimpleMessageEvent
    {
        final int messagingVersion;
        SerializeMessageEvent(EventType type, long at, long messageId, int messagingVersion)
        {
            super(type, at, messageId);
            this.messagingVersion = messagingVersion;
        }
    }

    public static class ExpiredMessageEvent extends SimpleMessageEvent
    {
        final int size;
        final long timeElapsed;
        final TimeUnit timeUnit;
        final EventType expectState;
        ExpiredMessageEvent(long at, long messageId, int size, long timeElapsed, TimeUnit timeUnit, EventType expectState)
        {
            super(FAIL_EXPIRED, at, messageId);
            this.size = size;
            this.timeElapsed = timeElapsed;
            this.timeUnit = timeUnit;
            this.expectState = expectState;
        }
    }

    public static class ProcessMessageEvent extends SimpleMessageEvent
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

    public static class ControllerEvent extends BoundedEvent
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

    EnqueueMessageEvent enqueue(Message<?> message) throws InterruptedException
    {
        EnqueueMessageEvent enqueue = new EnqueueMessageEvent(ENQUEUE, this, message);
        sequence.put(enqueue.start, enqueue);
        return enqueue;
    }
    private static EnqueueMessageEvent enqueue(Event e) { return (EnqueueMessageEvent)e; }

    void serialize(long messageId, int messagingVersion) 
    {
        long at = nextId();
        sequence.put(at, new SerializeMessageEvent(SERIALIZE, at, messageId, messagingVersion));
    }
    private static SerializeMessageEvent serialize(Event e) { return (SerializeMessageEvent)e; }

    void arrive(long messageId)
    {
        long at = nextId();
        sequence.put(at, new SimpleMessageEvent(ARRIVE, at, messageId));
    }
    private static SimpleMessageEvent arrive(Event e) { return (SimpleMessageEvent)e; }

    void deserialize(long messageId, int messagingVersion) 
    {
        long at = nextId();
        sequence.put(at, new SerializeMessageEvent(DESERIALIZE, at, messageId, messagingVersion));
    }
    private static SimpleMessageEvent deserialize(Event e) { return (SimpleMessageEvent)e; }

    void process(Message<?> message, int size) 
    {
        long at = nextId();
        sequence.put(at, new ProcessMessageEvent(at, message, size));
    }
    private static ProcessMessageEvent process(Event e) { return (ProcessMessageEvent)e; }

    void expireOnSend(long messageId, int size, long timeElapsed, TimeUnit timeUnit)
    {
        expired(messageId, size, timeElapsed, timeUnit, ENQUEUE);
    }
    void expireOnArrival(long messageId, int size, long timeElapsed, TimeUnit timeUnit)
    {
        expired(messageId, size, timeElapsed, timeUnit, SERIALIZE);
    }
    void processExpired(long messageId, int size, long timeElapsed, TimeUnit timeUnit)
    {
        expired(messageId, size, timeElapsed, timeUnit, DESERIALIZE);
    }
    private void expired(long messageId, int size, long timeElapsed, TimeUnit timeUnit, EventType expectState)
    {
        long at = nextId();
        sequence.put(at, new ExpiredMessageEvent(at, messageId, size, timeElapsed, timeUnit, expectState));
    }
    private static ExpiredMessageEvent expired(Event e) { return (ExpiredMessageEvent)e; }

    final AtomicLong sequenceId = new AtomicLong();
    final Sequence sequence = new Sequence();
    final ConnectionType outboundType;

    private long nextId()
    {
        return sequenceId.getAndIncrement();
    }

    public Verifier(ConnectionType outboundType)
    {
        this.outboundType = outboundType;
    }

    private void fail(String message, Object ... params)
    {
        logger.error("{}", String.format(message, params));
    }

    public void run(Runnable onFailure, long deadlineNanos)
    {
        try
        {
            // TODO: - verify timeliness of all messages, not just those arriving out of order
            //         (integrate bytes in flight controller info into timeliness decisions)
            //       - indicate a message is expected to fail serialization (or deserialization)
            final LongObjectOpenHashMap<MessageState> messages = new LongObjectOpenHashMap<>();
            final Messages enqueue = new Messages(m -> m.enqueueStart);
            final Messages serialize = new Messages(m -> m.serialize);
            final Messages arrive = new Messages(m -> m.arrive);
            final Messages deserializeOnEventLoop = new Messages(m -> m.deserialize);
            final Messages deserializeOffEventLoop = new Messages(m -> m.deserialize);
            final Messages processOutOfOrder = new Messages(m -> m.deserialize);

            long canonicalBytesInFlight = 0;
            long nextId = 0;
            long now;
            while ((now = ApproximateTime.nanoTime()) < deadlineNanos)
            {
                Event next = sequence.await(nextId, 100L, TimeUnit.MILLISECONDS);
                if (next == null)
                {
                    // decide if we have any messages waiting too long to proceed
                    while (!processOutOfOrder.isEmpty())
                    {
                        MessageState m = processOutOfOrder.get(0);
                        if (now - m.lastUpdateNanos > TimeUnit.SECONDS.toNanos(10L))
                        {
                            fail("Unreasonably long period spent waiting for out-of-order deser/delivery of received message %d", m.message.id());
                            messages.remove(m.message.id());
                            processOutOfOrder.remove(0);
                        }
                        else break;
                    }
                    continue;
                }
                sequence.clear(nextId); // TODO: simplify collection if we end up using it exclusively as a queue, as we are now

                switch (next.type)
                {
                    case ENQUEUE:
                    {
                        MessageState m;
                        EnqueueMessageEvent e = enqueue(next);
                        assert nextId == e.start || nextId == e.end;
                        assert e.message != null;
                        if (nextId == e.start)
                        {
                            canonicalBytesInFlight += e.message.serializedSize(current_version);
                            m = new MessageState(e.message, e.start);
                            messages.put(e.messageId, m);
                            enqueue.add(m);
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
                    case SERIALIZE:
                    {
                        // serialize happens serially, so we can compress the asynchronicity of the above enqueue
                        // into a linear sequence of events we expect to occur on arrival
                        SerializeMessageEvent e = serialize(next);
                        assert nextId == e.at;
                        MessageState m = messages.get(e.messageId);
                        assert m.state == ENQUEUE;
                        m.serialize = e.at;
                        m.messagingVersion = e.messagingVersion;
                        m.processOnEventLoop = isOnEventLoop(outboundType, m.message, e.messagingVersion);
                        int mi = enqueue.indexOf(m);
                        for (int i = 0 ; i < mi ; ++i)
                        {
                            MessageState pm = enqueue.get(i);
                            if (pm.enqueueEnd != 0)
                                fail("Invalid order of events: %d (%d, %d) enqueued strictly before %d (%d, %d), but serialized after",
                                     pm.message.id(), pm.enqueueEnd, pm.serialize, m.message.id(), m.enqueueEnd, m.serialize);
                        }
                        enqueue.remove(mi);
                        serialize.add(m);
                        m.update(next.type);
                        break;
                    }
                    case ARRIVE:
                    {
                        SimpleMessageEvent e = arrive(next);
                        assert nextId == e.at;
                        MessageState m = messages.get(e.messageId);
                        assert m.state == SERIALIZE;
                        m.arrive = e.at;
                        int mi = serialize.indexOf(m);
                        for (int i = 0 ; i < mi ; ++i)
                        {
                            MessageState pm = serialize.get(i);
                            fail("Invalid order of events: %d (%d, %d) serialized strictly before %d (%d, %d), but arrived after",
                                 pm.message.id(), pm.serialize, pm.arrive, m.message.id(), m.serialize, m.arrive);
                        }
                        serialize.remove(mi);
                        arrive.add(m);
                        m.update(next.type);
                        break;
                    }
                    case DESERIALIZE:
                    {
                        // deserialize may happen in parallel for large messages, but in sequence for small messages
                        // so we separate the two
                        SimpleMessageEvent e = deserialize(next);
                        assert nextId == e.at;
                        MessageState m = messages.get(e.messageId);
                        assert m.state == ARRIVE;
                        m.deserialize = e.at;
                        // deserialize may be off-loaded, so we can only impose meaningful ordering constraints
                        // on those messages we know to have been processed on the event loop
                        int mi = arrive.indexOf(m);
                        if (m.processOnEventLoop)
                        {
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = arrive.get(i);
                                if (pm.processOnEventLoop)
                                {
                                    fail("Invalid order of events: %d (%d, %d) arrived strictly before %d (%d, %d), but deserialized after",
                                         pm.message.id(), pm.arrive, pm.deserialize, m.message.id(), m.arrive, m.deserialize);
                                }
                            }
                            deserializeOnEventLoop.add(m);
                        }
                        else
                        {
                            deserializeOffEventLoop.add(m);
                        }
                        arrive.remove(mi);
                        m.update(next.type);
                        break;
                    }
                    case PROCESS:
                    {
                        ProcessMessageEvent e = process(next);
                        assert nextId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        assert m.state == DESERIALIZE;
                        if (m == null)
                            break;

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
                            processOutOfOrder.remove(m);
                        }
                        else if (m.processOnEventLoop)
                        {
                            // we can expect that processing happens sequentially in this case, more specifically
                            // we can actually expect that this event will occur _immediately_ after the deserialize event
                            // so that we have exactly one mess
                            // c
                            int mi = deserializeOnEventLoop.indexOf(m);
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = deserializeOnEventLoop.get(i);
                                fail("Invalid order of events: %d (%d, %d) deserialized strictly before %d (%d, %d), but processed after",
                                     pm.message.id(), pm.arrive, pm.deserialize, m.message.id(), m.arrive, m.deserialize);
                                messages.remove(pm.message.id());
                            }
                            deserializeOnEventLoop.clearHead(mi + 1);
                        }
                        else
                        {
                            int mi = deserializeOffEventLoop.indexOf(m);
                            // process may be off-loaded, so we can only impose meaningful ordering constraints
                            // on those messages we know to have been processed on the event loop
                            for (int i = 0 ; i < mi ; ++i)
                            {
                                MessageState pm = deserializeOffEventLoop.get(i);
                                pm.processOutOfOrder = true;
                                processOutOfOrder.add(pm);
                            }
                            deserializeOffEventLoop.clearHead(mi + 1);
                        }
                        // this message has been fully validated
                        break;
                    }
                    case FAIL_EXPIRED:
                    {
                        // serialize happens serially, so we can compress the asynchronicity of the above enqueue
                        // into a linear sequence of events we expect to occur on arrival
                        ExpiredMessageEvent e = expired(next);
                        assert nextId == e.at;
                        MessageState m = messages.remove(e.messageId);
                        if (m.state != e.expectState)
                            fail("Invalid expiry of %d: expected message in %s, found %s", m.message.id(), e.expectState, m.state);

                        now = System.nanoTime();
                        if (m.message.expiresAtNanos() > now)
                        {
                            // TODO: even with new ApproximateTime comparison methods, there's no strict guarantees here given NTP
                            //       we could fix the conversion AlmostSameTime for an entire run, which should suffice
                            fail("Invalid expiry of %d: expiry should occur in %dms; event believes %dms have elapsed, and %dms have actually elapsed", m.message.id(),
                                 NANOSECONDS.toMillis(m.message.expiresAtNanos() - m.message.createdAtNanos()),
                                 e.timeUnit.toMillis(e.timeElapsed),
                                 NANOSECONDS.toMillis(now - m.message.createdAtNanos()));
                        }

                        switch (m.state)
                        {
                            case ENQUEUE: enqueue.remove(m); break;
                            case SERIALIZE: serialize.remove(m); break;
                            case DESERIALIZE: (m.processOnEventLoop ? deserializeOnEventLoop : deserializeOffEventLoop).remove(m); break;
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

    private static class Sequence
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

        Sequence()
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

    private static class Messages
    {
        final ToLongFunction<MessageState> sortById;
        private MessageState[] messages = new MessageState[10];
        private int begin, end;

        private Messages(ToLongFunction<MessageState> sortById)
        {
            this.sortById = sortById;
        }

        public int size()
        {
            return end - begin;
        }

        public MessageState get(int i)
        {
            return messages[i + begin];
        }

        public int floorIndex(long sortId)
        {
            int i = nearestIndex(sortId);
            if (sortId > sortById.applyAsLong(messages[i]))
                while (sortId > sortById.applyAsLong(messages[--i]));
            else
                while (i + 1 < end && sortId < sortById.applyAsLong(messages[i + 1])) i += 1;
            return i - begin;
        }

        public int ceilIndex(long sortId)
        {
            int i = nearestIndex(sortId);
            if (sortId < sortById.applyAsLong(messages[i]))
                while (sortId < sortById.applyAsLong(messages[++i]));
            else
                while (i > 0 && sortId >= sortById.applyAsLong(messages[i - 1])) --i;
            return i - begin;
        }

        private int nearestIndex(long sortId)
        {
            long beginId = sortById.applyAsLong(messages[begin]);
            long endId = sortById.applyAsLong(messages[end - 1]);
            if (sortId <= beginId) return begin;
            else if (sortId >= endId) return end - 1;
            else return begin + (int) (((end - begin) * (sortId - beginId)) / (endId - beginId));
        }

        public int indexOf(MessageState m)
        {
            if (messages[begin] == m)
            {
                return 0;
            }
            else if (messages[end - 1] == m)
            {
                return (end - 1) - begin;
            }
            else
            {
                // seek to the approximate position implied by the sortById, then seek in the right direction
                int i = nearestIndex(sortById.applyAsLong(m));
                if (m != messages[i])
                {
                    if (sortById.applyAsLong(m) < sortById.applyAsLong(messages[i]))
                    {
                        while (true)
                        {
                            if (i == begin)
                                throw new IllegalStateException();
                            if (messages[--i] == m)
                                break;
                        }
                    }
                    else
                    {
                        while (true)
                        {
                            if (++i == end)
                                throw new IllegalStateException();
                            if (messages[i] == m)
                                break;
                        }
                    }
                }
                return i - begin;
            }
        }

        public int indexOf(long sortId)
        {
            // seek to the approximate position implied by the sortById, then seek in the right direction
            int i = nearestIndex(sortId);
            if (sortId < sortById.applyAsLong(messages[i]))
                while (sortById.applyAsLong(messages[--i]) != sortId);
            else
                while (sortById.applyAsLong(messages[i]) != sortId) ++i;
            return i - begin;
        }

        public void add(MessageState m)
        {
            assert begin == end || sortById.applyAsLong(messages[end - 1]) < sortById.applyAsLong(m);
            if (end == messages.length)
            {
                MessageState[] src = messages;
                MessageState[] trg;
                if (end - begin < src.length / 2) trg = src;
                else trg = new MessageState[src.length * 2];
                System.arraycopy(src, begin, trg, 0, end - begin);
                end -= begin;
                begin = 0;
                messages = trg;
            }
            messages[end++] = m;
        }

        public void remove(MessageState m)
        {
            remove(indexOf(m));
        }

        public void remove(int i)
        {
            i += begin;
            assert i < end;

            if (i - begin < end - i)
            {
                System.arraycopy(messages, begin, messages, begin + 1, i - begin);
                messages[begin++] = null;
            }
            else
            {
                System.arraycopy(messages, i + 1, messages, i, (end - 1) - i);
                messages[--end] = null;
            }
        }

        public void remove(int from, int to, Predicate<MessageState> p)
        {
            if (from == to)
                return;
            from += begin;
            to += begin;
            if (from - begin < end - to)
            {
                int newFrom = to;
                for (int i = to - 1 ; i >= from ; --i)
                {
                    if (!p.test(messages[i]))
                        messages[--newFrom] = messages[i];
                }
                int removed = newFrom - from;
                System.arraycopy(messages, begin, messages, begin + removed, from - begin);
                Arrays.fill(messages, begin, begin + removed, null);
                end -= removed;
            }
            else
            {
                int newTo = from;
                for (int i = from ; i < to ; ++i)
                {
                    if (!p.test(messages[i]))
                        messages[newTo++] = messages[i];
                }
                System.arraycopy(messages, to, messages, newTo, end - to);
                int removed = to - newTo;
                Arrays.fill(messages, end - removed, end, null);
                end -= removed;
            }
        }

        public void clearHead(int to)
        {
            Arrays.fill(messages, begin, begin + to, null);
            begin += to;
        }

        public boolean isEmpty()
        {
            return begin == end;
        }
    }


    private static boolean isOnEventLoop(ConnectionType type, Message<?> message, int messagingVersion)
    {
        int size = message.serializedSize(messagingVersion);
        if (type == ConnectionType.SMALL_MESSAGES && messagingVersion >= VERSION_40)
            return size <= LARGE_MESSAGE_THRESHOLD;
        else if (messagingVersion >= VERSION_40)
            return size <= DEFAULT_BUFFER_SIZE;
        else
            return size <= LARGE_MESSAGE_THRESHOLD;
    }

}
