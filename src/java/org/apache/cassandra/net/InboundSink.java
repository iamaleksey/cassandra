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

package org.apache.cassandra.net;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Predicate;

import net.openhft.chronicle.core.util.ThrowingConsumer;
import org.apache.cassandra.net.async.InboundMessageHandlers;

public abstract class InboundSink implements InboundMessageHandlers.MessageSink
{
    private static class Filtered implements ThrowingConsumer<Message<?>, IOException>
    {
        final Predicate<Message<?>> condition;
        final ThrowingConsumer<Message<?>, IOException> next;
        private Filtered(Predicate<Message<?>> condition, ThrowingConsumer<Message<?>, IOException> next)
        {
            this.condition = condition;
            this.next = next;
        }

        public void accept(Message<?> message) throws IOException
        {
            if (condition.test(message))
                next.accept(message);
        }
    }

    private static final AtomicReferenceFieldUpdater<InboundSink, ThrowingConsumer> sinkUpdater
    = AtomicReferenceFieldUpdater.newUpdater(InboundSink.class, ThrowingConsumer.class, "sink");

    private volatile ThrowingConsumer<Message<?>, IOException> sink;

    public InboundSink(ThrowingConsumer<Message<?>, IOException> sink)
    {
        this.sink = sink;
    }

    public void accept(Message<?> message) throws IOException
    {
        sink.accept(message);
    }

    public void add(Predicate<Message<?>> allow)
    {
        sinkUpdater.updateAndGet(this, sink -> new Filtered(allow, sink));
    }

    public void remove(Predicate<Message<?>> allow)
    {
        sinkUpdater.updateAndGet(this, sink -> without(sink, allow));
    }

    public void clear()
    {
        sinkUpdater.updateAndGet(this, InboundSink::clear);
    }

    @Deprecated // TODO: this is not the correct way to do things
    public boolean allow(Message<?> message)
    {
        return allows(sink, message);
    }

    private static ThrowingConsumer<Message<?>, IOException> clear(ThrowingConsumer<Message<?>, IOException> sink)
    {
        while (sink instanceof Filtered)
            sink = ((Filtered) sink).next;
        return sink;
    }

    private static ThrowingConsumer<Message<?>, IOException> without(ThrowingConsumer<Message<?>, IOException> sink, Predicate<Message<?>> condition)
    {
        if (!(sink instanceof Filtered))
            return sink;

        Filtered filtered = (Filtered) sink;
        ThrowingConsumer<Message<?>, IOException> next = without(filtered.next, condition);
        return condition.equals(filtered.condition) ? next
                                                    : next == filtered.next
                                                      ? sink
                                                      : new Filtered(filtered.condition, next);
    }

    private static boolean allows(ThrowingConsumer<Message<?>, IOException> sink, Message<?> message)
    {
        while (sink instanceof Filtered)
            if (!((Filtered) sink).condition.test(message))
                return false;
        return true;
    }

}
