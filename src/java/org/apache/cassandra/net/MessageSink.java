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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.locator.InetAddressAndPort;

public class MessageSink
{
    public interface InboundSink
    {
        public boolean allowInbound(Message<?> message);
        public default InboundSink without(InboundSink without) { return (this == without) ? null : this; }
    }

    public interface OutboundSink
    {
        public boolean allowOutbound(Message<?> message, InetAddressAndPort to);
        public default OutboundSink without(OutboundSink without) { return (this == without) ? null : this; }
    }

    public interface Sink extends InboundSink, OutboundSink { }

    private volatile InboundSink inbound;
    private volatile OutboundSink outbound;

    private static final AtomicReferenceFieldUpdater<MessageSink, InboundSink> inboundUpdater
    = AtomicReferenceFieldUpdater.newUpdater(MessageSink.class, InboundSink.class, "inbound");

    private static final AtomicReferenceFieldUpdater<MessageSink, OutboundSink> outboundUpdater
    = AtomicReferenceFieldUpdater.newUpdater(MessageSink.class, OutboundSink.class, "outbound");

    boolean allowInbound(Message<?> message)
    {
        InboundSink sink = inbound;
        if (sink == null)
            return true;
        return sink.allowInbound(message);
    }

    boolean allowOutbound(Message<?> message, InetAddressAndPort to)
    {
        OutboundSink sink = outbound;
        if (sink == null)
            return true;
        return sink.allowOutbound(message, to);
    }

    private static InboundSink mergeInbound(InboundSink a, InboundSink b)
    {
        if (a == null) return b;
        if (b == null) return a;
        return new InboundSink()
        {
            public boolean allowInbound(Message<?> message)
            {
                return a.allowInbound(message)
                       && b.allowInbound(message);
            }

            public InboundSink without(InboundSink without)
            {
                return mergeInbound(a.without(without), b.without(without));
            }
        };
    }

    private static OutboundSink mergeOutbound(OutboundSink a, OutboundSink b)
    {
        if (a == null) return b;
        if (b == null) return a;
        return new OutboundSink()
        {
            public boolean allowOutbound(Message<?> message, InetAddressAndPort to)
            {
                return a.allowOutbound(message, to)
                       && b.allowOutbound(message, to);
            }

            public OutboundSink without(OutboundSink without)
            {
                return mergeOutbound(a.without(without), b.without(without));
            }
        };
    }

    public void addInbound(InboundSink inbound)
    {
        inboundUpdater.accumulateAndGet(this, inbound, MessageSink::mergeInbound);
    }

    public void addOutbound(OutboundSink outbound)
    {
        outboundUpdater.accumulateAndGet(this, outbound, MessageSink::mergeOutbound);
    }

    public void add(Sink sink)
    {
        addInbound(sink);
        addOutbound(sink);
    }

    public void removeInbound(InboundSink inbound)
    {
        inboundUpdater.accumulateAndGet(this, inbound, InboundSink::without);
    }

    public void removeOutbound(OutboundSink outbound)
    {
        outboundUpdater.accumulateAndGet(this, outbound, OutboundSink::without);
    }

    public void remove(Sink sink)
    {
        removeInbound(sink);
        removeOutbound(sink);
    }

    public void clear()
    {
        inbound = null;
        outbound = null;
    }

}
