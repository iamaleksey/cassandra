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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.locator.InetAddressAndPort;

public class LatencySubscribers
{
    private volatile ILatencySubscriber subscribers;
    private final AtomicReferenceFieldUpdater<LatencySubscribers, ILatencySubscriber> subscribersUpdater
        = AtomicReferenceFieldUpdater.newUpdater(LatencySubscribers.class, ILatencySubscriber.class, "subscribers");

    private static ILatencySubscriber merge(ILatencySubscriber a, ILatencySubscriber b)
    {
        if (a == null) return b;
        if (b == null) return a;
        return (address, latency, unit) -> {
            a.receiveTiming(address, latency, unit);
            b.receiveTiming(address, latency, unit);
        };
    }

    public void subscribe(ILatencySubscriber subscriber)
    {
        subscribersUpdater.accumulateAndGet(this, subscriber, LatencySubscribers::merge);
    }

    public void add(InetAddressAndPort address, long latency, TimeUnit unit)
    {
        ILatencySubscriber subscribers = this.subscribers;
        if (subscribers != null)
            subscribers.receiveTiming(address, latency, unit);
    }

    /**
     * Track latency information for the dynamic snitch
     *
     * @param cb      the callback associated with this message -- this lets us know if it's a message type we're interested in
     * @param address the host that replied to the message
     * @param latency
     */
    public void maybeAdd(IAsyncCallback cb, InetAddressAndPort address, long latency, TimeUnit unit)
    {
        if (cb.isLatencyForSnitch())
            add(address, latency, unit);
    }

}
