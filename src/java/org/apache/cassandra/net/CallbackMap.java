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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RemoteCallbacks.CallbackInfo;
import org.apache.cassandra.utils.Clock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

class CallbackMap
{
    private static final Logger logger = LoggerFactory.getLogger(CallbackMap.class);

    private volatile boolean shutdown;

    // if we use more ExpiringMaps we may want to add multiple threads to this executor
    private final ScheduledExecutorService service = new DebuggableScheduledThreadPoolExecutor("EXPIRING-MAP-REAPER");

    private final ConcurrentMap<CallbackKey, CallbackInfo> cache = new ConcurrentHashMap<>();

    private final Consumer<CallbackInfo> onExpired;

    CallbackMap(long expirationInterval, TimeUnit expirationIntervalUnits, Consumer<CallbackInfo> onExpired)
    {
        this.onExpired = onExpired;

        if (expirationInterval <= 0)
            throw new IllegalArgumentException("Argument specified must be a positive number");

        service.scheduleWithFixedDelay(this::expire, expirationInterval, expirationInterval, expirationIntervalUnits);
    }

    CallbackInfo put(long id, InetAddressAndPort peer, CallbackInfo value)
    {
        if (shutdown)
        {
            /*
             * StorageProxy isn't equipped to deal with "I'm nominally alive, but I can't send any messages out."
             * So we'll just sit on this thread until the rest of the server shutdown completes.
             * See comments in CASSANDRA-3335, and CASSANDRA-3727.
             *
             * TODO: what?
             */
            //noinspection UnstableApiUsage
            Uninterruptibles.sleepUninterruptibly(Long.MAX_VALUE, NANOSECONDS);
        }

        return cache.put(new CallbackKey(id, peer), value);
    }

    CallbackInfo get(long id, InetAddressAndPort peer)
    {
        return cache.get(new CallbackKey(id, peer));
    }

    CallbackInfo remove(long id, InetAddressAndPort peer)
    {
        return cache.remove(new CallbackKey(id, peer));
    }

    @SuppressWarnings("UnusedReturnValue")
    CallbackInfo removeAndExpire(long id, InetAddressAndPort peer)
    {
        CallbackInfo ci = remove(id, peer);
        if (null != ci)
            onExpired.accept(ci);
        return ci;
    }

    private void expire()
    {
        long start = Clock.instance.nanoTime();
        int n = 0;
        for (Map.Entry<CallbackKey, CallbackInfo> entry : cache.entrySet())
        {
            if (entry.getValue().isReadyToDieAt(start))
            {
                if (cache.remove(entry.getKey(), entry.getValue()))
                {
                    n++;
                    onExpired.accept(entry.getValue());
                }
            }
        }
        logger.trace("Expired {} entries", n);
    }

    private void forceExpire()
    {
        for (Map.Entry<CallbackKey, CallbackInfo> entry : cache.entrySet())
            if (cache.remove(entry.getKey(), entry.getValue()))
                onExpired.accept(entry.getValue());
    }

    void shutdownNow(boolean expireContents)
    {
        service.shutdownNow();
        if (expireContents)
            forceExpire();
    }

    void shutdownGracefully()
    {
        expire();
        if (!cache.isEmpty())
            service.schedule(this::shutdownGracefully, 100L, TimeUnit.MILLISECONDS);
        else
            service.shutdownNow();
    }

    void awaitTerminationUntil(long deadlineNanos) throws TimeoutException, InterruptedException
    {
        if (!service.isTerminated())
        {
            long wait = deadlineNanos - System.nanoTime();
            if (wait <= 0 || !service.awaitTermination(wait, NANOSECONDS))
                throw new TimeoutException();
        }
    }

    void reset()
    {
        shutdown = false;
        cache.clear();
    }

    static class CallbackKey
    {
        final long id;
        final InetAddressAndPort peer;

        CallbackKey(long id, InetAddressAndPort peer)
        {
            this.id = id;
            this.peer = peer;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof CallbackKey))
                return false;
            CallbackKey that = (CallbackKey) o;
            return this.id == that.id && this.peer.equals(that.peer);
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(id) + 31 * peer.hashCode();
        }

        @Override
        public String toString()
        {
            return "{id:" + id + ", peer:" + peer + '}';
        }
    }
}
