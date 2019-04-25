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
package org.apache.cassandra.utils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ExpiringMap<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(ExpiringMap.class);
    private volatile boolean shutdown;

    public static class CacheableObject<T>
    {
        public final T value;
        public final long expiresAtNanos;
        public final long createdAt;

        private CacheableObject(T value, long timeout, TimeUnit unit)
        {
            assert value != null;
            this.value = value;
            this.createdAt = Clock.instance.nanoTime();
            this.expiresAtNanos = createdAt + unit.toNanos(timeout);
        }

        private CacheableObject(T value, long expiresAtNanos)
        {
            assert value != null;
            this.value = value;
            this.expiresAtNanos = expiresAtNanos;
            this.createdAt = Clock.instance.nanoTime();
        }

        public long timeout()
        {
            return expiresAtNanos - createdAt;
        }

        private boolean isReadyToDieAt(long atNano)
        {
            return atNano > expiresAtNanos;
        }
    }

    // if we use more ExpiringMaps we may want to add multiple threads to this executor
    private final ScheduledExecutorService service = new DebuggableScheduledThreadPoolExecutor("EXPIRING-MAP-REAPER");

    private final ConcurrentMap<K, CacheableObject<V>> cache = new ConcurrentHashMap<>();
    private final long defaultExpiration;
    private final TimeUnit defaultExpirationUnit;

    private final BiConsumer<K, CacheableObject<V>> onExpired;

    /**
     *
     * @param defaultExpiration the TTL for objects in the cache in milliseconds
     */
    public ExpiringMap(long defaultExpiration, TimeUnit defaultExpirationUnit, BiConsumer<K, CacheableObject<V>> onExpired)
    {
        assert onExpired != null;

        this.defaultExpiration = defaultExpiration;
        this.defaultExpirationUnit = defaultExpirationUnit;
        this.onExpired = onExpired;

        if (defaultExpiration <= 0)
            throw new IllegalArgumentException("Argument specified must be a positive number");

        Runnable runnable = () ->
        {
        };

        service.scheduleWithFixedDelay(runnable, defaultExpiration / 2, defaultExpiration / 2, TimeUnit.MILLISECONDS);
    }

    public void shutdownNow(boolean expireContents)
    {
        service.shutdownNow();
        if (expireContents)
            forceExpire();
    }

    public void shutdownGracefully()
    {
        expire();
        if (!cache.isEmpty())
            service.schedule(this::shutdownGracefully, 100L, TimeUnit.MILLISECONDS);
        else
            service.shutdownNow();
    }

    public void awaitTerminationUntil(long deadlineNanos) throws TimeoutException, InterruptedException
    {
        if (!service.isTerminated())
        {
            long wait = deadlineNanos - System.nanoTime();
            if (wait <= 0 || !service.awaitTermination(wait, NANOSECONDS))
                throw new TimeoutException();
        }
    }

    private void expire()
    {
        long start = Clock.instance.nanoTime();
        int n = 0;
        for (Map.Entry<K, CacheableObject<V>> entry : cache.entrySet())
        {
            if (entry.getValue().isReadyToDieAt(start))
            {
                if (cache.remove(entry.getKey()) != null)
                {
                    n++;
                    onExpired.accept(entry.getKey(), entry.getValue());
                }
            }
        }
        logger.trace("Expired {} entries", n);
    }

    private void forceExpire()
    {
        for (Map.Entry<K, CacheableObject<V>> e : cache.entrySet())
        {
            if (cache.remove(e.getKey(), e.getValue()))
                onExpired.accept(e.getKey(), e.getValue());
        }
    }

    public void reset()
    {
        shutdown = false;
        cache.clear();
    }

    public V put(K key, V value)
    {
        return putWithTimeout(key, value, defaultExpiration, defaultExpirationUnit);
    }

    public V putWithTimeout(K key, V value, long timeout, TimeUnit unit)
    {
        if (shutdown)
        {
            // StorageProxy isn't equipped to deal with "I'm nominally alive, but I can't send any messages out."
            // So we'll just sit on this thread until the rest of the server shutdown completes.
            //
            // See comments in CustomTThreadPoolServer.serve, CASSANDRA-3335, and CASSANDRA-3727.
            Uninterruptibles.sleepUninterruptibly(Long.MAX_VALUE, NANOSECONDS);
        }
        CacheableObject<V> previous = cache.put(key, new CacheableObject<>(value, timeout, unit));
        return (previous == null) ? null : previous.value;
    }

    public V putWithExpiration(K key, V value, long expiresAtNanos)
    {
        if (shutdown)
        {
            // StorageProxy isn't equipped to deal with "I'm nominally alive, but I can't send any messages out."
            // So we'll just sit on this thread until the rest of the server shutdown completes.
            //
            // See comments in CustomTThreadPoolServer.serve, CASSANDRA-3335, and CASSANDRA-3727.
            Uninterruptibles.sleepUninterruptibly(Long.MAX_VALUE, NANOSECONDS);
        }
        CacheableObject<V> previous = cache.put(key, new CacheableObject<>(value, expiresAtNanos));
        return (previous == null) ? null : previous.value;
    }

    public V get(K key)
    {
        CacheableObject<V> co = cache.get(key);
        return co == null ? null : co.value;
    }

    public V remove(K key)
    {
        CacheableObject<V> co = cache.remove(key);
        return co == null ? null : co.value;
    }

    public V removeAndExpire(K key)
    {
        CacheableObject<V> co = cache.remove(key);
        if (null == co)
            return null;

        onExpired.accept(key, co);
        return co.value;
    }

    /**
     * @return System.nanoTime() when key was put into the map.
     */
    public long getCreationTimeNanos(K key)
    {
        CacheableObject<V> co = cache.get(key);
        return co == null ? 0 : co.createdAt;
    }

    public int size()
    {
        return cache.size();
    }

    public boolean containsKey(K key)
    {
        return cache.containsKey(key);
    }

    public boolean isEmpty()
    {
        return cache.isEmpty();
    }

    public Set<K> keySet()
    {
        return cache.keySet();
    }
}
