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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public abstract class ResourceLimits
{
    public interface Limit
    {
        long limit();
        long remaining();
        long using();

        boolean tryAllocate(long amount);
        void release(long amount);
    }

    public static class EndpointAndGlobal
    {
        public final Limit endpoint;
        public final Limit global;

        public EndpointAndGlobal(Limit endpoint, Limit global)
        {
            this.endpoint = endpoint;
            this.global = global;
        }

        Outcome tryAllocate(long amount)
        {
            return ResourceLimits.tryAllocate(endpoint, global, amount);
        }

        void release(long amount)
        {
            ResourceLimits.release(endpoint, global, amount);
        }
    }

    public enum Outcome { SUCCESS, INSUFFICIENT_ENDPOINT, INSUFFICIENT_GLOBAL }

    static Outcome tryAllocate(Limit endpoint, Limit global, long amount)
    {
        if (!global.tryAllocate(amount))
            return Outcome.INSUFFICIENT_GLOBAL;

        if (endpoint.tryAllocate(amount))
            return Outcome.SUCCESS;

        global.release(amount);
        return Outcome.INSUFFICIENT_ENDPOINT;
    }

    public static void release(Limit endpoint, Limit global, long amount)
    {
        endpoint.release(amount);
        global.release(amount);
    }

    public static class Concurrent implements Limit
    {
        private final long limit;

        private volatile long using;
        private static final AtomicLongFieldUpdater<Concurrent> usingUpdater =
            AtomicLongFieldUpdater.newUpdater(Concurrent.class, "using");

        public Concurrent(long limit)
        {
            this.limit = limit;
        }

        public long limit()
        {
            return limit;
        }

        public long remaining()
        {
            return limit - using;
        }

        public long using()
        {
            return using;
        }

        public boolean tryAllocate(long amount)
        {
            long current, next;
            do
            {
                current = using;
                next = current + amount;

                if (next > limit)
                    return false;
            } while (!usingUpdater.compareAndSet(this, current, next));

            return true;
        }

        public void release(long amount)
        {
            assert amount >= 0;
            long using = usingUpdater.addAndGet(this, -amount);
            assert using >= 0;
        }
    }

    public static class Basic implements Limit
    {
        private final long limit;
        private long using;

        public Basic(long limit)
        {
            this.limit = limit;
        }

        public long limit()
        {
            return limit;
        }

        public long remaining()
        {
            return limit - using;
        }

        public long using()
        {
            return using;
        }

        public boolean tryAllocate(long amount)
        {
            if (using + amount > limit)
                return false;

            using += amount;
            return true;
        }

        public void release(long amount)
        {
            assert amount >= 0 && amount <= using;
            using -= amount;
        }
    }
}
