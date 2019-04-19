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

/*
 * TODO: consider adding padding to prevent false sharing
 */
class InboundCounters
{
    private volatile long errorCount;
    private volatile long errorBytes;

    private static final AtomicLongFieldUpdater<InboundCounters> errorCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "errorCount");
    private static final AtomicLongFieldUpdater<InboundCounters> errorBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "errorBytes");

    void addError(int bytes)
    {
        errorCountUpdater.incrementAndGet(this);
        errorBytesUpdater.addAndGet(this, bytes);
    }

    long errorCount()
    {
        return errorCount;
    }

    long errorBytes()
    {
        return errorBytes;
    }

    private volatile long expiredCount;
    private volatile long expiredBytes;

    private static final AtomicLongFieldUpdater<InboundCounters> expiredCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "expiredCount");
    private static final AtomicLongFieldUpdater<InboundCounters> expiredBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "expiredBytes");

    void addExpired(int bytes)
    {
        expiredCountUpdater.incrementAndGet(this);
        expiredBytesUpdater.addAndGet(this, bytes);
    }

    long expiredCount()
    {
        return expiredCount;
    }

    long expiredBytes()
    {
        return expiredBytes;
    }

    private volatile long processedCount;
    private volatile long processedBytes;

    private static final AtomicLongFieldUpdater<InboundCounters> processedCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "processedCount");
    private static final AtomicLongFieldUpdater<InboundCounters> processedBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "processedBytes");

    void addProcessed(int bytes)
    {
        processedCountUpdater.incrementAndGet(this);
        processedBytesUpdater.addAndGet(this, bytes);
    }

    long processedCount()
    {
        return processedCount;
    }

    long processedBytes()
    {
        return processedBytes;
    }

    private volatile long pendingCount;
    private volatile long pendingBytes;

    private static final AtomicLongFieldUpdater<InboundCounters> pendingCountUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "pendingCount");
    private static final AtomicLongFieldUpdater<InboundCounters> pendingBytesUpdater =
        AtomicLongFieldUpdater.newUpdater(InboundCounters.class, "pendingBytes");

    void addPending(int bytes)
    {
        pendingCountUpdater.incrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, bytes);
    }

    void removePending(int bytes)
    {
        pendingCountUpdater.decrementAndGet(this);
        pendingBytesUpdater.addAndGet(this, -bytes);
    }

    long pendingCount()
    {
        return pendingCount;
    }

    long pendingBytes()
    {
        return pendingBytes;
    }
}
