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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DroppedMessageMetrics;
import org.apache.cassandra.utils.StatusLogger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DroppedMessages
{
    private static final Logger logger = LoggerFactory.getLogger(DroppedMessages.class);
    private static final int LOG_DROPPED_INTERVAL_IN_MS = 5000;

    private static final class DroppedForVerb
    {
        final DroppedMessageMetrics metrics;
        final AtomicInteger droppedFromSelf;
        final AtomicInteger droppedFromPeer;

        DroppedForVerb(Verb verb)
        {
            this(new DroppedMessageMetrics(verb));
        }

        DroppedForVerb(DroppedMessageMetrics metrics)
        {
            this.metrics = metrics;
            this.droppedFromSelf = new AtomicInteger(0);
            this.droppedFromPeer = new AtomicInteger(0);
        }
    }

    @VisibleForTesting
    public void resetMap(String scope)
    {
        for (Verb verb : droppedMessagesMap.keySet())
        {
            droppedMessagesMap.put(verb, new DroppedForVerb(new DroppedMessageMetrics(metricName ->
                                                                                      new CassandraMetricsRegistry.MetricName("DroppedMessages", metricName, scope)
            )));
        }
    }

    // total dropped message counts for server lifetime
    private final Map<Verb, DroppedForVerb> droppedMessagesMap = new EnumMap<>(Verb.class);

    public DroppedMessages()
    {
        for (Verb verb : Verb.values())
            droppedMessagesMap.put(verb, new DroppedForVerb(verb));
    }

    void scheduleLogging()
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::logDroppedMessages,
                                                                 LOG_DROPPED_INTERVAL_IN_MS,
                                                                 LOG_DROPPED_INTERVAL_IN_MS,
                                                                 MILLISECONDS);
    }

    public void increment(Verb verb)
    {
        increment(verb, false);
    }

    public void incrementWithLatency(Verb verb, long timeTaken, TimeUnit units)
    {
        incrementWithLatency(verb, timeTaken, units, false);
    }

    public void incrementWithLatency(Message message, long timeTaken, TimeUnit units)
    {
        incrementWithLatency(message.verb, timeTaken, units, message.isCrossNode());
    }

    public void incrementWithLatency(Verb verb, long timeTaken, TimeUnit units, boolean isCrossNode)
    {
        incrementWithLatency(droppedMessagesMap.get(verb), timeTaken, units, isCrossNode);
    }

    public void increment(Verb verb, boolean isCrossNode)
    {
        increment(droppedMessagesMap.get(verb), isCrossNode);
    }

    private static void incrementWithLatency(DroppedForVerb droppedMessages, long timeTaken, TimeUnit units, boolean isCrossNode)
    {
        if (isCrossNode)
            droppedMessages.metrics.crossNodeDroppedLatency.update(timeTaken, units);
        else
            droppedMessages.metrics.internalDroppedLatency.update(timeTaken, units);
        increment(droppedMessages, isCrossNode);
    }

    private static void increment(DroppedForVerb droppedMessages, boolean isCrossNode)
    {
        droppedMessages.metrics.dropped.mark();
        if (isCrossNode)
            droppedMessages.droppedFromPeer.incrementAndGet();
        else
            droppedMessages.droppedFromSelf.incrementAndGet();
    }

    private void logDroppedMessages()
    {
        List<String> logs = getLogs();
        for (String log : logs)
            logger.info(log);

        if (logs.size() > 0)
            StatusLogger.log();
    }

    @VisibleForTesting
    List<String> getLogs()
    {
        List<String> ret = new ArrayList<>();
        for (Map.Entry<Verb, DroppedForVerb> entry : droppedMessagesMap.entrySet())
        {
            Verb verb = entry.getKey();
            DroppedForVerb droppedForVerb = entry.getValue();

            int droppedInternal = droppedForVerb.droppedFromSelf.getAndSet(0);
            int droppedCrossNode = droppedForVerb.droppedFromPeer.getAndSet(0);
            if (droppedInternal > 0 || droppedCrossNode > 0)
            {
                ret.add(String.format("%s messages were dropped in last %d ms: %d internal and %d cross node."
                                      + " Mean internal dropped latency: %d ms and Mean cross-node dropped latency: %d ms",
                                      verb,
                                      LOG_DROPPED_INTERVAL_IN_MS,
                                      droppedInternal,
                                      droppedCrossNode,
                                      TimeUnit.NANOSECONDS.toMillis((long) droppedForVerb.metrics.internalDroppedLatency.getSnapshot().getMean()),
                                      TimeUnit.NANOSECONDS.toMillis((long) droppedForVerb.metrics.crossNodeDroppedLatency.getSnapshot().getMean())));
            }
        }
        return ret;
    }

    public Map<String, Integer> getDroppedMessages()
    {
        Map<String, Integer> map = new HashMap<>(droppedMessagesMap.size());
        for (Map.Entry<Verb, DroppedForVerb> entry : droppedMessagesMap.entrySet())
            map.put(entry.getKey().toString(), (int) entry.getValue().metrics.dropped.getCount());
        return map;
    }

}
