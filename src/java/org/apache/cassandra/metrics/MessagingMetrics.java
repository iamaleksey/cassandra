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
package org.apache.cassandra.metrics;

import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;

import com.codahale.metrics.Timer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ApproximateTime;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Metrics for messages
 */
public class MessagingMetrics
{
    private static final MetricNameFactory factory = new DefaultNameFactory("Messaging");
    private final Timer allLatency;
    public final ConcurrentHashMap<String, Updater> dcUpdaters;
    public final EnumMap<Verb, Timer> queueWaitLatency;

    public MessagingMetrics()
    {
        allLatency = Metrics.timer(factory.createMetricName("CrossNodeLatency"));
        dcUpdaters = new ConcurrentHashMap<>();
        queueWaitLatency = new EnumMap<>(Verb.class);
        for (Verb verb : Verb.VERBS)
            queueWaitLatency.put(verb, Metrics.timer(factory.createMetricName(verb + "-WaitLatency")));
    }

    public static class Updater
    {
        public final Timer dcLatency;
        public final Timer allLatency;

        public Updater(Timer dcLatency, Timer allLatency)
        {
            this.dcLatency = dcLatency;
            this.allLatency = allLatency;
        }

        public void addTimeTaken(long timeTaken, TimeUnit units)
        {
            dcLatency.update(timeTaken, units);
            allLatency.update(timeTaken, units);
        }
    }

    public Updater getForPeer(InetAddressAndPort from)
    {
        String dcName = DatabaseDescriptor.getEndpointSnitch().getDatacenter(from);
        Updater dcUpdater = dcUpdaters.get(dcName);
        if (dcUpdater == null)
            dcUpdater = dcUpdaters.computeIfAbsent(dcName, k -> new Updater(Metrics.timer(factory.createMetricName(dcName + "-Latency")), allLatency));
        return dcUpdater;
    }

    public void addQueueWaitTime(Verb verb, long timeTaken, TimeUnit units)
    {
        if (timeTaken > 0)
            queueWaitLatency.get(verb).update(timeTaken, units);
    }
}
