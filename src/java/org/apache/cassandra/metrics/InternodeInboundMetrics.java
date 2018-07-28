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

import com.codahale.metrics.Gauge;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.async.InboundMessageHandlers;
import org.apache.cassandra.metrics.CassandraMetricsRegistry.MetricName;

/**
 * Metrics for internode connections.
 */
public class InternodeInboundMetrics
{
    private final MetricName receivedCount;
    private final MetricName receivedBytes;
    private final MetricName processedCount;
    private final MetricName processedBytes;
    private final MetricName pendingCount;
    private final MetricName pendingBytes;
    private final MetricName expiredCount;
    private final MetricName expiredBytes;
    private final MetricName errorCount;
    private final MetricName errorBytes;

    /**
     * Create metrics for given inbound message handlers.
     *
     * @param peer IP address and port to use for metrics label
     */
    public InternodeInboundMetrics(InetAddressAndPort peer, InboundMessageHandlers handlers)
    {
        // ipv6 addresses will contain colons, which are invalid in a JMX ObjectName
        MetricNameFactory factory = new DefaultNameFactory("InboundConnection", peer.toString().replace(':', '_'));

        register(receivedCount = factory.createMetricName("ReceivedCount"), handlers::receivedCount);
        register(receivedBytes = factory.createMetricName("ReceivedBytes"), handlers::receivedBytes);
        register(processedCount = factory.createMetricName("ProcessedCount"), handlers::processedCount);
        register(processedBytes = factory.createMetricName("ProcessedBytes"), handlers::processedBytes);
        register(pendingCount = factory.createMetricName("PendingCount"), handlers::pendingCount);
        register(pendingBytes = factory.createMetricName("PendingBytes"), handlers::pendingBytes);
        register(expiredCount = factory.createMetricName("ExpiredCount"), handlers::expiredCount);
        register(expiredBytes = factory.createMetricName("ExpiredBytes"), handlers::expiredBytes);
        register(errorCount = factory.createMetricName("ErrorCount"), handlers::errorCount);
        register(errorBytes = factory.createMetricName("ErrorBytes"), handlers::errorBytes);
    }

    public void release()
    {
        remove(receivedCount);
        remove(receivedBytes);
        remove(processedCount);
        remove(processedBytes);
        remove(pendingCount);
        remove(pendingBytes);
        remove(expiredCount);
        remove(expiredBytes);
        remove(errorCount);
        remove(errorBytes);
    }

    private static void register(MetricName name, Gauge<Long> gauge)
    {
        CassandraMetricsRegistry.Metrics.register(name, gauge);
    }

    private static void remove(MetricName name)
    {
        CassandraMetricsRegistry.Metrics.remove(name);
    }
}
