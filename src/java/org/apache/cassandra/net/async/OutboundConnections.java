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

import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.net.BackPressureState;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.OutboundConnection.*;
import static org.apache.cassandra.net.async.OutboundConnection.Type.URGENT;
import static org.apache.cassandra.net.async.OutboundConnection.Type.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnection.Type.SMALL_MESSAGE;

/**
 * Groups a set of outbound connections to a given peer, and routes outgoing messages to the appropriate connection
 * (based upon message's type or size). Contains a {@link OutboundConnection} for each of the
 * {@link Type} type.
 */
public class OutboundConnections
{
    @VisibleForTesting
    public static final int LARGE_MESSAGE_THRESHOLD = Integer.getInteger(Config.PROPERTY_PREFIX + "otcp_large_message_threshold", 1024 * 64);

    private final SimpleCondition metricsReady = new SimpleCondition();
    private volatile InternodeOutboundMetrics metrics;
    private final BackPressureState backPressureState;

    private OutboundConnectionSettings template;
    public final OutboundConnection small;
    public final OutboundConnection large;
    public final OutboundConnection urgent;

    private OutboundConnections(OutboundConnectionSettings template, BackPressureState backPressureState)
    {
        this.backPressureState = backPressureState;
        this.template = template = template.withDefaultReserveLimits();
        ResourceLimits.Limit reserveEndpointCapacityInBytes = new ResourceLimits.Concurrent(template.applicationReserveSendQueueEndpointCapacityInBytes);
        ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(reserveEndpointCapacityInBytes, template.applicationReserveSendQueueGlobalCapacityInBytes);
        this.small = new OutboundConnection(SMALL_MESSAGE, template, reserveCapacityInBytes);
        this.large = new OutboundConnection(LARGE_MESSAGE, template, reserveCapacityInBytes);
        this.urgent = new OutboundConnection(URGENT, template, reserveCapacityInBytes);
    }

    public static <K> OutboundConnections tryRegister(ConcurrentMap<K, OutboundConnections> in, K key, OutboundConnectionSettings template, BackPressureState backPressureState)
    {
        OutboundConnections connections = in.get(key);
        if (connections == null)
        {
            connections = new OutboundConnections(template, backPressureState);
            OutboundConnections existing = in.putIfAbsent(key, connections);

            if (existing == null)
            {
                connections.metrics = new InternodeOutboundMetrics(template.endpoint, connections);
                connections.metricsReady.signalAll();
            }
            else
            {
                connections.metricsReady.signalAll();
                connections.close(false);
                connections = existing;
            }
        }
        return connections;
    }

    public BackPressureState getBackPressureState()
    {
        return backPressureState;
    }

    public void enqueue(Message msg, Type type) throws ClosedChannelException
    {
        connectionFor(msg, type).enqueue(msg);
    }

    /**
     * Reconnect to the peer using the given {@code addr}. Outstanding messages in each channel will be sent on the
     * current channel. Typically this function is used for something like EC2 public IP addresses which need to be used
     * for communication between EC2 regions.
     *
     * @param addr IP Address to use (and prefer) going forward for connecting to the peer
     */
    public synchronized Future<Void> reconnectWithNewIp(InetAddressAndPort addr)
    {
        template = template.withConnectTo(addr);
        return new FutureCombiner(
            apply(c -> c.reconnectWithNewTemplate(template))
        );
    }

    /**
     * Close the connections permanently
     *
     * @param flushQueues {@code true} if existing messages in the queue should be sent before closing.
     */
    public synchronized Future<Void> scheduleClose(long time, TimeUnit unit, boolean flushQueues)
    {
        // immediately release our metrics, so that if we need to re-open immediately we can safely register a new one
        releaseMetrics();
        return new FutureCombiner(
            apply(c -> c.scheduleClose(time, unit, flushQueues))
        );
    }

    /**
     * Close the connections permanently
     *
     * @param flushQueues {@code true} if existing messages in the queue should be sent before closing.
     */
    public synchronized Future<Void> close(boolean flushQueues)
    {
        // immediately release our metrics, so that if we need to re-open immediately we can safely register a new one
        releaseMetrics();
        return new FutureCombiner(
            apply(c -> c.close(flushQueues))
        );
    }

    private void releaseMetrics()
    {
        try
        {
            metricsReady.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        if (metrics != null)
            metrics.release();
    }

    /**
     * Close each netty channel and its socket
     */
    public Future<Void> interrupt()
    {
        return new FutureCombiner(
            apply(OutboundConnection::interrupt)
        );
    }

    /**
     * Apply the given function to each of the connections we are pooling, returning the results as a list
     */
    private <V> List<V> apply(Function<OutboundConnection, V> f)
    {
        return ImmutableList.of(
            f.apply(urgent), f.apply(small), f.apply(large)
        );
    }

    @VisibleForTesting
    public OutboundConnection connectionFor(Message<?> message)
    {
        return connectionFor(message, null);
    }

    @VisibleForTesting
    OutboundConnection connectionFor(Message msg, Type forceConnection)
    {
        return connectionFor(connectionTypeFor(msg, forceConnection));
    }

    @VisibleForTesting
    public static Type connectionTypeFor(Message<?> msg, Type specifyConnection)
    {
        if (specifyConnection != null)
            return specifyConnection;

        if (msg.verb.priority == Verb.Priority.P0)
            return URGENT;

        return msg.serializedSize(current_version) <= LARGE_MESSAGE_THRESHOLD
               ? SMALL_MESSAGE
               : LARGE_MESSAGE;
    }

    @VisibleForTesting
    final OutboundConnection connectionFor(Type type)
    {
        switch (type)
        {
            case SMALL_MESSAGE:
                return small;
            case LARGE_MESSAGE:
                return large;
            case URGENT:
                return urgent;
            default:
                throw new IllegalArgumentException("unsupported connection type: " + type);
        }
    }

    public long expiredCallbacks()
    {
        return metrics.expiredCallbacks.getCount();
    }

    public void incrementExpiredCallbackCount()
    {
        metrics.expiredCallbacks.mark();
    }

    public OutboundConnectionSettings template()
    {
        return template;
    }

    private static class UnusedConnectionMonitor
    {
        public UnusedConnectionMonitor(MessagingService messagingService)
        {
            this.messagingService = messagingService;
        }

        static class Counts
        {
            final long small, large, urgent;
            Counts(long small, long large, long urgent)
            {
                this.small = small;
                this.large = large;
                this.urgent = urgent;
            }
        }

        final MessagingService messagingService;
        ObjectObjectOpenHashMap<InetAddressAndPort, Counts> prevEndpointToCounts = new ObjectObjectOpenHashMap<>();

        private void closeUnusedSinceLastRun()
        {
            ObjectObjectOpenHashMap<InetAddressAndPort, Counts> curEndpointToCounts = new ObjectObjectOpenHashMap<>();
            for (OutboundConnections connections : messagingService.channelManagers.values())
            {
                Counts cur = new Counts(
                    connections.small.submittedCount(),
                    connections.large.submittedCount(),
                    connections.urgent.submittedCount()
                );
                curEndpointToCounts.put(connections.template.endpoint, cur);

                Counts prev = prevEndpointToCounts.get(connections.template.endpoint);
                if (prev == null)
                    continue;

                if (cur.small != prev.small && cur.large != prev.large && cur.urgent != prev.urgent)
                    continue;

                if (cur.small == prev.small && cur.large == prev.large && cur.urgent == prev.urgent
                    && !Gossiper.instance.isKnownEndpoint(connections.template.endpoint))
                {
                    // close entirely if no traffic and the endpoint is unknown
                    messagingService.closeOutboundNow(connections);
                    continue;
                }

                if (cur.small == prev.small)
                    connections.small.interrupt();

                if (cur.large == prev.large)
                    connections.large.interrupt();

                if (cur.urgent == prev.urgent)
                    connections.urgent.interrupt();
            }

            prevEndpointToCounts = curEndpointToCounts;
        }
    }

    public static void scheduleUnusedConnectionMonitoring(MessagingService messagingService, ScheduledExecutorService executor, long delay, TimeUnit units)
    {
        executor.scheduleWithFixedDelay(new UnusedConnectionMonitor(messagingService)::closeUnusedSinceLastRun, 0L, delay, units);
    }

    @VisibleForTesting
    public static OutboundConnections unsafeCreate(OutboundConnectionSettings template, BackPressureState backPressureState)
    {
        OutboundConnections connections = new OutboundConnections(template, backPressureState);
        connections.metricsReady.signalAll();
        return connections;
    }

}
