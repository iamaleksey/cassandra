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

package org.apache.cassandra.service.reads.repair;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import com.codahale.metrics.Meter;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tracing.Tracing;

/**
 * Makes a best effort to send repair mutations to out of sync replicas,
 * but doesn't block on it. Doesn't provice monotonic quorum reads and
 * breaks atomic writes
 */
public class AsyncReadRepair extends AbstractReadRepair
{
    public AsyncReadRepair(ReadCommand command, long queryStartNanoTime, ConsistencyLevel consistency)
    {
        super(command, queryStartNanoTime, consistency);
    }

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddressAndPort[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, consistency, this);
    }

    @Override
    Meter getRepairMeter()
    {
        return ReadRepairMetrics.repairedAsync;
    }

    @Override
    public void maybeSendAdditionalWrites()
    {
        // noop
    }

    @Override
    public void awaitWrites()
    {
        // noop
    }

    @VisibleForTesting
    protected void sendRR(MessageOut<Mutation> message, InetAddressAndPort endpoint)
    {
        MessagingService.instance().sendOneWay(message, endpoint);
    }

    @Override
    public void repairPartition(DecoratedKey key, Map<InetAddressAndPort, Mutation> mutations, InetAddressAndPort[] destinations)
    {
        for (Map.Entry<InetAddressAndPort, Mutation> entry: mutations.entrySet())
        {
            InetAddressAndPort destination = entry.getKey();
            Mutation mutation = entry.getValue();
            TableId tableId = Iterables.getOnlyElement(mutation.getTableIds());

            Tracing.trace("Sending read-repair-mutation to {}", destination);
            // use a separate verb here to avoid writing hints on timeouts
            sendRR(mutation.createMessage(MessagingService.Verb.READ_REPAIR), destination);
            ColumnFamilyStore.metricsFor(tableId).readRepairRequests.mark();
        }
    }
}
