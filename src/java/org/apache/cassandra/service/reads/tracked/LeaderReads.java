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
package org.apache.cassandra.service.reads.tracked;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.TrackedRead.Id;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.jctools.maps.NonBlockingHashMap;

// TODO: expiration
class LeaderReads
{
    enum State { CREATED, LAUNCHED, AUGMENTED }

    private static final NonBlockingHashMap<Id, LeaderRead> reads = new NonBlockingHashMap<>();

    interface LeaderRead
    {
        Id id();

        // TODO (consider): getting rid of State
        State state();
    }

    interface Created extends LeaderRead
    {
        @Nonnull Launched launch(AsyncPromise<TrackedDataResponse> promise);

        void executeOnSingleNode(AsyncPromise<TrackedDataResponse> promise);
        int[] summaryNodes();
    }

    interface Launched extends LeaderRead
    {
        Augmented augment(List<PartitionUpdate> updates);
        Id id();
        TableMetadata table();
        MutationSummary initialSummary();
        MutationSummary secondarySummary();
        int[] summaryNodes();
    }

    interface Augmented extends LeaderRead
    {
        Augmented extend();
        Augmented append();
    }

    /**
     * The main entry point into on-leader read state machine.
     * </p>
     * Trigger the ititial read from the LSM, create the initial
     * and the secondary mutation summaries, and feed the summary
     * to the reconciliation coordinator.
     * </p>
     * Alternatively, skips reconciliation logic for CL ONE/LOCAL_ONE/NODE_LOCAL or RF=1.
     */
    void launchOrExecuteOnSingleNode(Created created, AsyncPromise<TrackedDataResponse> promise)
    {
        Stage.READ.submit(() -> doLaunchOrExecuteOnSingleNode(created, promise));
    }

    @VisibleForTesting
    Launched doLaunchOrExecuteOnSingleNode(Created created, AsyncPromise<TrackedDataResponse> promise)
    {
        if (created.summaryNodes().length == 0)
        {
            created.executeOnSingleNode(promise);
            return null;
        }

        Launched launched = created.launch(promise);
        if (reads.put(created.id(), launched) != null)
            throw new IllegalStateException("pre-existing read with the same id; this shouldn't be possible");
        ReadReconciliations.instance.acceptLocalSummary(
            launched.id(), launched.secondarySummary(), launched.summaryNodes()
        );
        return launched;
    }

    /**
     * Stage two of on-leader read state machine.
     * </p>
     * Get notified of read reconcilliation completion.
     * Augment the data read from the LSM with the missing
     * mutations from {@code augmentingOffsets}.
     */
    void augment(Id id, Log2OffsetsMap<?> augmentingOffsets)
    {
        Stage.READ.submit(() -> doAugment(id, augmentingOffsets));
    }

    @VisibleForTesting
    Augmented doAugment(Id id, Log2OffsetsMap<?> augmentingOffsets)
    {
        LeaderRead current = reads.get(id);
        if (current == null)
            return null; // expired

        if (current.state() != State.LAUNCHED)
            throw new IllegalStateException("existing read not in LAUNCHED state; this shouldn't be possible");
        Launched launched = (Launched) current;

        /*
         * The initially read data must be augmented by the difference
         * between secondary and initial summary and by reconciled
         * offsets from fellow replicas.
         */
        Set<ShortMutationId> augmentingIds = new HashSet<>();
        MutationSummary initialSummary = launched.initialSummary();
        MutationSummary secondarySummary = launched.secondarySummary();
        MutationSummary.difference(secondarySummary, initialSummary, augmentingIds);
        augmentingOffsets.iterator().forEachRemaining(augmentingIds::add);

        List<PartitionUpdate> augmentingUpdates = new ArrayList<>(augmentingIds.size());
        for (ShortMutationId mutationId : augmentingIds)
        {
            PartitionUpdate update = MutationJournal.instance().fetchPartitionUpdate(mutationId, launched.table());
            // update will be null if the mutation exists in the log, but has no partition updates for our table
            if (update != null) augmentingUpdates.add(update);
        }

        // will return null if there was nothing else to be done (no SRP needed)
        Augmented augmented = launched.augment(augmentingUpdates);
        LeaderRead previous = reads.putIfMatchAllowNull(id, augmented, current);
        if (previous != null && previous != current)
            throw new IllegalStateException("unexpected read encountered in the map; this shouldn't be possible");
        return augmented;
    }

    /**
     * TODO: document
     */
    void extend(Id id, ByteBuffer partition)
    {
        Stage.READ.submit(() -> doExtend(id, partition));
    }

    @VisibleForTesting
    Augmented doExtend(Id id, ByteBuffer partition)
    {
        LeaderRead current = reads.get(id);
        if (current == null)
            return null; // expired

        if (current.state() != State.AUGMENTED)
            throw new IllegalStateException("existing read not in AUGMENTED state; this shouldn't be possible");
        Augmented augmented = (Augmented) current;

        Augmented extended = augmented.extend();
        if (extended == null) // the request is now complete
        {
            LeaderRead previous = reads.remove(id);
            if (previous != null && previous != current)
                throw new IllegalStateException("unexpected read removed; this shouldn't be possible");
        }
        return extended;
    }

    /**
     * TODO: document
     */
    void extend(Id id)
    {
        Stage.READ.submit(() -> doExtend(id));
    }

    @VisibleForTesting
    Augmented doExtend(Id id)
    {
        LeaderRead current = reads.get(id);
        if (current == null)
            return null; // expired

        if (current.state() != State.AUGMENTED)
            throw new IllegalStateException("existing read not in AUGMENTED state; this shouldn't be possible");
        Augmented augmented = (Augmented) current;

        Augmented extended = augmented.append();
        if (extended == null) // the request is now complete
        {
            LeaderRead previous = reads.remove(id);
            if (previous != null && previous != current)
                throw new IllegalStateException("unexpected read removed; this shouldn't be possible");
        }
        return extended;
    }
}
