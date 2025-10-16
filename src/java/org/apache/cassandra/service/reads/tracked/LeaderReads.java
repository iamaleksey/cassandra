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

import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.service.reads.tracked.TrackedRead.Id;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.jctools.maps.NonBlockingHashMap;

class LeaderReads
{
    enum Stage { INITIALIZED, READ, AUGMENTED, SHORT, COMPLETED }

    private static final NonBlockingHashMap<Id, LeaderRead> reads = new NonBlockingHashMap<>();

    interface LeaderRead
    {
        Id id();

        // TODO (consider): getting rid of Stage
        Stage stage();
    }

    interface Created extends LeaderRead
    {
        Launched launch(AsyncPromise<TrackedDataResponse> promise);
    }

    interface Launched extends LeaderRead
    {
        Augmented augment(Log2OffsetsMap<?> augmentingOffsets);
        Id id();
        MutationSummary initialSummary();
        MutationSummary secondarySummary();
        int[] summaryNodes();
    }

    interface Augmented extends LeaderRead
    {
        /*
         * At this point, we either have it all or we may need SRP
         */
        ShortOrCompleted complete();
    }

    interface ShortOrCompleted extends LeaderRead
    {
        boolean isShort();
        boolean isComplete();
        ShortOrCompleted complete();
        void respond();
    }

    /**
     * The main entry point into on-leader read state machine.
     * </p>
     * Trigger the ititial read from the LSM, create the initial
     * and the secondary mutation summaries, and feed the summary
     * to the reconciliation process.
     */
    AsyncPromise<TrackedDataResponse> launch(Created created)
    {
        AsyncPromise<TrackedDataResponse> promise = new AsyncPromise<>();
        Launched launched = created.launch(promise);
        if (reads.put(launched.id(), launched) != null)
            throw new IllegalStateException();
        // TODO: special path for CL.ONE
        ReadReconciliations.instance.acceptLocalSummary(
            launched.id(), launched.secondarySummary(), launched.summaryNodes()
        );
        return promise;
    }

    /**
     * Stage two of on-leader read state machine.
     * </p>
     * Get notified of read reconcilliation completion.
     * Augment the data read from the LSM with the missing
     * mutations from {@code augmentingOffsets}.
     * TODO: potential completion
     */
    void augment(Id id, Log2OffsetsMap<?> augmentingOffsets)
    {
        LeaderRead prev = reads.get(id);
        if (prev == null)
            return; // TODO? handle expiration

        if (prev.stage() != Stage.READ)
            throw new IllegalStateException();
        Launched launched = (Launched) prev;

        // TODO (required): ensure this runs in the appropriate executor Stage
        Augmented augmented = launched.augment(augmentingOffsets);
        ShortOrCompleted shortOrCompleted = augmented.complete();

        if (shortOrCompleted.isShort())
        {
            shortOrCompleted.complete();
        }
        else
        {
        }

//        LeaderRead replaced = reads.putIfMatch(read.id(), augmented, prev);
//        if (replaced == null)
//            return; // TODO? handle expiration
//        if (replaced != prev)
//            throw new IllegalStateException();
    }

    void complete(Id id)
    {
        LeaderRead prev = reads.get(id);
        if (prev == null)
            return; // TODO? handle expiration
    }
}
