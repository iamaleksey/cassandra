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

//import java.util.ArrayList;
//import java.util.List;
//import java.util.Set;
//
//import org.apache.cassandra.db.ColumnFamilyStore;
//import org.apache.cassandra.db.ReadExecutionController;
//import org.apache.cassandra.db.SinglePartitionReadCommand;
//import org.apache.cassandra.db.filter.DataLimits;
//import org.apache.cassandra.db.partitions.PartitionIterator;
//import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
//import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
//import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
//import org.apache.cassandra.db.rows.UnfilteredRowIterator;
//import org.apache.cassandra.db.rows.UnfilteredRowIterators;
//import org.apache.cassandra.db.transform.RTBoundValidator;
//import org.apache.cassandra.replication.MutationJournal;
//import org.apache.cassandra.replication.MutationSummary;
//import org.apache.cassandra.replication.ShortMutationId;
//import org.apache.cassandra.service.reads.tracked.TrackedRead.Id;
//import org.apache.cassandra.service.reads.tracked.LeaderReads.State;
//import org.apache.cassandra.transport.Dispatcher.RequestTime;
//import org.apache.cassandra.utils.concurrent.AsyncPromise;
//
//import static org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener.NOOP;

abstract class SinglePartition
{
//    /**
//     * Initialized single partition read object, ready to perform a local read.
//     * TODO: ensure that all happens in the appropriate Stage
//     * TODO: ensure that everything is closed
//     * TODO: ensure that errors are propagated
//     * TODO: searcher non-null?
//     */
//    static final class Created implements LeaderReads.Created
//    {
//        private final Id id;
//        private final ColumnFamilyStore store;
//        private final SinglePartitionReadCommand command;
//        private final int[] summaryNodes;
//        private final RequestTime startedAt;
//
//        private Created(Id id, ColumnFamilyStore store, SinglePartitionReadCommand command, int[] summaryNodes, RequestTime startedAt)
//        {
//            this.id = id;
//            this.store = store;
//            this.command = command;
//            this.summaryNodes = summaryNodes;
//            this.startedAt = startedAt;
//        }
//
//        static Created create(Id id, ColumnFamilyStore store, SinglePartitionReadCommand command, int[] summaryNodes, RequestTime startedAt)
//        {
//            return new Created(id, store, command, summaryNodes, startedAt);
//        }
//
//        /*
//         * CL ONE/LOCAL_ONE/NODE_LOCAL or RF=1; no reconciliation to perform, so we can skip creation summaries as well
//         */
//        @Override
//        public void executeOnSingleNode(AsyncPromise<TrackedDataResponse> promise)
//        {
//            ReadExecutionController controller = command.executionController(false);
//
//            UnfilteredPartitionIterator localData;
//            try
//            {
//                localData = command.beginTrackedRead2(controller);
//            }
//            catch (Throwable t)
//            {
//                controller.close();
//                promise.tryFailure(t);
//                throw t;
//            }
//
//            UnfilteredPartitionIterator iterator =
//                command.completeRead(localData, controller, null, store, startedAt.startedAtNanos());
//
//            promise.trySuccess(createResponse(iterator, command));
//        }
//
//        /**
//         * Capture the initial mutation summary, perform the local data read,
//         * then capture the secondary mutation summary. After that await reconciliation
//         * completion.
//         */
//        @Override
//        public Launched launch(AsyncPromise<TrackedDataResponse> promise)
//        {
//            MutationSummary initialSummary, secondarySummary;
//            initialSummary = command.createMutationSummary(false);
//            ReadExecutionController controller = command.executionController(false);
//            UnfilteredPartitionIterator localData = null;
//            try
//            {
//                localData = command.beginTrackedRead2(controller);
//
//                // catch any mutations that may have arrived during initial read execution
//                secondarySummary = command.createMutationSummary(true);
//            }
//            catch (Throwable t)
//            {
//                controller.close();
//                if (localData != null)
//                    localData.close();
//                promise.tryFailure(t);
//                throw t;
//            }
//
//            return new Launched(id, store, command, controller, localData,
//                                initialSummary, secondarySummary, summaryNodes,
//                                startedAt, promise);
//        }
//
//        @Override
//        public Id id()
//        {
//            return id;
//        }
//
//        @Override
//        public State state()
//        {
//            return State.CREATED;
//        }
//
//        @Override
//        public int[] summaryNodes()
//        {
//            return summaryNodes;
//        }
//    }
//
//    /**
//     * Local read is completed, now awaiting reconciliation results for augmentation.
//     */
//    static final class Launched implements LeaderReads.Launched
//    {
//        private final Id id;
//        private final ColumnFamilyStore store;
//        private final SinglePartitionReadCommand command;
//        private final ReadExecutionController controller;
//        private final UnfilteredPartitionIterator localData;
//        private final MutationSummary initialSummary;
//        private final MutationSummary secondarySummary;
//        private final int[] summaryNodes;
//        private final RequestTime startedAt;
//        private final AsyncPromise<TrackedDataResponse> promise;
//
//        private Launched(
//            Id id, ColumnFamilyStore store, SinglePartitionReadCommand command, ReadExecutionController controller, UnfilteredPartitionIterator localData,
//            MutationSummary initialSummary, MutationSummary secondaySummary, int[] summaryNodes,
//            RequestTime startedAt, AsyncPromise<TrackedDataResponse> promise)
//        {
//            this.id = id;
//            this.store = store;
//            this.command = command;
//            this.controller = controller;
//            this.localData = localData;
//            this.initialSummary = initialSummary;
//            this.secondarySummary = secondaySummary;
//            this.summaryNodes = summaryNodes;
//            this.startedAt = startedAt;
//            this.promise = promise;
//        }
//
//        @Override
//        public LeaderReads.Augmented augment(Set<ShortMutationId> mutationIds)
//        {
//            UnfilteredPartitionIterator augmentingData = augmentingIterator(mutationIds);
//            UnfilteredPartitionIterator allData = augmentingData != null
//                                                ? UnfilteredPartitionIterators.merge(List.of(localData, augmentingData), NOOP)
//                                                : localData;
//            allData = command.completeRead(allData, controller, null, store, startedAt.startedAtNanos());
//            promise.trySuccess(createResponse(allData, command));
//            return null;
//        }
//
//        private UnfilteredPartitionIterator augmentingIterator(Set<ShortMutationId> mutationIds)
//        {
//            if (mutationIds.isEmpty())
//                return null;
//
//            List<UnfilteredRowIterator> rowIterators = new ArrayList<>(mutationIds.size());
//            for (ShortMutationId mutationId : mutationIds)
//            {
//                UnfilteredRowIterator iter =
//                    MutationJournal.instance.rowIterator(mutationId, command.partitionKey(), command.metadata());
//                if (iter == null)
//                    throw new IllegalStateException("reconciled mutation missing from journal; this shouldn't be possible");
//                rowIterators.add(iter);
//            }
//            UnfilteredRowIterator mergedRowIterator = UnfilteredRowIterators.merge(rowIterators);
//            return new SingletonUnfilteredPartitionIterator(mergedRowIterator);
//        }
//
//        @Override
//        public Id id()
//        {
//            return id;
//        }
//
//        @Override
//        public State state()
//        {
//            return State.LAUNCHED;
//        }
//
//        @Override
//        public MutationSummary initialSummary()
//        {
//            return initialSummary;
//        }
//
//        @Override
//        public MutationSummary secondarySummary()
//        {
//            return secondarySummary;
//        }
//
//        @Override
//        public int[] summaryNodes()
//        {
//            return summaryNodes;
//        }
//    }
//
//    private static TrackedDataResponse createResponse(UnfilteredPartitionIterator unfiltered, SinglePartitionReadCommand command)
//    {
//        // validate that the sequence of RT markers is correct: open is followed by close, deletion times for both
//        // ends equal, and there are no dangling RT bound in any partition.
//        unfiltered = RTBoundValidator.validate(unfiltered, RTBoundValidator.Stage.PROCESSED, true);
//
//        PartitionIterator filtered = UnfilteredPartitionIterators.filter(unfiltered, command.nowInSec());
//        DataLimits.Counter counter =
//        command.limits()
//               .newCounter(command.nowInSec(),
//                           false,
//                           command.selectsFullPartition(),
//                           command.metadata().enforceStrictLiveness()).onlyCount();
//
//        return TrackedDataResponse.create(counter.applyTo(filtered), command.columnFilter());
//    }
}
