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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.BTreePartitionData;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SimpleBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.RowIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.FilteredRows;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.tracked.LeaderReads.State;
import org.apache.cassandra.service.reads.tracked.TrackedRead.Id;
import org.apache.cassandra.transport.Dispatcher.RequestTime;
import org.apache.cassandra.utils.btree.BTreeMap;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

// TODO: ensure that all iterators are properly closed, and errors are fully handled
// TODO: expiration
// TODO: reverse order
abstract class UnfilteredRangeRead
{
    static final class Created implements LeaderReads.Created
    {
        private final Id id;
        private final ColumnFamilyStore store;
        private final PartitionRangeReadCommand command;
        private final int[] summaryNodes;
        private final RequestTime startedAt;

        private Created(Id id, ColumnFamilyStore store, PartitionRangeReadCommand command, int[] summaryNodes, RequestTime startedAt)
        {
            this.id = id;
            this.store = store;
            this.command = command;
            this.summaryNodes = summaryNodes;
            this.startedAt = startedAt;
        }

        static Created create(Id id, ColumnFamilyStore store, PartitionRangeReadCommand command, int[] summaryNodes, RequestTime startedAt)
        {
            return new Created(id, store, command, summaryNodes, startedAt);
        }

        /*
         * CL ONE/LOCAL_ONE/NODE_LOCAL or RF=1; no reconciliation to perform, so we can skip creation summaries as well
         */
        @Override
        public void executeOnSingleNode(AsyncPromise<TrackedDataResponse> promise)
        {
            ReadExecutionController controller = command.executionController(false);

            UnfilteredPartitionIterator localData;
            try
            {
                localData = command.beginTrackedRead2(controller);
            }
            catch (Throwable t)
            {
                controller.close();
                promise.tryFailure(t);
                throw t;
            }

            UnfilteredPartitionIterator iterator =
                command.completeRead(localData, controller, null, store, startedAt.startedAtNanos());

            promise.trySuccess(createResponse(iterator, command));
        }

        private static TrackedDataResponse createResponse(UnfilteredPartitionIterator unfiltered, PartitionRangeReadCommand command)
        {
            // validate that the sequence of RT markers is correct: open is followed by close, deletion times for both
            // ends equal, and there are no dangling RT bound in any partition.
            unfiltered = RTBoundValidator.validate(unfiltered, RTBoundValidator.Stage.PROCESSED, true);

            PartitionIterator filtered = UnfilteredPartitionIterators.filter(unfiltered, command.nowInSec());
            DataLimits.Counter counter =
                command.limits()
                       .newCounter(command.nowInSec(),
                                   false,
                                   command.selectsFullPartition(),
                                   command.metadata().enforceStrictLiveness()).onlyCount();

            return TrackedDataResponse.create(counter.applyTo(filtered), command.columnFilter());
        }

        @Override
        public Launched launch(AsyncPromise<TrackedDataResponse> promise)
        {
            MutationSummary initialSummary, secondarySummary;
            initialSummary = command.createMutationSummary(false);

            ReadExecutionController controller = command.executionController(false);
            UnfilteredPartitionIterator localPartitions = null;
            LSMData lsmData;
            try
            {
                // read and materialize/freeze the contents of the iterator on heap
                // will have the limits applied, and an artificial RT bound possibly injected
                localPartitions = command.beginTrackedRead2(controller);
                localPartitions = command.completeRead(localPartitions, controller, null, store, startedAt.startedAtNanos());
                lsmData = LSMData.materialize(localPartitions);

                // catch any mutations that may have arrived during initial read execution
                secondarySummary = command.createMutationSummary(true);
            }
            catch (Throwable t)
            {
                promise.tryFailure(t);
                throw t;
            }
            finally
            {
                controller.close();
                if (localPartitions != null)
                    localPartitions.close();
            }

            return new Launched(id, store, command, controller, lsmData,
                                initialSummary, secondarySummary, summaryNodes,
                                startedAt, promise);
        }

        @Override
        public TrackedRead.Id id()
        {
            return id;
        }

        @Override
        public State state()
        {
            return State.CREATED;
        }

        @Override
        public int[] summaryNodes()
        {
            return summaryNodes;
        }
    }

    static final class Launched implements LeaderReads.Launched
    {
        private final Id id;
        private final ColumnFamilyStore store;
        private final PartitionRangeReadCommand command;
        private final ReadExecutionController controller;
        private final MutationSummary initialSummary;
        private final MutationSummary secondarySummary;
        private final int[] summaryNodes;
        private final RequestTime startedAt;
        private final AsyncPromise<TrackedDataResponse> promise;

        private final LSMData lsmData;
        private final JournalData journalData = new JournalData();
        private final MergedData mergedData = new MergedData();

        private Launched(Id id, ColumnFamilyStore store, PartitionRangeReadCommand command,
                         ReadExecutionController controller, LSMData lsmData,
                         MutationSummary initialSummary, MutationSummary secondaySummary, int[] summaryNodes,
                         RequestTime startedAt, AsyncPromise<TrackedDataResponse> promise)
        {
            this.id = id;
            this.store = store;
            this.command = command;
            this.controller = controller;
            this.lsmData = lsmData;
            this.initialSummary = initialSummary;
            this.secondarySummary = secondaySummary;
            this.summaryNodes = summaryNodes;
            this.startedAt = startedAt;
            this.promise = promise;
        }

        @Override
        public Augmented augment(List<PartitionUpdate> updates)
        {
            for (PartitionUpdate update : updates)
                journalData.offer(update);

            while (true)
            {
                Result result = mergeNext(command, lsmData, journalData, mergedData);
                if (result == Result.DONE)
                    break;
            }
            promise.trySuccess(mergedData.makeResponse());
            return null;

            // TODO: might be done, might need SRP, return null if the former
            // return new Augmented(id, lsmData, journalData, mergedData);
        }

        @Override
        public Id id()
        {
            return id;
        }

        @Override
        public TableMetadata table()
        {
            return command.metadata();
        }

        @Override
        public State state()
        {
            return State.LAUNCHED;
        }

        @Override
        public MutationSummary initialSummary()
        {
            return initialSummary;
        }

        @Override
        public MutationSummary secondarySummary()
        {
            return secondarySummary;
        }

        @Override
        public int[] summaryNodes()
        {
            return summaryNodes;
        }
    }

    static final class Augmented implements LeaderReads.Augmented
    {
        private final Id id;

        private final LSMData lsmData;
        private final JournalData journalData;
        private final MergedData mergedData;

        private Augmented(Id id, LSMData lsmData, JournalData journalData, MergedData mergedData)
        {
            this.id = id;
            this.lsmData = lsmData;
            this.journalData = journalData;
            this.mergedData = mergedData;
        }

        @Override
        public Id id()
        {
            return id;
        }

        @Override
        public State state()
        {
            return State.AUGMENTED;
        }

        @Override
        public Augmented extend()
        {
            return null;
        }

        @Override
        public Augmented append()
        {
            return null;
        }
    }

    /*
     * 0. No results, empty LSM, empty journal          - DONE
     * 1. One partition, no journal, no row filter      - DONE
     * 2. Range of partitions, no journal               - DONE
     * 3. One from Journal, no LSM
     */

    enum Result { CONTINUE, DONE }

    /*
     * TODO: think RTBoundsCloser potentially shortening a range tombstone
     *       that would otherwise shadow stuff from journal's PartitionUpdates
     *       does it affect no-limit case or only the yes-limit case?
     * TODO: reverse orders (partition and row)
     * TODO: per partition limits, overall limits
     * TODO: make the data structures safe for concurrency/visibility
     */
    private static Result mergeNext(ReadCommand command, LSMData lsm, JournalData journal, MergedData merged)
    {
        /*
         * 1. Peek next from LSMData and Journal
         * 2. If partition key not equal, poll the smallest one, put into merged data
         * 3. If equal, do the merge; possibly stop and trigger SRP
         *      TODO: ELABORATE
         * 4. If done w/ limits, finish
         */
        DecoratedKey lsmKey = lsm.peek();
        DecoratedKey journalKey = journal.peek();

        if (lsmKey == null && journalKey == null) // both sources have been exhausted
        {
            // no augmentations from the journal - only LSM partitions
            if (journal.pollCount() == 0)
                return Result.DONE;

            // may need to read extra if the result was shortened;
            // can't rely on paging - it could be disabled for the query
            throw new UnsupportedOperationException("Not yet implemented");
        }
        else if (journalKey == null)
        {
            if (journal.pollCount() == 0)
            {
                // no augmentations from the journal - only LSM partitions
                merged.offer(lsm.poll(), command.columnFilter(), command.nowInSec());
                return Result.CONTINUE;
            }

            // could be this, but could also have a shortened last partition
            // or shortened total due to the total limit being reached prematurely;
            // need special logic for when it's the very last lsm partition
            throw new UnsupportedOperationException("Not yet implemented");
        }
        else if (lsmKey == null)
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
        else if (lsmKey.compareTo(journalKey) < 0)
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
        else if (lsmKey.compareTo(journalKey) > 0)
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
        else
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    /**
     * Represents the data that was read from the LSM (CFS).
     * TODO: this is supposed to be row-filtered, and with limits applied
     * TODO: the bound closer is an issue potentially? to be sure,
     *       mark short if the data we are merging with is on boundary?
     *       this is only an issue with per partition limit, and with regular limit for the last fetched row
     * Here we want to minimize how much we materialize to the absolute minimum if possible.
     * TODO: consider the cost of materializing many row/range tombstones in memory;
     *       consider an escape hatch into the special single-partition SRP read if we hit a certain limit
     *       of tombstones to materialize
     * TODO: okay, how different is the case of filtering out a bunch of rows because of ALLOW FILTERING
     *       and doing the same for a bunch of range tombstones and such for un-filtered, if there are too many?
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private static final class LSMData
    {
        private final TableMetadata metadata;

        // the fields won't be updated concurrently, but execution of the request will often take place over multiple
        // threads, serially, as each stage of the request may happen inside a different thread of the READ Stage.
        private volatile BTreeMap<DecoratedKey, BTreePartitionData> partitions;
        private volatile int pollCount;

        private LSMData(TableMetadata metadata, BTreeMap<DecoratedKey, BTreePartitionData> partitions)
        {
            this.metadata = metadata;
            this.partitions = partitions;
        }

        static LSMData materialize(UnfilteredPartitionIterator iterator)
        {
            BTreeMap<DecoratedKey, BTreePartitionData> partitions = BTreeMap.empty(DecoratedKey.comparator);
            while (iterator.hasNext())
            {
                UnfilteredRowIterator rows = iterator.next();
                partitions = partitions.with(rows.partitionKey(), SimpleBTreePartition.makePartitionDataFromIterator(rows));
            }
            return new LSMData(iterator.metadata(), partitions);
        }

        @Nullable
        DecoratedKey peek()
        {
            return partitions.firstKey();
        }

        @Nonnull
        SimpleBTreePartition poll()
        {
            Map.Entry<DecoratedKey, BTreePartitionData> first = partitions.firstEntry();
            if (first == null)
                throw new IllegalStateException(); // peek() should confirm element existence first; this shouldn't happen

            partitions = partitions.without(first.getKey());
            pollCount++;

            return new SimpleBTreePartition(first.getKey(), metadata, UpdateTransaction.NO_OP);
        }

        /*
         * Count of partitions that were polled from this set.
         */
        int pollCount()
        {
            return pollCount;
        }
    }

    /**
     * Represents the augmenting data, read from the {@link org.apache.cassandra.replication.MutationJournal}.
     * TODO: fill this up, ensure it's row-filtered, and mutations for the same key are merged
     * Here we want to keep data as references to PartitionUpdate from Mutation from Journal until the very last
     * moment. Rely on mutation cache. Only merge when dealing with the partition.
     * TODO: handle multiple partition updates to merge for the same key
     * TODO: mention the mutation journal cache and only needed recent mutations usually, why PartitionUpdate is the best
     *       structure to keep
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private static final class JournalData
    {
        // the fields won't be updated concurrently, but execution of the request will often take place over multiple
        // threads, serially, as each stage of the request may happen inside a different thread of the READ Stage.
        private volatile BTreeMap<DecoratedKey, PartitionUpdate> updates = BTreeMap.empty(DecoratedKey.comparator);
        private volatile int pollCount;

        @Nullable
        private DecoratedKey peek()
        {
            return updates.firstKey();
        }

        @Nonnull
        PartitionUpdate poll()
        {
            Map.Entry<DecoratedKey, PartitionUpdate> first = updates.firstEntry();
            if (first == null)
                throw new IllegalStateException(); // peek() should confirm element existence first; this shouldn't happen

            updates = updates.without(first.getKey());
            pollCount++;

            return first.getValue();
        }

        void offer(PartitionUpdate update)
        {
            updates = updates.with(update.partitionKey(), update);
        }

        /*
         * Count of partitions that were polled from this set.
         */
        int pollCount()
        {
            return pollCount;
        }
    }

    /**
     * LSM data and journal data - combined.
     * An ordered map of {partition key -> serialized filtered (no tombstones) partition}
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private static final class MergedData
    {
        // the fields won't be updated concurrently, but execution of the request will often take place over multiple
        // threads, serially, as each stage of the request may happen inside a different thread of the READ Stage.
        private volatile BTreeMap<DecoratedKey, ByteBuffer> partitions = BTreeMap.empty(DecoratedKey.comparator);

        void offer(UnfilteredRowIterator unfilteredPartition, ColumnFilter columnFilter, long nowInSeconds)
        {
            RowIterator filteredPartition = FilteredRows.filter(unfilteredPartition, nowInSeconds);
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                RowIterators.Serializer.serialize(filteredPartition, columnFilter, out, MessagingService.current_version);
                partitions = partitions.with(unfilteredPartition.partitionKey(), out.buffer(false));
            }
            catch (IOException e)
            {
                throw new AssertionError(e); // we're serializing in memory - this should never happen
            }
        }

        void offer(SimpleBTreePartition unfilteredPartition, ColumnFilter columnFilter, long nowInSeconds)
        {
            offer(unfilteredPartition.unfilteredIterator(), columnFilter, nowInSeconds);
        }

        TrackedDataResponse makeResponse()
        {
            // TODO
            return null;
        }

        int partitionCount()
        {
            return partitions.size();
        }
    }
}
