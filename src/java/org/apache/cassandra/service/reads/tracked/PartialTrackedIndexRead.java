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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadableView;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SimpleBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSource;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.Index.IndexMatch;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.CloseablePeekingIterator;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class PartialTrackedIndexRead<Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> extends AbstractPartialTrackedRead
{
    private final ReadCommand command;
    private final Searcher searcher;

    private ConsistencyLevel consistencyLevel;
    private long expiresAtNanos;

    PartialTrackedIndexRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, ReadCommand command, Searcher searcher)
    {
        super(executionController, cfs, startTimeNanos);
        this.command = command;
        this.searcher = searcher;
    }

    public static <Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> PartialTrackedIndexRead<Match, Searcher> create(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, ReadCommand command, Searcher searcher)
    {
        PartialTrackedIndexRead<Match, Searcher> read = new PartialTrackedIndexRead<>(executionController, cfs, startTimeNanos, command, searcher);
        read.prepare(null);
        return read;
    }

    @Override
    public ReadCommand command()
    {
        return command;
    }

    @Override
    public Searcher searcher()
    {
        return searcher;
    }

    @Override
    public void setFollowUpReadContext(ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        this.consistencyLevel = consistencyLevel;
        this.expiresAtNanos = expiresAtNanos;
    }

    public interface CompletedIndexPartitionRead<Match extends IndexMatch>
    {
        UnfilteredRowIterator readHit(CloseablePeekingIterator<Match> matchIterator);
    }

    public interface CompletedIndexRead<Match extends IndexMatch> extends CompletedRead
    {
        CompletedIndexPartitionRead<Match> partitionRead(ByteBuffer key);
    }

    private static class FollowUpRead<Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> implements CompletedIndexPartitionRead<Match>
    {
        private final DecoratedKey key;
        private final AsyncPromise<TrackedDataResponse> promise;
        private final PartialTrackedIndexRead<Match, Searcher> read;
        private final CompletedIndexRead<Match> completedRead;
        private final CompletedIndexPartitionRead<Match> partitionRead;
        private final ColumnFilter selection;
        private final ConsistencyLevel consistencyLevel;
        private final long expiresAtNanos;

        public FollowUpRead(DecoratedKey key, AsyncPromise<TrackedDataResponse> promise, PartialTrackedIndexRead<Match, Searcher> read, ColumnFilter selection, ConsistencyLevel consistencyLevel, long expiresAtNanos)
        {
            this.key = key;
            this.promise = promise;
            this.read = read;
            this.completedRead = (CompletedIndexRead<Match>) read.complete();
            this.partitionRead = Preconditions.checkNotNull(completedRead.partitionRead(key.getKey()));
            this.selection = selection;
            this.consistencyLevel = consistencyLevel;
            this.expiresAtNanos = expiresAtNanos;
        }

        static <Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> Future<FollowUpRead<Match, Searcher>> start(ReadCommand command, DecoratedKey key, ConsistencyLevel consistencyLevel, long expiresAtNanos)
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            Preconditions.checkState(command instanceof PartitionRangeReadCommand, "additional reads can only be made with range reads");

            PartitionRangeReadCommand rangeReadCommand = (PartitionRangeReadCommand) command;
            SinglePartitionReadCommand partitionReadCommand = SinglePartitionReadCommand.fromRangeRead(key, rangeReadCommand, rangeReadCommand.limits());

            AsyncPromise<FollowUpRead<Match, Searcher>> followUpPromise = new AsyncPromise<>();
            TrackedRead.Partition trackedRead = TrackedRead.create(metadata, partitionReadCommand, consistencyLevel);

            trackedRead.startLocal(expiresAtNanos, null, ((promise1, read, selection1, consistencyLevel1, expiresAtNanos1) -> {
                try
                {
                    followUpPromise.trySuccess(new FollowUpRead<>(key, promise1, (PartialTrackedIndexRead<Match, Searcher>) read, selection1, consistencyLevel1, expiresAtNanos1));
                }
                catch (Exception e)
                {
                    followUpPromise.tryFailure(e);
                }
            }));
            return followUpPromise;
        }

        @Override
        public UnfilteredRowIterator readHit(CloseablePeekingIterator<Match> matchIterator)
        {
            Preconditions.checkState(matchIterator.hasNext());
            Preconditions.checkState(matchIterator.peek().baseKey().equals(key));
            return partitionRead.readHit(matchIterator);
        }
    }

    private static class SnapshotView implements ReadableView
    {
        final List<SinglePartitionSource> snapshots;
        final List<SSTableReader> sstables;
        private AugmentedPartition augmentedPartition = null;

        public SnapshotView(List<SinglePartitionSource> snapshots, List<SSTableReader> sstables)
        {
            this.snapshots = snapshots;
            this.sstables = sstables;
        }

        public static SnapshotView create(DecoratedKey key, ColumnFamilyStore cfs)
        {
            ColumnFamilyStore.ViewFragment view = cfs.select(View.select(SSTableSet.LIVE, key));
            return new SnapshotView(MemtableSnapshot.create(key, view.memtables), view.sstables());
        }

        @Override
        public Iterable<? extends UnfilteredSource> memtables()
        {
            return snapshots;
        }

        @Override
        public List<SSTableReader> sstables()
        {
            return sstables;
        }

        public void augment(PartitionUpdate update)
        {
            if (augmentedPartition == null)
            {
                augmentedPartition = new AugmentedPartition(update.partitionKey(), update.metadata());
                snapshots.add(augmentedPartition);
            }

            augmentedPartition.augment(update);
        }
    }

    private static abstract class SinglePartitionSource implements UnfilteredSource
    {
        abstract Partition partition();

        @Override
        public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter columnFilter, boolean reversed, SSTableReadsListener listener)
        {
            Partition partition = partition();
            Preconditions.checkState(key.equals(partition.partitionKey()));
            return partition.unfilteredIterator(columnFilter, slices, reversed);
        }

        @Override
        public UnfilteredPartitionIterator partitionIterator(ColumnFilter columnFilter, DataRange dataRange, SSTableReadsListener listener)
        {
            throw new IllegalStateException("Range scans not supported");
        }

        @Override
        public long getMinTimestamp()
        {
            return partition().stats().minTimestamp;
        }

        @Override
        public long getMinLocalDeletionTime()
        {
            return partition().stats().minLocalDeletionTime;
        }
    }

    private static class MemtableSnapshot extends SinglePartitionSource
    {
        private final Partition partition;

        public MemtableSnapshot(Partition partition)
        {
            this.partition = partition;
        }

        static List<SinglePartitionSource> create(DecoratedKey key, Iterable<Memtable> memtables)
        {
            List<SinglePartitionSource> snapshots = new ArrayList<>();
            for (Memtable memtable : memtables)
            {
                Partition partition = memtable.snapshotPartition(key);
                if (partition != null)
                    snapshots.add(new MemtableSnapshot(partition));
            }
            return snapshots;
        }

        @Override
        Partition partition()
        {
            return partition;
        }
    }

    private static class AugmentedPartition extends SinglePartitionSource
    {
        private final SimpleBTreePartition data;

        AugmentedPartition(DecoratedKey key, TableMetadata metadata)
        {
            this.data = new SimpleBTreePartition(key, metadata, UpdateTransaction.NO_OP);
        }

        void augment(PartitionUpdate update)
        {
            data.update(update);
        }

        @Override
        Partition partition()
        {
            return data;
        }
    }

    class IndexPartitionRead implements CompletedIndexPartitionRead<Match>
    {
        private final DecoratedKey partitionKey;
        private final SnapshotView view;

        IndexPartitionRead(DecoratedKey partitionKey, SnapshotView view)
        {
            this.partitionKey = partitionKey;
            this.view = view;
        }

        void augment(PartitionUpdate update)
        {
            Preconditions.checkArgument(update.partitionKey().equals(partitionKey));
            view.augment(update);
        }

        @Override
        public UnfilteredRowIterator readHit(CloseablePeekingIterator<Match> matchIterator)
        {
            Preconditions.checkArgument(matchIterator.hasNext());
            Preconditions.checkArgument(matchIterator.peek().baseKey().equals(partitionKey.getKey()));
            return searcher.queryNextMatches(executionController, partitionKey, view, matchIterator);
        }
    }

    IndexPartitionRead createRead(ByteBuffer key, ColumnFamilyStore cfs)
    {
        DecoratedKey partitionKey = command.metadata().partitioner.decorateKey(key);
        SnapshotView view = SnapshotView.create(partitionKey, cfs);
        return new IndexPartitionRead(partitionKey, view);
    }

    @Override
    protected Prepared prepareInternal(UnfilteredPartitionIterator initialData)
    {
        SortedMap<ByteBuffer, IndexPartitionRead> reads = new TreeMap<>();
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
            ByteBuffer key = cmd.partitionKey().getKey();
            IndexPartitionRead partitionRead = createRead(key, cfs);
            reads.put(key, partitionRead);
        }

        try (CloseableIterator<Match> iterator = searcher.matchIterator(executionController))
        {
            Set<Match> matches = new HashSet<>();
            while (iterator.hasNext() && matches.size() < command.limits().count())
            {
                Match match = iterator.next();
                matches.add(match);
                if (!reads.containsKey(match.baseKey()))
                {
                    IndexPartitionRead partitionRead = createRead(match.baseKey(), cfs);
                    reads.put(match.baseKey(), partitionRead);
                }
            }
            return new IndexPrepared(matches, reads);
        }
    }

    private class IndexPrepared extends Prepared
    {
        private final Set<Match> matches;
        private final SortedMap<ByteBuffer, IndexPartitionRead> reads;
        private Index.MatchIndexer<Match> matchIndexer = null;

        // for range scans, if we learn of new keys with matching contents as part of reconciliation, we need
        // to do follow up reads against them since we didn't snapshot memtable contents for the keys during
        // the prepare phase of the read. Futures for those reads are kept here
        private final Map<ByteBuffer, Future<FollowUpRead<Match, Searcher>>> followUpReads = new HashMap<>();

        public IndexPrepared(Set<Match> matches, SortedMap<ByteBuffer, IndexPartitionRead> reads)
        {
            this.matches = matches;
            this.reads = reads;
        }

        @Override
        Completed complete()
        {
            return new IndexCompleted(matches, reads, followUpReads);
        }

        private boolean indexUpdate(PartitionUpdate update)
        {
            if (matchIndexer == null)
                matchIndexer = searcher.matchIndexer();

            int startingSize = matches.size();
            matchIndexer.index(update, matches);
            return matches.size() > startingSize;
        }

        @Override
        public State augment(PartitionUpdate update)
        {
            Preconditions.checkState(consistencyLevel != null,
                                     "PartialTrackedRead#setFollowUpReadContext needs to be called before making reads available for augmenting mutation");
            ByteBuffer key = update.partitionKey().getKey();
            IndexPartitionRead read = reads.get(key);
            if (read == null)
            {
                // TODO: maybe we should immediately start a follow up read if it's likely this key will be included in the response
                if (indexUpdate(update) && !followUpReads.containsKey(key))
                {
                    Future<FollowUpRead<Match, Searcher>> followUpRead = FollowUpRead.start(command, update.partitionKey(), consistencyLevel, expiresAtNanos);
                    followUpReads.put(key, followUpRead);
                }
                return this;
            }

            read.augment(update);
            indexUpdate(update);

            // TODO: calling this method mever results in a state change, remove return?
            return this;
        }
    }

    private class IndexCompleted extends Completed
    {
        private final Set<Match> matches;
        private final SortedMap<ByteBuffer, IndexPartitionRead> reads;
        private final Map<ByteBuffer, Future<FollowUpRead<Match, Searcher>>> followUpReads;

        public IndexCompleted(Set<Match> matches, SortedMap<ByteBuffer, IndexPartitionRead> reads, Map<ByteBuffer, Future<FollowUpRead<Match, Searcher>>> followUpReads)
        {
            this.matches = matches;
            this.reads = reads;
            this.followUpReads = followUpReads;
        }

        @Override
        protected CompletedRead getResult()
        {
            return new IndexCompletedRead(matches, reads, followUpReads);
        }
    }

    private class IndexCompletedRead implements CompletedIndexRead<Match>
    {
        private final SortedMap<ByteBuffer, IndexPartitionRead> reads;
        private final Map<ByteBuffer, Future<FollowUpRead<Match, Searcher>>> followUpReadFutures;
        final CloseablePeekingIterator<Match> matchIter;
        protected boolean followUpRequired = false;

        public IndexCompletedRead(Set<Match> matches, SortedMap<ByteBuffer, IndexPartitionRead> reads, Map<ByteBuffer, Future<FollowUpRead<Match, Searcher>>> followUpReadFutures)
        {
            this.reads = reads;
            this.followUpReadFutures = followUpReadFutures;
            List<Match> matchList = new ArrayList<>(matches);
            matchList.sort(searcher.matchComparator());

            this.matchIter = new AbstractIterator<>() {
                final Iterator<Match> iter = matches.iterator();
                @Override
                protected Match computeNext()
                {
                    return iter.hasNext() ? iter.next() : endOfData();
                }
            };
        }

        private abstract class UnfilteredResultIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
        {
            private final CloseablePeekingIterator<Match> matchIter;

            public UnfilteredResultIterator(CloseablePeekingIterator<Match> matchIter)
            {
                this.matchIter = matchIter;
            }

            @Override
            public TableMetadata metadata()
            {
                return command.metadata();
            }

            abstract UnfilteredRowIterator readFollowUp(ByteBuffer key, CloseablePeekingIterator<Match> matchIter);

            @Override
            protected UnfilteredRowIterator computeNext()
            {
                if (!matchIter.hasNext())
                    return endOfData();

                ByteBuffer nextKey = matchIter.peek().baseKey();
                IndexPartitionRead read = reads.get(nextKey);
                if (read != null)
                    return read.readHit(matchIter);

                return readFollowUp(nextKey, matchIter);
            }

            @Override
            public void close()
            {
                matchIter.close();
            }
        }

        class Initial extends UnfilteredResultIterator
        {
            private final Map<ByteBuffer, Future<FollowUpRead<Match, Searcher>>> followUpReadFutures;
            private boolean followUpRequired = false;

            public Initial(CloseablePeekingIterator<Match> matchIter, Map<ByteBuffer, Future<FollowUpRead<Match, Searcher>>> followUpReadFutures)
            {
                super(matchIter);
                this.followUpReadFutures = followUpReadFutures;
            }

            @Override
            UnfilteredRowIterator readFollowUp(ByteBuffer key, CloseablePeekingIterator<Match> matchIter)
            {
                Future<FollowUpRead<Match, Searcher>> future = followUpReadFutures.get(key);
                if (future == null)
                    throw new IllegalStateException("Received match for key without initial or followup read: " + ByteBufferUtil.bytesToHex(key));

                if (!future.isDone())
                {
                    followUpRequired = true;
                    return endOfData();
                }

                try
                {
                    FollowUpRead<Match, Searcher> followUpRead = future.get();
                    return followUpRead.readHit(matchIter);
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e.getCause());
                }
            }
        }

        class FollowUp extends UnfilteredResultIterator
        {
            private final Map<ByteBuffer, FollowUpRead<Match, Searcher>> followUpReads;

            public FollowUp(CloseablePeekingIterator<Match> matchIter, Map<ByteBuffer, FollowUpRead<Match, Searcher>> followUpReads)
            {
                super(matchIter);
                this.followUpReads = followUpReads;
            }

            @Override
            UnfilteredRowIterator readFollowUp(ByteBuffer key, CloseablePeekingIterator<Match> matchIter)
            {
                FollowUpRead<Match, Searcher> followUpRead = followUpReads.get(key);
                if (followUpRead == null)
                    throw new IllegalStateException("Received match for key without initial or followup read: " + ByteBufferUtil.bytesToHex(key));

                return followUpRead.readHit(matchIter);
            }
        }

        private PartitionIterator filter(UnfilteredPartitionIterator iterator)
        {
//            iterator = searcher.filterCompletedRead(iterator);
            iterator = command.completeTrackedRead(iterator, PartialTrackedIndexRead.this);
            PartitionIterator filtered = UnfilteredPartitionIterators.filter(iterator, command.nowInSec());
            return filtered;
//            PartitionIterator counted = Transformation.apply(filtered, mergedResultCounter);
//            PartitionIterator result = Transformation.apply(counted, new EmptyPartitionsDiscarder());
//            return result;
        }

        @Override
        public TrackedDataResponse response()
        {
            try (Initial iterator = new Initial(matchIter, followUpReadFutures))
            {
                PartitionIterator result = PartitionIterators.loggingIterator(filter(iterator), "PartialTrackedIndexRead#response");
                TrackedDataResponse response = TrackedDataResponse.create(result, command.columnFilter());
                followUpRequired = iterator.followUpRequired;
                return response;
            }
        }

        @Override
        public Future<TrackedDataResponse> followupRead(TrackedDataResponse initialResponse, ConsistencyLevel consistencyLevel, long expiresAtNanos)
        {
            if (!followUpRequired)
                return null;

            // TODO: add normal short read protection support

            AsyncPromise<TrackedDataResponse> promise = new AsyncPromise<>();
            FutureCombiner.allOf(followUpReadFutures.values()).addCallback((result, error) -> {
                if (error != null)
                {
                    promise.tryFailure(error);
                    return;
                }

                Map<ByteBuffer, FollowUpRead<Match, Searcher>> followupReads = new HashMap<>();
                for (FollowUpRead<Match, Searcher> followUpRead : result)
                    followupReads.put(followUpRead.key.getKey(), followUpRead);

                try (FollowUp iterator = new FollowUp(matchIter, followupReads))
                {
                    PartitionIterator initialIterator = initialResponse.makeIterator(command);
                    PartitionIterator followUpIterator = filter(iterator);
                    PartitionIterator concatenated = PartitionIterators.concat(List.of(initialIterator, followUpIterator));
                    promise.trySuccess(TrackedDataResponse.create(concatenated, command.columnFilter()));
                }
                catch (Exception e)
                {
                    promise.tryFailure(e);
                }
            }, Stage.READ.executor());

            return promise;
        }

        @Override
        public void close()
        {
            // TODO: make sure we're closing everything
        }

        @Override
        public CompletedIndexPartitionRead<Match> partitionRead(ByteBuffer key)
        {
            return reads.get(key);
        }
    }

}
