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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadableView;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SimpleBTreePartition;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.Index.IndexMatch;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.CloseablePeekingIterator;
import org.apache.cassandra.utils.concurrent.Future;

public class PartialTrackedIndexRead<Match extends IndexMatch, Searcher extends Index.MultiStepSearcher<Match>> extends AbstractPartialTrackedRead
{
    private final ReadCommand command;
    private final Searcher searcher;

    public PartialTrackedIndexRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, ReadCommand command, Searcher searcher)
    {
        super(executionController, cfs, startTimeNanos);
        this.command = command;
        this.searcher = searcher;
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

    static ReadableView freezeView(ColumnFamilyStore.ViewFragment view)
    {
        throw new UnsupportedOperationException("TODO: freeze memtable state");
    }

    class IndexPartitionRead
    {
        private final DecoratedKey partitionKey;
        private final ReadableView view;
        private SimpleBTreePartition augmentedData = null;

        IndexPartitionRead(DecoratedKey partitionKey, ReadableView view)
        {
            this.partitionKey = partitionKey;
            this.view = view;
        }

        void augment(PartitionUpdate update)
        {
            Preconditions.checkArgument(update.partitionKey().equals(partitionKey));
            if (augmentedData == null)
                augmentedData = new SimpleBTreePartition(partitionKey, command.metadata(), UpdateTransaction.NO_OP);

            augmentedData.update(update);
        }

        UnfilteredRowIterator readHit(CloseablePeekingIterator<Match> matchIterator)
        {
            Preconditions.checkArgument(matchIterator.hasNext());
            Preconditions.checkArgument(matchIterator.peek().baseKey().equals(partitionKey.getKey()));
            return searcher.queryNextMatches(executionController, partitionKey, view, matchIterator);
        }
    }

    IndexPartitionRead createRead(ByteBuffer key, ColumnFamilyStore cfs)
    {
        DecoratedKey partitionKey = command.metadata().partitioner.decorateKey(key);
        ReadableView view = freezeView(cfs.select(View.select(SSTableSet.LIVE, partitionKey)));
        return new IndexPartitionRead(partitionKey, view);
    }

    @Override
    protected Prepared prepareInternal(UnfilteredPartitionIterator initialData)
    {
        // TODO: materialize enough hits to satisfy limit
        // TODO: reference memtable and sstables
        try (CloseableIterator<Match> iterator = searcher.matchIterator(executionController))
        {
            Set<Match> matches = new HashSet<>();
            SortedMap<ByteBuffer, IndexPartitionRead> reads = new TreeMap<>();
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
        private final Set<ByteBuffer> newKeys = new HashSet<>();
        private Index.MatchIndexer<Match> matchIndexer = null;

        public IndexPrepared(Set<Match> matches, SortedMap<ByteBuffer, IndexPartitionRead> reads)
        {
            this.matches = matches;
            this.reads = reads;
        }

        @Override
        Completed complete()
        {
            return new IndexCompleted(matches, reads, newKeys);
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
            ByteBuffer key = update.partitionKey().getKey();
            IndexPartitionRead read = reads.get(key);
            if (read == null)
            {
                if (indexUpdate(update))
                    newKeys.add(key);
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
        private final Set<ByteBuffer> newKeys;

        public IndexCompleted(Set<Match> matches, SortedMap<ByteBuffer, IndexPartitionRead> reads, Set<ByteBuffer> newKeys)
        {
            this.matches = matches;
            this.reads = reads;
            this.newKeys = newKeys;
        }

        @Override
        protected CompletedRead getResult()
        {
            return new IndexCompletedRead(matches, reads, newKeys);
        }
    }

    private class IndexCompletedRead implements CompletedRead
    {
        private final Set<Match> matches;
        private final SortedMap<ByteBuffer, IndexPartitionRead> reads;
        private final Set<ByteBuffer> newKeys;

        public IndexCompletedRead(Set<Match> matches, SortedMap<ByteBuffer, IndexPartitionRead> reads, Set<ByteBuffer> newKeys)
        {
            this.matches = matches;
            this.reads = reads;
            this.newKeys = newKeys;
        }

        private class UnfilteredResultIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
        {
            final CloseablePeekingIterator<Match> matchIter;

            UnfilteredResultIterator(Set<Match> matches)
            {
                List<Match> matchList = new ArrayList<>(matches);
                matchList.sort(searcher.matchComparator());
                this.matchIter = new AbstractIterator<>()
                {
                    final Iterator<Match> iter = matchList.iterator();

                    @Override
                    protected Match computeNext()
                    {
                        return iter.hasNext() ? iter.next() : endOfData();
                    }
                };
            }

            @Override
            public TableMetadata metadata()
            {
                return command.metadata();
            }

            @Override
            protected UnfilteredRowIterator computeNext()
            {
                if (!matchIter.hasNext())
                    return endOfData();

                IndexPartitionRead read = reads.get(matchIter.peek().baseKey());
                if (read == null)
                    throw new IllegalStateException("Handle short reads");

                return read.readHit(matchIter);
            }

            @Override
            public void close()
            {
                matchIter.close();
            }
        }

        @Override
        public TrackedDataResponse response()
        {
            try (UnfilteredResultIterator iterator = new UnfilteredResultIterator(matches))
            {
                // TODO: filter
                // TODO: detect attempted read of key we didn't initially read and create a followup read, merging that result in with this one
                // TODO: do a post filter of the results, we may have had false positive matches

                throw new UnsupportedOperationException("TODO");
            }
        }

        @Override
        public Future<TrackedDataResponse> followupRead(TrackedDataResponse initialResponse, ConsistencyLevel consistencyLevel, long expiresAtNanos)
        {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void close()
        {
            throw new UnsupportedOperationException("TODO");
        }
    }

}
