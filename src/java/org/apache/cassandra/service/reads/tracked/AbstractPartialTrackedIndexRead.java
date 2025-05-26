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

import java.util.ArrayList;
import java.util.Comparator;
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
import org.apache.cassandra.index.Index.MultiStepSearcher.IndexMatch;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.concurrent.Future;

public abstract class AbstractPartialTrackedIndexRead<Match extends IndexMatch> extends AbstractPartialTrackedRead
{
    private final ReadCommand command;
    private final Index.MultiStepSearcher<Match> searcher;

    public AbstractPartialTrackedIndexRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, ReadCommand command, Index.MultiStepSearcher<Match> searcher)
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
    public Index.Searcher searcher()
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

        UnfilteredPartitionIterator readHit(Match match)
        {
            return searcher.queryMatch(view, match);
        }
    }

    IndexPartitionRead createRead(DecoratedKey partitionKey, ColumnFamilyStore cfs)
    {
        ReadableView view = freezeView(cfs.select(View.select(SSTableSet.LIVE, partitionKey)));
        return new IndexPartitionRead(partitionKey, view);
    }

    @Override
    protected Prepared prepareInternal(UnfilteredPartitionIterator initialData)
    {
        // TODO: materialize enough hits to satisfy limit
        // TODO: reference memtable and sstables
        Iterator<Match> iterator = queryIndex();
        Set<Match> matches = new HashSet<>();
        SortedMap<DecoratedKey, IndexPartitionRead> reads = new TreeMap<>();
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

    private class IndexPrepared extends Prepared
    {
        private final Set<Match> matches;
        private final SortedMap<DecoratedKey, IndexPartitionRead> reads;
        private final Set<DecoratedKey> newKeys = new HashSet<>();
        private final Set<Match> newMatches = new HashSet<>();

        public IndexPrepared(Set<Match> matches, SortedMap<DecoratedKey, IndexPartitionRead> reads)
        {
            this.matches = matches;
            this.reads = reads;
        }

        @Override
        Completed complete()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public State augment(PartitionUpdate update)
        {
            DecoratedKey key = update.partitionKey();
            IndexPartitionRead read = reads.get(key);
            if (read == null)
            {
                if (searcher.isPossibleHit(update))
                {
                    newKeys.add(key);
                    newMatches.add(createHitMarker(update));
                }
                return this;
            }

            read.augment(update);
            if (searcher.isPossibleHit(update))
                matches.add(createHitMarker(update));

            // TODO: calling this method mever results in a state change, remove return?
            return this;
        }
    }

    private class IndexCompleted extends Completed
    {
        private final Set<Match> matches;
        private final SortedMap<DecoratedKey, IndexPartitionRead> reads;
        private final Set<DecoratedKey> newKeys;
        private final Set<Match> newMatches;

        public IndexCompleted(Set<Match> matches, SortedMap<DecoratedKey, IndexPartitionRead> reads, Set<DecoratedKey> newKeys, Set<Match> newMatches)
        {
            this.matches = matches;
            this.reads = reads;
            this.newKeys = newKeys;
            this.newMatches = newMatches;
        }

        @Override
        protected CompletedRead getResult()
        {
            return new IndexCompletedRead(matches, reads, newKeys, newMatches);
        }
    }

    private class IndexCompletedRead implements CompletedRead
    {
        private final Set<Match> matches;
        private final SortedMap<DecoratedKey, IndexPartitionRead> reads;
        private final Set<DecoratedKey> newKeys;
        private final Set<Match> newMatches;

        public IndexCompletedRead(Set<Match> matches, SortedMap<DecoratedKey, IndexPartitionRead> reads, Set<DecoratedKey> newKeys, Set<Match> newMatches)
        {
            this.matches = matches;
            this.reads = reads;
            this.newKeys = newKeys;
            this.newMatches = newMatches;
        }

        private class UnfilteredResultIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
        {
            final Iterator<Match> matchIter;
            UnfilteredPartitionIterator current;

            UnfilteredResultIterator(Set<Match> matches)
            {
                List<Match> matchList = new ArrayList<>(matches);
                matchList.sort(Comparator.naturalOrder());
                this.matchIter = matchList.iterator();
            }

            @Override
            public TableMetadata metadata()
            {
                return command.metadata();
            }

            @Override
            protected UnfilteredRowIterator computeNext()
            {
                while (true)
                {
                    if (current == null || !current.hasNext())
                    {
                        if (current != null)
                        {
                            current.close();
                            current = null;
                        }

                        if (!matchIter.hasNext())
                            return endOfData();

                        Match match = matchIter.next();

                        IndexPartitionRead read = reads.get(match.baseKey());
                        if (read == null)
                            throw new IllegalStateException("Handle short reads");

                        current = read.readHit(match);
                        continue;
                    }
                    return current.next();
                }
            }

            @Override
            public void close()
            {
                if (current != null)
                    current.close();
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

    protected abstract Iterator<Match> queryIndex();
    protected abstract PartialTrackedRead query(Match indexMatch);
    protected abstract Match createHitMarker(PartitionUpdate update);
}
