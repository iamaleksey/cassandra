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
package org.apache.cassandra.db;


import static com.google.common.collect.Iterables.transform;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Row.SimpleBuilder;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.ColumnMetadata.Raw;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.SystemViewQueryPager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

/**
 * An implementation of VirtualTable that will handle paging, aggregations, filtering, selections and limits but
 * requires the entire result set to possibly be loaded into memory.
 *
 * Extending classes must handle the read method which will return up to the entire result but may optionally reduce the
 * returning rows itself based on the select statement.
 */
public abstract class SystemView extends VirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(SystemView.class);

    public SystemView(TableMetadata metadata)
    {
        super(metadata);
    }

    public abstract void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result);

    public ReadQuery getQuery(SelectStatement selectStatement, QueryOptions options, DataLimits limits, int nowInSec)
    {
        ColumnFilter columnFilter = selectStatement.getSelection().newSelectors(options).getColumnFilter();
        ClusteringIndexFilter clusteringFilter = selectStatement.makeClusteringIndexFilter(options, columnFilter);
        AbstractBounds<PartitionPosition> keyBounds = selectStatement.getRestrictions().getPartitionKeyBounds(options);
        if (keyBounds == null)
            return ReadQuery.EMPTY;
        return new SystemViewReadCommand(selectStatement, options, limits, nowInSec, null);
    }

    public class SystemViewReadCommand implements ReadQuery
    {
        public final SelectStatement selectStatement;
        public final StatementRestrictions restrictions;
        public final QueryOptions options;
        public final int nowInSec;
        public final TableMetadata metadata;
        public final DataLimits limits;
        private PagingState state = null;

        public SystemViewReadCommand(SelectStatement selectStatement, QueryOptions options, DataLimits limits, int nowInSec, PagingState state)
        {
            this.selectStatement = selectStatement;
            this.metadata = selectStatement.table;
            this.options = options;
            this.limits = limits;
            this.nowInSec = nowInSec;
            this.restrictions = selectStatement.getRestrictions();
            this.state = state;
        }

        public SystemViewReadCommand withUpdatedLimits(DataLimits newLimits)
        {
            return new SystemViewReadCommand(selectStatement, options, newLimits, nowInSec, state);
        }

        public SystemViewReadCommand withUpdatedLimitsAndState(DataLimits newLimits, PagingState state)
        {
            return new SystemViewReadCommand(selectStatement, options, newLimits, nowInSec, state);
        }


        public ReadExecutionController executionController()
        {
            return ReadExecutionController.empty();
        }

        public ResultBuilder fetch(QueryOptions options)
                throws RequestExecutionException, RequestValidationException
        {
            ResultBuilder result = new ResultBuilder(selectStatement, options);
            Tracing.trace("Building virtual table results from {}", metadata.virtualClass().getName());
            read(restrictions, options, result);
            return result;
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState,
                long queryStartNanoTime) throws RequestExecutionException
        {
            return UnfilteredPartitionIterators.filter(executeLocally(executionController()), nowInSec);
        }

        public PartitionIterator executeInternal(ReadExecutionController controller)
        {
            return UnfilteredPartitionIterators.filter(executeLocally(controller), nowInSec);
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            ResultIterator readState = new ResultIterator(selectStatement, limits, options, fetch(options), state);
            return limits.filter(selectStatement
                    .getRestrictions()
                    .getRowFilter(null, options)
                    .filter(readState, nowInSec),
                    nowInSec, false);
        }

        public DataLimits limits()
        {
            return limits;
        }

        public QueryPager getPager(PagingState state, ProtocolVersion protocolVersion)
        {
            return new SystemViewQueryPager(this, state, protocolVersion);
        }

        /* used for views */
        public boolean selectsKey(DecoratedKey key)
        {
            return false;
        }

        /* used for views */
        public boolean selectsClustering(DecoratedKey key, Clustering clustering)
        {
            return false;
        }

        /* used for views */
        public boolean selectsFullPartition()
        {
            return false;
        }
    }

    private class ResultIterator extends AbstractIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        private final Deque<Entry<DecoratedKey, List<Row>>> partitions;
        private final ColumnFilter columnFilter;
        private final ClusteringIndexFilter clusteringFilter;
        private final DataLimits limits;
        private PagingState state;
        private boolean first = true;

        public ResultIterator(SelectStatement selectStatement, DataLimits limits, QueryOptions options,
                ResultBuilder results, PagingState state)
        {
            this.limits = limits;
            this.state = state;

            columnFilter = selectStatement.getSelection().newSelectors(options).getColumnFilter();
            clusteringFilter = selectStatement.makeClusteringIndexFilter(options, columnFilter);
            results.update();
            for (Entry<DecoratedKey, List<Row>> e : results.values.entrySet())
            {
                List<Row> rows = e.getValue();
                if (selectStatement.parameters.orderings.isEmpty())
                    rows.sort(metadata.comparator);
                else
                {
                    // order by support here, make the data the order requested
                    ClusteringComparator clustering =
                            new ClusteringComparator(transform(metadata.clusteringColumns(),
                                    c -> {
                                        Boolean val = selectStatement.parameters.orderings.get(Raw.forUnquoted(c.name.toString()));
                                        if (val != null)
                                            return val? ReversedType.getInstance(c.type) : c.type;
                                        return c.type;
                                    }));
                    rows.sort(clustering);
                }
            }
            partitions = new LinkedList<>(results.values.entrySet());

            if (state != null)
            {
                // skip ahead to where next partition was last one read
                while (!partitions.isEmpty() && partitionComparator()
                        .compare(partitions.peek().getKey(), metadata.partitioner.decorateKey(state.partitionKey)) < 0)
                    partitions.pop();

                // at end of partition so can move to next
                if (state.rowMark == null && !partitions.isEmpty())
                    partitions.pop();
            }
        }

        public void close()
        {
        }

        @Override
        public TableMetadata metadata()
        {
            return metadata;
        }

        @Override
        protected synchronized UnfilteredRowIterator computeNext()
        {
            if (partitions.isEmpty())
            {
                return endOfData();
            }

            Entry<DecoratedKey, List<Row>> currentPartition = partitions.pop();

            List<Row> allRows = currentPartition.getValue();
            Deque<Row> currentRows = new LinkedList<>(allRows.subList(0, Math.min(allRows.size(), limits.perPartitionCount())));

            if (first)
            {
                while (state != null && !currentRows.isEmpty() && state.rowMark != null
                        && metadata.comparator.compare(currentRows.pop().clustering(), state.rowMark.clustering(metadata)) < 0);
                first = false;
            }
            if (!currentRows.isEmpty())
            {
                UnfilteredRowIterator r = new AbstractUnfilteredRowIterator(metadata,
                        currentPartition.getKey(),
                        DeletionTime.LIVE,
                        metadata.regularAndStaticColumns(),
                        Rows.EMPTY_STATIC_ROW,
                        false,
                        EncodingStats.NO_STATS)
                {
                    protected Unfiltered computeNext()
                    {
                        if (currentRows.isEmpty())
                        {
                            return endOfData();
                        }
                        return currentRows.pop();
                    }
                };
                return clusteringFilter.filterNotIndexed(columnFilter, r);
            }
            if (!partitions.isEmpty())
            {
                return computeNext();
            }
            return endOfData();
        }

    }

    public class PartitionBuilder
    {
        ResultBuilder parent;
        SimpleBuilder builder;
        DecoratedKey key;
        Row row;

        public PartitionBuilder(ResultBuilder parent, Object... keys)
        {
            this.parent = parent;
            int current = 0;
            int keyLength = metadata.partitionKeyColumns().size();
            key = SimpleBuilders.makePartitonKey(metadata, Arrays.copyOfRange(keys, 0, keyLength));
            current += keyLength;
            int clusteringLength = metadata.clusteringColumns().size();
            Object[] clustering = Arrays.copyOfRange(keys, current, current + clusteringLength);
            builder = new SimpleBuilders.RowBuilder(metadata, clustering);
        }

        public PartitionBuilder column(String key, Object value)
        {
            builder.add(key, value);
            return this;
        }

        public ResultBuilder endRow()
        {
            return parent;
        }
    }

    public Comparator<DecoratedKey> partitionComparator()
    {
        return (p1, p2) ->
        {
            return metadata.partitionKeyType.compare(p1.getKey(), p2.getKey());
        };
    }

    /**
     * Builds the Rows in the appropriate order, ignoring data that would be filtered.
     */
    public class ResultBuilder
    {
        TreeMap<DecoratedKey, List<Row>> values = new TreeMap<>(partitionComparator());
        RowFilter rowFilter;
        RowFilter partitionFilter;
        PartitionBuilder last;
        StatementRestrictions restrictions;
        QueryOptions options;
        boolean hasUnrestrictedPartitionKeyComponents;

        public ResultBuilder(SelectStatement selectStatement, QueryOptions options)
        {
            this.options = options;
            this.rowFilter = RowFilter.create();
            this.partitionFilter = RowFilter.create();
            this.restrictions = selectStatement.getRestrictions();
            hasUnrestrictedPartitionKeyComponents = restrictions.getPartitionKeyRestrictions().hasUnrestrictedPartitionKeyComponents(metadata);
            for (ColumnMetadata cm : metadata.primaryKeyColumns())
            {
                if (hasUnrestrictedPartitionKeyComponents)
                {
                    for (Restriction r : selectStatement.getRestrictions().getPartitionKeyRestrictions()
                            .getRestrictions(cm))
                    {
                        r.addRowFilterTo(partitionFilter, null, options);
                    }
                }
                for (Restriction r : restrictions.getClusteringColumnsRestrictions().getRestrictions(cm))
                {
                    r.addRowFilterTo(rowFilter, null, options);
                }
            }
        }

        public PartitionBuilder row(Object... keys)
        {
            update();
            last = new PartitionBuilder(this, keys);
            return last;
        }

        void update()
        {
            if (last != null)
            {
                try
                {
                    Row row = last.builder.build();
                    boolean included = false;
                    if (!hasUnrestrictedPartitionKeyComponents)
                    {
                        List<ByteBuffer> keys = restrictions.getPartitionKeys(options);
                        included = keys.stream().anyMatch(
                                key -> last.key.equals(metadata.partitioner.decorateKey(ByteBufferUtil.clone(key))));
                    }
                    else
                    {
                        included = partitionFilter.isSatisfiedBy(metadata, last.key, row, FBUtilities.nowInSeconds());
                    }
                    if (included && rowFilter.isSatisfiedBy(metadata, last.key, row, FBUtilities.nowInSeconds()))
                        values.computeIfAbsent(last.key, k -> Lists.newArrayList()).add(row);
                }
                finally
                {
                    last = null;
                }
            }
        }

    }
}
