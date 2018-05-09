package org.apache.cassandra.db;
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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * A system view implementation based on {@code ConcurrentNavigableMap}s.
 */
public final class MapBasedSystemView implements SystemView
{
    /**
     * Wrapper used to expose some in memory data as a {@code Cell}.
     */
    public static class DataWrapper
    {
        /**
         * The column metadata.
         */
        private final ColumnMetadata metadata;

        /**
         * The supplier used to retrieve the cell value
         */
        private final Supplier<ByteBuffer> supplier;

        /**
         * The consumer used to set the cell value
         */
        private final Consumer<ByteBuffer> consumer;

        public static DataWrapper wrap(ColumnMetadata metadata,
                                       Supplier<ByteBuffer> supplier)
        {
            return new DataWrapper(metadata, supplier, null);
        }

        public static DataWrapper wrap(ColumnMetadata metadata,
                                       Supplier<ByteBuffer> supplier,
                                       Consumer<ByteBuffer> consumer)
        {
            return new DataWrapper(metadata, supplier, consumer);
        }

        private DataWrapper(ColumnMetadata metadata, Supplier<ByteBuffer> supplier, Consumer<ByteBuffer> consumer)
        {
            this.metadata = metadata;
            this.supplier = supplier;
            this.consumer = consumer;
        }

        public boolean isMutable()
        {
            return consumer != null;
        }

        /**
         * Returns the column metadata
         *
         * @return the column metadata
         */
        private ColumnMetadata column()
        {
            return metadata;
        }

        /**
         * Returns the {@code Cell}.
         *
         * @param now the timestamp
         * @return {@code Cell} corresponding to the current column value.
         */
        public Cell getCell(long now)
        {
            return BufferCell.live(column(), now, supplier.get());
        }

        /**
         * Sets the {@code Cell} value.
         *
         * @param cell the new cell value.
         */
        public void setCell(Cell cell)
        {
            assert consumer != null;
            consumer.accept(cell.value());
        }
    }

    /**
     * The view metadata
     */
    private final TableMetadata metadata;

    /**
     * Represents the view containing the data.
     */
    protected final NavigableMap<DecoratedKey, Partition> view = new ConcurrentSkipListMap<>(DecoratedKey.comparator);

    public MapBasedSystemView(String table,
                              String comment,
                              ColumnMetadata... columns)
    {
        this.metadata = TableMetadata.ofSystemView(table, comment, columns);
    }

    @Override
    public final TableMetadata metadata()
    {
        return metadata;
    }

    /**
     * Adds to the specified row to the given partition overriding any existing one.
     *
     * @param partitionKey the partition key value
     * @param columns the row columns
     */
    public final void addRow(Object partitionKey,
                             DataWrapper... columns)
    {
        addRow(toDecoratedKey(partitionKey), Clustering.EMPTY, columns);
    }

    /**
     * Adds to the specified row to the given partition overriding any existing one.
     *
     * @param partitionKey the partition key value
     * @param columns the row columns
     */
    public final void addRow(Object partitionKey,
                             Object clustering,
                             DataWrapper... columns)
    {
        addRow(toDecoratedKey(partitionKey), toClustering(clustering), columns);
    }

    /**
     * Adds to the specified row to the given partition overriding any existing one.
     *
     * @param partitionKey the partition key value
     * @param firstClustering the first clustering column value
     * @param secondClustering the second clustering column value
     * @param columns the row columns
     */
    public final void addRow(Object partitionKey,
                             Object firstClustering,
                             Object secondClustering,
                             DataWrapper... columns)
    {
        addRow(toDecoratedKey(partitionKey), toClustering(firstClustering, secondClustering), columns);
    }

    /**
     * Adds to the specified row to the given partition overriding any existing one.
     *
     * @param partitionKey the partition key value
     * @param firstClustering the first clustering column value
     * @param secondClustering the second clustering column value
     * @param thirdClustering the third clustering column value
     * @param columns the row columns
     */
    public final void addRow(Object partitionKey,
                             Object firstClustering,
                             Object secondClustering,
                             Object thirdClustering,
                             DataWrapper... columns)
    {
        addRow(toDecoratedKey(partitionKey), toClustering(firstClustering, secondClustering, thirdClustering), columns);
    }

    private Clustering toClustering(Object... clusteringComponents)
    {
        ImmutableList<ColumnMetadata> clusteringColumns = metadata.clusteringColumns();
        assert clusteringColumns.size() == clusteringComponents.length;
        ByteBuffer[] buffers = new ByteBuffer[clusteringColumns.size()];
        for (int i = 0, m = clusteringColumns.size(); i < m; i++)
        {
            ColumnMetadata column = clusteringColumns.get(i);
            buffers[i] = decompose(column.type, clusteringComponents[i]);
        }
        return Clustering.make(buffers);
    }

    @SuppressWarnings("unchecked")
    private static <T> ByteBuffer decompose(AbstractType<?> type, T value)
    {
        return ((AbstractType<T>) type).decompose(value);
    }

    private <T> DecoratedKey toDecoratedKey(T pk)
    {
        ByteBuffer key = decompose(metadata().partitionKeyType, pk);
        return metadata().partitioner.decorateKey(key);
    }

    /**
     * Adds to the specified row to the given partition overriding any existing one.
     *
     * @param partitionKey the partition key
     * @param clustering the row clustering
     * @param columns the row columns
     */
    private final void addRow(DecoratedKey partitionKey,
                              Clustering clustering,
                              DataWrapper... columns)
    {
        validateClustering(clustering);
        validateColumns(columns);

        Partition partition = view.computeIfAbsent(partitionKey, dk -> Partition.create(dk, metadata.comparator));
        partition.addRow(new Row(clustering, columns));
    }

    /**
     * Validates that the clustering has the expected size.
     *
     * @param clustering the {@code Clustering} to check.
     */
    private void validateClustering(Clustering clustering)
    {
        if (metadata.clusteringColumns().size() != clustering.size())
            throw new IllegalArgumentException("Invalid clustering [expected size: " + metadata.clusteringColumns().size() + " actual size: " + clustering.size());
    }

    /**
     * Validates that the columns are part of the table schema.
     *
     * @param columns the {@code ColumnAdapter} to check.
     */
    private void validateColumns(DataWrapper... columns)
    {
        for (DataWrapper provider : columns)
        {
            ColumnMetadata column = provider.column();
            ColumnMetadata expected = metadata.getColumn(column.name);
            if (!column.equals(expected))
                throw new IllegalArgumentException("Unknown column: " + column);
        }
    }

    /**
     * Removes the specified row.
     * @param partitionKey the row partition
     * @param clustering the row clustering key
     */
    public void removeRow(Object partitionKey, Object... clustering)
    {
        removeRow(toDecoratedKey(partitionKey), toClustering(clustering));
    }

    /**
     * Removes the specified row.
     * @param partitionKey the row partition
     * @param clustering the row clustering key
     */
    private void removeRow(DecoratedKey partitionKey, Clustering clustering)
    {
        Partition partition = view.get(partitionKey);
        partition.removeRow(clustering);
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        Partition partition = view.get(update.partitionKey());
        checkNotNull(partition, "INSERT are not supported on system views");
        partition.apply(update);
    }

    @Override
    public final UnfilteredPartitionIterator select(DataRange dataRange, ColumnFilter columnFilter)
    {
        long now = System.currentTimeMillis();

        if (view.isEmpty())
            return EmptyIterators.unfilteredPartition(metadata);

        Iterator<Entry<DecoratedKey, Partition>> iterator = getPartitions(dataRange);

        return new AbstractUnfilteredPartitionIterator()
        {
            @Override
            public UnfilteredRowIterator next()
            {
                Entry<DecoratedKey, Partition> entry = iterator.next();
                DecoratedKey key = entry.getKey();
                return entry.getValue().toRowIterator(metadata, dataRange.clusteringIndexFilter(key), columnFilter, now);
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public TableMetadata metadata()
            {
                return metadata;
            }
        };
    }

    /**
     * Returns the requested partitions.
     *
     * @param dataRange the selected data range
     * @return an iterator containing the requested partitions.
     */
    private Iterator<Entry<DecoratedKey, Partition>> getPartitions(DataRange dataRange)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();
        PartitionPosition startKey = keyRange.left;
        PartitionPosition endKey = keyRange.right;

        // We optimize the filtering by trying to narrow down the set of metrics if we can.
        NavigableMap<DecoratedKey, Partition> selection = view;

        if (startKey.isMinimum())
        {
            if (endKey.isMinimum())
                return selection.entrySet().iterator();

            if (endKey instanceof DecoratedKey)
            {
                return selection.headMap((DecoratedKey) endKey, keyRange.isEndInclusive())
                                .entrySet()
                                .iterator();
            }
        }
        else
        {
            if (startKey instanceof DecoratedKey)
            {
                if (endKey instanceof DecoratedKey)
                {
                    return selection.subMap((DecoratedKey) endKey,
                                            keyRange.isStartInclusive(),
                                            (DecoratedKey) endKey,
                                            keyRange.isEndInclusive())
                                    .entrySet()
                                    .iterator();
                }

                selection = selection.tailMap((DecoratedKey) startKey, keyRange.isStartInclusive());
            }

            if (endKey instanceof DecoratedKey)
            {
                selection = selection.headMap((DecoratedKey) endKey, keyRange.isEndInclusive());
            }
        }

        // If we have reach this point it means that one of the PartitionPosition is a KeyBound and we have
        // to use filtering for eliminating the unwanted partitions.
        Iterator<Entry<DecoratedKey, Partition>> iterator = selection.entrySet().iterator();

        return new AbstractIterator<Entry<DecoratedKey, Partition>>()
        {
            private boolean withinRange;

            @Override
            protected Entry<DecoratedKey, Partition> computeNext()
            {
                while (iterator.hasNext())
                {
                    Entry<DecoratedKey, Partition> entry = iterator.next();
                    if (dataRange.contains(entry.getKey()))
                    {
                        withinRange = true;
                        return entry;
                    }
                    if (withinRange)
                        return endOfData(); // We have reach the end of the range.
                }
                return endOfData();
            }
        };
    }

    @Override
    public final UnfilteredPartitionIterator select(DecoratedKey partitionKey,
                                                          ClusteringIndexFilter clusteringIndexFilter,
                                                          ColumnFilter columnFilter)
    {
        long now = System.currentTimeMillis();
        Partition partition = view.get(partitionKey);

        if (partition == null)
            return EmptyIterators.unfilteredPartition(metadata);

        UnfilteredRowIterator rowIterator = partition.toRowIterator(metadata(),
                                                                    clusteringIndexFilter,
                                                                    columnFilter,
                                                                    now);
        return new SingletonUnfilteredPartitionIterator(rowIterator);
    }

    /**
     * Represnts a system view partition.
     */
    private static abstract class Partition
    {
        /**
         * The partition key.
         */
        private final DecoratedKey dk;

        private Partition(DecoratedKey dk)
        {
            this.dk = dk;
        }

        /**
         * Applies the specified update to this partition.
         * @param update the update to apply.
         */
        public abstract void apply(PartitionUpdate update);

        public static Partition create(DecoratedKey dk, ClusteringComparator comparator)
        {
            return comparator.size() == 0 ? new PartitionWithoutClusteringColumns(dk) : new PartitionWithClusteringColumns(dk, comparator);
        }

        public UnfilteredRowIterator toRowIterator(TableMetadata metadata,
                                                   ClusteringIndexFilter clusteringIndexFilter,
                                                   ColumnFilter columnFilter,
                                                   long now)
        {
            RegularAndStaticColumns columns = columnFilter.queriedColumns();
            Iterator<Unfiltered> rows = getRows(clusteringIndexFilter, columns, now);
            return newUnfilteredRowIterator(metadata, dk, rows, columns);
        }

        /**
         * Creates a {@code UnfilteredRowIterator} for the specified rows.
         *
         * @param partitionKey the metric partition key
         * @param rows the rows
         * @param columns the queried columns
         * @return a {@code UnfilteredRowIterator}
         */
        private UnfilteredRowIterator newUnfilteredRowIterator(TableMetadata metadata,
                                                               DecoratedKey partitionKey,
                                                               Iterator<Unfiltered> rows,
                                                               RegularAndStaticColumns columns)
        {
            return new AbstractUnfilteredRowIterator(metadata, partitionKey,
                                                     DeletionTime.LIVE,
                                                     columns,
                                                     Rows.EMPTY_STATIC_ROW,
                                                     false,
                                                     EncodingStats.NO_STATS)
            {
                @Override
                protected Unfiltered computeNext()
                {
                    if (!rows.hasNext())
                        return endOfData();

                    return rows.next();
                }
            };
         }

        /**
         * Retrieves the partition rows.
         *
         * @param clusteringIndexFilter the clustering filter
         * @param columns the queried columns
         * @param now the metric retrieval timestamp
         * @return the rows
         */
        protected abstract Iterator<Unfiltered> getRows(ClusteringIndexFilter clusteringIndexFilter,
                                                        RegularAndStaticColumns columns,
                                                        long now);

        /**
         * Adds the specified row to this partition overriding any existing one.
         * @param row the row to add
         */
        public abstract void addRow(Row row);

        /**
         * Removes the specified row from tthis partition.
         * @param clustering the clustering key from the row to remove
         */
        public abstract Row removeRow(Clustering clustering);
    }

    /**
     * A {@code Partition} for a view without clustering columns.
     */
    private static final class PartitionWithClusteringColumns extends Partition
    {
        /**
         * The rows contained within this partition.
         */
        private final NavigableMap<Clustering, Row> rows;

        public PartitionWithClusteringColumns(DecoratedKey dk, ClusteringComparator comparator)
        {
            super(dk);
            this.rows = new ConcurrentSkipListMap<>(comparator);
        }

        @Override
        public void addRow(Row row)
        {
            rows.put(row.clustering(), row);
        }

        @Override
        public Row removeRow(Clustering clustering)
        {
            return rows.remove(clustering);
        }

        /**
         * Retrieves the partition rows.
         *
         * @param clusteringIndexFilter the clustering filter
         * @param columns the queried columns
         * @param now the metric retrieval timestamp
         * @return the rows
         */
        protected Iterator<Unfiltered> getRows(ClusteringIndexFilter clusteringIndexFilter,
                                               RegularAndStaticColumns columns,
                                               long now)
        {
            NavigableMap<Clustering, Row> partitionRows = clusteringIndexFilter.isReversed() ? rows.descendingMap()
                                                                                             : rows;

            Iterator<Entry<Clustering, Row>> rowIterator = partitionRows.entrySet().iterator();

            return new AbstractIterator<Unfiltered>()
            {
                @Override
                protected Unfiltered computeNext()
                {
                    while (rowIterator.hasNext())
                    {
                        Entry<Clustering, Row> next = rowIterator.next();
                        Clustering clustering = next.getKey();

                        if (clusteringIndexFilter.selects(clustering))
                            return next.getValue().toTableRow(columns, now);
                    }
                    return endOfData();
                }
            };
        }

        @Override
        public void apply(PartitionUpdate update)
        {
            for (org.apache.cassandra.db.rows.Row r : update)
            {
                Row row = rows.get(r.clustering());
                checkNotNull(row, "INSERT are not supported on system views");
                row.update(update.partitionKey(), r.cells());
            }
        }
    }

    /**
     * A {@code Partition} for a view without clustering columns.
     */
    private static final class PartitionWithoutClusteringColumns extends Partition
    {
        /**
         * The row contained within this partition.
         */
        private volatile Row row;

        public PartitionWithoutClusteringColumns(DecoratedKey dk)
        {
            super(dk);
        }

        @Override
        public void addRow(Row row)
        {
            this.row = row;
        }

        @Override
        public Row removeRow(Clustering clustering)
        {
            assert clustering == Clustering.EMPTY;
            Row old = row;
            row = null;
            return old;
        }

        /**
         * Retrieves the partition rows.
         *
         * @param clusteringIndexFilter the clustering filter
         * @param columns the queried columns
         * @param now the metric retrieval timestamp
         * @return the rows
         */
        protected Iterator<Unfiltered> getRows(ClusteringIndexFilter clusteringIndexFilter,
                                               RegularAndStaticColumns columns,
                                               long now)
        {
            return Iterators.singletonIterator(row.toTableRow(columns, now));
        }

        @Override
        public void apply(PartitionUpdate update)
        {
            row.update(update.partitionKey(), update.getRow(Clustering.EMPTY).cells());
        }
    }

    /**
     * Represents a row of the system view.
     */
    private class Row
    {
        /**
         * The row clustering key.
         */
        private final Clustering clustering;

        /**
         * The row columns.
         */
        private final Map<ColumnMetadata, DataWrapper> rowColumns;

        public Row(Clustering clustering, DataWrapper... wrappers)
        {
            this.clustering = clustering;

            ImmutableMap.Builder<ColumnMetadata, DataWrapper> builder = ImmutableMap.builder(); 
            for (DataWrapper wrapper : wrappers)
                builder.put(wrapper.column(), wrapper);
            this.rowColumns = builder.build();
        }

        /**
         * Updates this row
         * @param cells the new cell values
         */
        public void update(DecoratedKey pk, Iterable<Cell> cells)
        {
            for (Cell cell : cells)
            {
                DataWrapper wrapper = rowColumns.get(cell.column());
                if (wrapper == null)
                    throw invalidRequest("Insert operations are not supported on column %s of system view=%s.%s, partition key=%s %s",
                                         cell.column(),
                                         metadata().keyspace,
                                         metadata().name,
                                         metadata.partitionKeyType.getString(pk.getKey()),
                                         clustering == Clustering.EMPTY ? "" : ", row=" + clustering.toString(metadata()));

                if (!wrapper.isMutable())
                    throw invalidRequest("Modifications are not supported on column %s of system view=%s.%s, partition key=%s %s",
                                         cell.column(),
                                         metadata().keyspace,
                                         metadata().name,
                                         metadata.partitionKeyType.getString(pk.getKey()),
                                         clustering == Clustering.EMPTY ? "" : ", row=" + clustering.toString(metadata()));
            }

            for (Cell cell : cells)
            {
                DataWrapper wrapper = rowColumns.get(cell.column());
                wrapper.setCell(cell);
            }
        }

        /**
         * Returns the row clustering key.
         *
         * @return the row clustering key.
         */
        public Clustering clustering()
        {
            return clustering;
        }

        /**
         * Converts this {@code Row} into a {@code org.apache.cassandra.db.rows.Row}.
         *
         * @param columns the queried columns
         * @param now the current timestamp
         * @return a {@code Row}
         */
        public org.apache.cassandra.db.rows.Row toTableRow(RegularAndStaticColumns columns, long now)
        {
            org.apache.cassandra.db.rows.Row.Builder builder = BTreeRow.unsortedBuilder(FBUtilities.nowInSeconds());
            builder.newRow(clustering);

            if (columns.isEmpty())
                return builder.build();

            for (Iterator<ColumnMetadata> iter = columns.selectOrderIterator(); iter.hasNext();)
            {
                ColumnMetadata column = iter.next();
                DataWrapper wrapper = this.rowColumns.get(column);
                if (wrapper != null)
                    builder.addCell(wrapper.getCell(now));
            }

            return builder.build();
        }
    }
}
