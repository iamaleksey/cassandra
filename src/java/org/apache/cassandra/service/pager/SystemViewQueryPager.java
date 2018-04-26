package org.apache.cassandra.service.pager;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemView.SystemViewReadCommand;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.pager.PagingState.RowMark;
import org.apache.cassandra.transport.ProtocolVersion;

public class SystemViewQueryPager extends AbstractQueryPager
{
    private volatile DecoratedKey lastReturnedKey;
    private volatile PagingState.RowMark lastReturnedRow;
    private SystemViewReadCommand virtualQuery;
    public PagingState state;

    public SystemViewQueryPager(SystemViewReadCommand command, PagingState state,
            ProtocolVersion protocolVersion)
    {
        super(command, command.nowInSec, command.metadata.enforceStrictLiveness(), protocolVersion);
        this.virtualQuery = command;
        this.state = state;

        if (state != null)
        {
            lastReturnedKey = command.metadata.partitioner.decorateKey(state.partitionKey);
            lastReturnedRow = state.rowMark;
            restoreState(lastReturnedKey, state.remaining, state.remainingInPartition);
        }
    }

    public SystemViewQueryPager(SystemViewReadCommand command,
            ProtocolVersion protocolVersion,
            DecoratedKey lastReturnedKey,
            PagingState.RowMark lastReturnedRow,
            int remaining,
            int remainingInPartition)
    {
        super(command, command.nowInSec, command.metadata.enforceStrictLiveness(), protocolVersion);
        this.virtualQuery = command;
        this.lastReturnedKey = lastReturnedKey;
        this.lastReturnedRow = lastReturnedRow;
        restoreState(lastReturnedKey, remaining, remainingInPartition);
    }

    public SystemViewQueryPager withUpdatedLimit(DataLimits newLimits)
    {
        return new SystemViewQueryPager(virtualQuery.withUpdatedLimits(newLimits),
                protocolVersion,
                lastReturnedKey,
                lastReturnedRow,
                maxRemaining(),
                remainingInPartition());
    }

    public PagingState state()
    {
        return lastReturnedKey == null
                ? null
                : new PagingState(lastReturnedKey.getKey(), lastReturnedRow, maxRemaining(), remainingInPartition());
    }


    protected ReadQuery nextPageReadCommand(int pageSize)
    throws RequestExecutionException
    {
        DataLimits limits;
        if (lastReturnedKey == null)
        {
            limits = command.limits().forPaging(pageSize);
        }
        else
        {
            // We want to include the last returned key only if we haven't achieved our per-partition limit, otherwise, don't bother.
            boolean includeLastKey = remainingInPartition() > 0 && lastReturnedRow != null;
            if (includeLastKey)
            {
                limits = command.limits().forPaging(pageSize, lastReturnedKey.getKey(), remainingInPartition());
            }
            else
            {
                limits = command.limits().forPaging(pageSize);
            }
        }

        return virtualQuery.withUpdatedLimitsAndState(limits, state());
    }

    protected void recordLast(DecoratedKey key, Row last)
    {
        if (last != null)
        {
            lastReturnedKey = key;
            if (last.clustering() != Clustering.STATIC_CLUSTERING)
                lastReturnedRow = PagingState.RowMark.create(virtualQuery.metadata, last, protocolVersion);
        }
    }

    protected boolean isPreviouslyReturnedPartition(DecoratedKey key)
    {
        return key.equals(lastReturnedKey);
    }
}
