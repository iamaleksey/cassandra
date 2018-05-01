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
package org.apache.cassandra.service.pager;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemView.SystemViewReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.transport.ProtocolVersion;

public class SystemViewQueryPager extends AbstractQueryPager<SystemViewReadCommand>
{
    private volatile DecoratedKey lastReturnedKey;
    private volatile PagingState.RowMark lastReturnedRow;
    private final SystemViewReadCommand virtualQuery;
    public PagingState state;

    public SystemViewQueryPager(SystemViewReadCommand command, PagingState state, ProtocolVersion protocolVersion)
    {
        super(command, protocolVersion);
        this.virtualQuery = command;
        this.state = state;

        if (state != null)
        {
            lastReturnedKey = command.metadata.partitioner.decorateKey(state.partitionKey);
            lastReturnedRow = state.rowMark;
            restoreState(lastReturnedKey, state.remaining, state.remainingInPartition);
        }
    }

    private SystemViewQueryPager(SystemViewReadCommand command,
                                 ProtocolVersion protocolVersion,
                                 DecoratedKey lastReturnedKey,
                                 PagingState.RowMark lastReturnedRow,
                                 int remaining,
                                 int remainingInPartition)
    {
        super(command, protocolVersion);
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


    protected SystemViewReadCommand nextPageReadQuery(int pageSize) throws RequestExecutionException
    {
        DataLimits limits;
        if (lastReturnedKey == null)
        {
            limits = virtualQuery.limits().forPaging(pageSize);
        }
        else
        {
            // We want to include the last returned key only if we haven't achieved our per-partition limit, otherwise, don't bother.
            boolean includeLastKey = remainingInPartition() > 0 && lastReturnedRow != null;
            limits = includeLastKey
                   ? virtualQuery.limits().forPaging(pageSize, lastReturnedKey.getKey(), remainingInPartition())
                   : virtualQuery.limits().forPaging(pageSize);
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
