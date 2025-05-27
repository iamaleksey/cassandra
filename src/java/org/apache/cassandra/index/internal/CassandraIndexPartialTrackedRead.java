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

package org.apache.cassandra.index.internal;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.index.internal.CassandraIndexSearcher.CassandraMatch;
import org.apache.cassandra.service.reads.tracked.AbstractPartialTrackedIndexRead;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

public class CassandraIndexPartialTrackedRead extends AbstractPartialTrackedIndexRead<CassandraMatch, CassandraIndexSearcher>
{
    public CassandraIndexPartialTrackedRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos, ReadCommand command, CassandraIndexSearcher searcher)
    {
        super(executionController, cfs, startTimeNanos, command, searcher);
    }

    @Override
    protected CloseableIterator<CassandraMatch> queryIndex()
    {
        DecoratedKey indexKey = searcher().indexKey();
        RowIterator indexIter = searcher().queryIndex(indexKey, executionController());
        return new AbstractIterator<>()
        {
            @Override
            protected CassandraMatch computeNext()
            {
                if (!indexIter.hasNext())
                    return endOfData();

                Row row = indexIter.next();
                IndexEntry entry = searcher().index.decodeEntry(indexKey, row);

                return new CassandraMatch(entry.indexValue, entry.indexClustering, indexKey, entry.indexedEntryClustering);
            }

            @Override
            public void close()
            {
                indexIter.close();
            }
        };
    }
}
