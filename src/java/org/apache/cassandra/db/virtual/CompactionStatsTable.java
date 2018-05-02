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
package org.apache.cassandra.db.virtual;

import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.SystemView;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

final class CompactionStatsTable extends SystemView
{
    private final static String HOST_ID = "host_id";
    private final static String COMPACTION_ID = "compaction_id";
    private final static String TASK_TYPE = "task_type";
    private final static String KEYSPACE_NAME = "keyspace_name";
    private final static String TABLE_NAME = "table_name";
    private final static String BYTES_COMPACTED = "bytes_compacted";
    private final static String BYTES_TOTAL = "bytes_total";

    CompactionStatsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "compaction_stats")
                           .comment("compactions state")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(HOST_ID, UUIDType.instance)
                           .addClusteringColumn(COMPACTION_ID, UUIDType.instance)
                           .addRegularColumn(TASK_TYPE, UTF8Type.instance)
                           .addRegularColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addRegularColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(BYTES_COMPACTED, UTF8Type.instance)
                           .addRegularColumn(BYTES_TOTAL, UTF8Type.instance)
                           .build());
    }

    public void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result)
    {
        UUID hostId = StorageService.instance.getLocalHostUUID();
        for (Map<String, String> c : CompactionManager.instance.getCompactions())
        {
            result.row(hostId, UUID.fromString(c.get(CompactionInfo.COMPACTION_ID)))
                  .column(TASK_TYPE, c.get(CompactionInfo.TASK_TYPE))
                  .column(KEYSPACE_NAME, c.get(CompactionInfo.KEYSPACE))
                  .column(TABLE_NAME, c.get(CompactionInfo.COLUMNFAMILY))
                  .column(BYTES_COMPACTED, c.get(CompactionInfo.COMPLETED))
                  .column(BYTES_TOTAL, c.get(CompactionInfo.TOTAL))
                  .endRow();
        }
    }
}
