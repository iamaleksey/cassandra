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

import java.util.List;

import org.apache.cassandra.db.VirtualTable;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class SystemViewKeyspace
{
    private static final Logger logger = LoggerFactory.getLogger(SystemViewKeyspace.class);

    private SystemViewKeyspace() {}

    public static final String SETTINGS = "settings";
    public static final String TABLE_METRICS = "table_stats";
    public static final String RING_STATE = "ring_state";
    public static final String COMPACTION = "compaction_stats";

    private static final TableMetadata Settings = create(SETTINGS, Settings.class, "Current configration settings");
    private static final TableMetadata RingState = create(RING_STATE, RingState.class,"Current configration settings");
    private static final TableMetadata Compactions = create(COMPACTION, CompactionStats.class, "Compaction State");

    private static final List<TableMetadata> ALL_TABLE_METADATA = ImmutableList.of(Settings, RingState, Compactions);

    private static TableMetadata create(String table, Class<? extends VirtualTable> klass, String comment)
    {
        return VirtualTable.createMetadata(SchemaConstants.SYSTEM_VIEW_NAME, table, klass)
                .comment(comment).build();
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.SYSTEM_VIEW_NAME, KeyspaceParams.local(), Tables.of(ALL_TABLE_METADATA));
    }

}
