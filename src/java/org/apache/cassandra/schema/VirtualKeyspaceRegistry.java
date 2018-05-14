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
package org.apache.cassandra.schema;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualTable;

final class VirtualKeyspaceRegistry
{
    private final Map<String, VirtualKeyspace> virtualKeyspaces = new ConcurrentHashMap<>();
    private final Map<TableId, VirtualTable>      virtualTables = new ConcurrentHashMap<>();

    void register(VirtualKeyspace keyspace)
    {
        virtualKeyspaces.put(keyspace.name(), keyspace);
        keyspace.tables().forEach(t -> virtualTables.put(t.metadata().id, t));
    }

    @Nullable
    VirtualKeyspace getKeyspaceNullable(String name)
    {
        return virtualKeyspaces.get(name);
    }

    @Nullable
    VirtualTable getTableNullable(TableId id)
    {
        return virtualTables.get(id);
    }

    @Nullable
    KeyspaceMetadata getKeyspaceMetadataNullable(String name)
    {
        VirtualKeyspace keyspace = virtualKeyspaces.get(name);
        return null != keyspace ? keyspace.metadata() : null;
    }

    @Nullable
    TableMetadata getTableMetadataNullable(TableId id)
    {
        VirtualTable table = virtualTables.get(id);
        return null != table ? table.metadata() : null;
    }

    Iterable<KeyspaceMetadata> virtualKeyspacesMetadata()
    {
        return Iterables.transform(virtualKeyspaces.values(), VirtualKeyspace::metadata);
    }
}
