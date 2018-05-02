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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Provides access to system views.
 */
public final class SystemViewManager
{
    /**
     * The managed system views.
     */
    private static final Map<TableId, SystemView> VIEWS = new ConcurrentHashMap<>();

    /**
     * Register the specified system view.
     *
     * @param views the system views to register
     */
    public static void register(SystemView... views)
    {
        for (SystemView view : views)
        {
            VIEWS.put(view.metadata().id, view);
        }
     }

    /**
     * Returns the metadata of the views.
     * @return the metadata of the views.
     */
    public static Set<TableMetadata> getViewsMetadata()
    {
        return VIEWS.values()
                    .stream()
                    .map(v -> v.metadata())
                    .collect(Collectors.toSet());
    }

    /**
     * Checks if the specified table is a registered {@code SystemView}
     * @param tableId the table ID to check
     * @return {@code true} if the table is a registered {@code SystemView}
     */
    public static boolean isRegistered(TableId tableId)
    {
        return VIEWS.containsKey(tableId);
    }

    /**
     * Retrieves the specified view.
     *
     * @param view the system view metadata
     * @return the system view
     */
    public static SystemView get(TableMetadata metadata)
    {
        SystemView systemView = VIEWS.get(metadata.id);

        if (systemView == null)
            throw new IllegalStateException(String.format("Unknown system view %s.%s", metadata.keyspace, metadata.name));

        return systemView;
    }
}
