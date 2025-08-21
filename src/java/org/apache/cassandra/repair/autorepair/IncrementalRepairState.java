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

package org.apache.cassandra.repair.autorepair;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.view.TableViews;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.streaming.PreviewKind;

public class IncrementalRepairState extends AutoRepairState
{
    public IncrementalRepairState()
    {
        super(AutoRepairConfig.RepairType.INCREMENTAL);
    }

    @Override
    public RepairCoordinator getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly)
    {
        RepairOption option = new RepairOption(RepairParallelism.PARALLEL, primaryRangeOnly, true, false,
                                               AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), ranges,
                                               false, false, PreviewKind.NONE, true, true, true, false, false, false);

        option.getColumnFamilies().addAll(filterOutUnsafeTables(keyspace, tables));

        return getRepairRunnable(keyspace, option);
    }

    @VisibleForTesting
    protected List<String> filterOutUnsafeTables(String keyspaceName, List<String> tables)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);

        return tables.stream()
                     .filter(table -> {
                         ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(table);
                         TableViews views = keyspace.viewManager.forTable(cfs.metadata());
                         if (views != null && !views.isEmpty())
                         {
                             logger.debug("Skipping incremental repair for {}.{} as it has materialized views", keyspaceName, table);
                             return false;
                         }

                         if (cfs.metadata().params != null && cfs.metadata().params.cdc)
                         {
                             logger.debug("Skipping incremental repair for {}.{} as it has CDC enabled", keyspaceName, table);
                             return false;
                         }

                         return true;
                     }).collect(Collectors.toList());
    }
}
