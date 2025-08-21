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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.AutoRepairService;
import org.apache.cassandra.streaming.PreviewKind;

public class PreviewRepairedState extends AutoRepairState
{
    public PreviewRepairedState()
    {
        super(AutoRepairConfig.RepairType.PREVIEW_REPAIRED);
    }

    @Override
    public RepairCoordinator getRepairRunnable(String keyspace, List<String> tables, Set<Range<Token>> ranges, boolean primaryRangeOnly)
    {
        RepairOption option = new RepairOption(RepairParallelism.PARALLEL, primaryRangeOnly, false, false,
                                               AutoRepairService.instance.getAutoRepairConfig().getRepairThreads(repairType), ranges, false, false, PreviewKind.REPAIRED, false, true, true, false, false, false);

        option.getColumnFamilies().addAll(tables);

        return getRepairRunnable(keyspace, option);
    }
}
