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

package org.apache.cassandra.distributed.test;

import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.util.PyDtest;
import org.apache.cassandra.service.StorageService;

public class StreamingTest extends DistributedTestBase
{

    private void testStreaming(int nodes, int replicationFactor, int rowCount) throws Throwable
    {
        try (Cluster cluster = Cluster.create(nodes))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + replicationFactor + "};");
            cluster.schemaChange(PyDtest.createCf(KEYSPACE, "cf")
                                        .withColumns("c1:text", "c2:text")
                                        .withCompactionStrategy("LeveledCompactionStrategy")
                                        .build());

            for (int i = 0 ; i < rowCount ; ++i)
                cluster.coordinator(1).execute(String.format("INSERT INTO %s.cf (key, c1, c2) VALUES (?, 'value1', 'value2');", KEYSPACE), ConsistencyLevel.ALL, Integer.toString(i));

            cluster.get(2).executeInternal("TRUNCATE system.available_ranges;");

            cluster.get(2).runOnInstance(() -> StorageService.instance.rebuild(null, KEYSPACE, null, null));
        }
    }

    @Test
    public void test() throws Throwable
    {
        testStreaming(2, 2, 1000);
    }

}
