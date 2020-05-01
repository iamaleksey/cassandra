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

package org.apache.cassandra.distributed.upgrade;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.Versions;

public class MixedModeReadRepairTest extends UpgradeTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(MixedModeReadRepairTest.class);
    @Test
    public void mixedModeReadRepairCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup((cluster) -> cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk ascii, b boolean, v blob, PRIMARY KEY (pk)) WITH COMPACT STORAGE"))
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node != 1)
                return;
            // now node1 is 3.0 and node2 is 2.2
            // make sure 2.2 side does not get the mutation
            cluster.get(1).executeInternal("DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                                                          "something");
            // trigger a read repair
            cluster.coordinator(2).execute("SELECT * FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                           ConsistencyLevel.ALL,
                                           "something");
            cluster.get(2).flush(DistributedTestBase.KEYSPACE);
        })
        .runAfterClusterUpgrade((cluster) -> cluster.get(2).forceCompact(DistributedTestBase.KEYSPACE, "tbl"))
        .run();
    }

    final String schema = "CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (" +
                          "    pk uuid," +
                          "    c1 bigint," +
                          "    c2 bigint," +
                          "    v map<text, text>," +
                          "    PRIMARY KEY(pk, c1, c2) " +
                          ") WITH CLUSTERING ORDER BY (c1 ASC, c2 ASC)";

    @Test
    public void mixedModeReadRepairDuplicateRows() throws Throwable
    {
        final String[] workload = new String[] {
        "DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl USING TIMESTAMP 1588349293777111 WHERE pk=28c66b85-ce0a-52d1-28c6-6b85ce0a52d1 AND c1=3785891058 AND c2=3575236614",
        "DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl USING TIMESTAMP 1588349293777111 WHERE pk=28c66b85-ce0a-52d1-28c6-6b85ce0a52d1 AND c1=2381384286 AND c2=1140446813",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 650802753, 2907058448,{'ruUwyljSAJalctANPq':'ETxSayjgitjgcBRq'}) USING TIMESTAMP 1588349293777111",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 4179467075, 3755849774,{'OZfWEdeOJLOVABwX':'DgRnNURBveuKKurGhp', 'ItSkSNeOfLTLukzs':'eQrPEdtxpmWmEAwKrG'}) USING TIMESTAMP 1588349293777111",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 1001777686, 559562217,{'lqdYxSylDgJksDXHHX':'CBPTDWOhukAgPPDW'}) USING TIMESTAMP 1588349293777111",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 3785891058, 3575236614,{'HHILJsKsXELoVkKs':'huYfILCUkTpLJqJkal'}) USING TIMESTAMP 1588349293777211",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 2381384286, 1140446813,{'VZbnFZbQVlTiyZme':'ylPTLojStxjBMQAdvm', 'vQtxxJukAgDgXhSk':'fGfWZHUwHtgBhpvm', 'TGTARtKuuKaYUwkGFY':'hVLilOygizXgfGOVDW'}) USING TIMESTAMP 1588349293777211",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 650802753, 2907058448,{'xbApXEuAatHoAgRtme':'HXoxPTPPSNralQDx', 'raBXEAiHilkGVlme':'EZDVowJTlOPPiaPTxi'}) USING TIMESTAMP 1588349293777211;",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 4179467075, 3755849774,{'HHDVOZVZveLoVpmPcB':'alPMeteOarUwLPxSEd', 'xGJkpqFhukgBIWcB':'CBwrPnTnKuANxaEd', 'ErAglZbwCmNUFhMxVC':'JsOhWWmejSWmgDuo'}) USING TIMESTAMP 1588349293777211",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 1001777686, 559562217,{'jSiaxQFZuAjpILatJc':'wxEdJuJkCBOZzsILVl', 'uKkTErlFPnjXJqRqOO':'wrPuoqoalQAgitJo', 'EZvvctaFJcsDebOO':'wXxbwxTAABETWlJkIL'}) USING TIMESTAMP 1588349293777211",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2,v) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 1001777686, 559562217,{}) USING TIMESTAMP 1588349293777910",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 3785891058, 3575236614) USING TIMESTAMP 1588349293777910",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 2381384286, 1140446813) USING TIMESTAMP 1588349293777910",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 650802753, 2907058448) USING TIMESTAMP 1588349293777910",
        "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk,c1,c2) VALUES (28c66b85-ce0a-52d1-28c6-6b85ce0a52d1, 4179467075, 3755849774) USING TIMESTAMP 1588349293777910"
        };

        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup((cluster) -> cluster.schemaChange(schema))
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node == 1)
            {
                System.out.println("IN MIXED MODE");
                // now node1 is 3.0 and node2 is 2.2
                for (int i=0; i<10; i++ )
                    cluster.coordinator(2).execute(workload[i], ConsistencyLevel.QUORUM);
                cluster.get(2).flush(KEYSPACE);
                validate(cluster, 2, false);
                for (int i=10; i<15; i++ )
                    cluster.coordinator(2).execute(workload[i], ConsistencyLevel.QUORUM);
                validate(cluster, 1, true);
            }
        })
        .run();
    }

    private void validate(UpgradeableCluster cluster, int nodeid, boolean local)
    {
        String query = "SELECT * FROM " + KEYSPACE + ".tbl";

        Iterator<Object[]> iter = local
                                  ? toIter(cluster.get(nodeid).executeInternal(query))
                                  : cluster.coordinator(nodeid).executeWithPaging(query, ConsistencyLevel.ALL, 3);

        Object[] lastCd = new Object[]{ 0L, 0L };
        long rowCount = 0;
        Object[] lastRow = null;
        while (iter.hasNext())
        {
            Object[] row = iter.next();
            Object[] clustering = new Object[]{ row[1], row[2] };
            if (Arrays.equals(lastCd, clustering))
            {
                logger.error(String.format("XXX Duplicate clustering on the node %d in %s mode: \n%s\n%s",
                                                 nodeid,
                                                 local ? "local" : "distributed",
                                                 Arrays.toString(lastRow),
                                                 Arrays.toString(row)));
            }
            System.out.println(String.format("XXX %s", Arrays.toString(row)));
            lastCd = clustering;
            rowCount++;
            lastRow = row;
        }
        System.out.println("THE END " + rowCount);
    }

    private static Iterator<Object[]> toIter(Object[][] objects)
    {
        return new Iterator<Object[]>() {
            int i = 0;
            @Override
            public boolean hasNext() {
                return i < objects.length;
            }

            @Override
            public Object[] next() {
                return objects[i++];
            }
        };
    }

}
