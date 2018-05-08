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
package org.apache.cassandra.cql3.validation.entities;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.MapBasedSystemView;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemViewManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.MapBasedSystemView.DataWrapper.wrap;
import static org.apache.cassandra.schema.ColumnMetadata.clusteringColumn;
import static org.apache.cassandra.schema.ColumnMetadata.partitionKeyColumn;
import static org.apache.cassandra.schema.ColumnMetadata.regularColumn;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class SystemViewsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        String sv_1_name = "sv_1";
        ColumnMetadata v_1 = regularColumn(SYSTEM_KEYSPACE_NAME, sv_1_name, "v1", Int32Type.instance);
        ColumnMetadata v_2 = regularColumn(SYSTEM_KEYSPACE_NAME, sv_1_name, "v2", LongType.instance);

        MapBasedSystemView sv_1 = new MapBasedSystemView(sv_1_name,
                                                         "",
                                                         partitionKeyColumn(SYSTEM_KEYSPACE_NAME, sv_1_name, "pk", UTF8Type.instance, 0),
                                                         clusteringColumn(SYSTEM_KEYSPACE_NAME, sv_1_name, "c", UTF8Type.instance, 0),
                                                         v_1,
                                                         v_2);

        sv_1.addRow("pk1", "c1", wrap(v_1, () -> ByteBufferUtil.bytes(11)), wrap(v_2, () -> ByteBufferUtil.bytes(11L)));
        sv_1.addRow("pk2", "c1", wrap(v_1, () -> ByteBufferUtil.bytes(21)), wrap(v_2, () -> ByteBufferUtil.bytes(21L)));
        sv_1.addRow("pk1", "c2", wrap(v_1, () -> ByteBufferUtil.bytes(12)), wrap(v_2, () -> ByteBufferUtil.bytes(12L)));
        sv_1.addRow("pk2", "c2", wrap(v_1, () -> ByteBufferUtil.bytes(22)), wrap(v_2, () -> ByteBufferUtil.bytes(22L)));
        sv_1.addRow("pk1", "c3", wrap(v_1, () -> ByteBufferUtil.bytes(13)), wrap(v_2, () -> ByteBufferUtil.bytes(13L)));
        sv_1.addRow("pk2", "c3", wrap(v_1, () -> ByteBufferUtil.bytes(23)), wrap(v_2, () -> ByteBufferUtil.bytes(23L)));

        SystemViewManager.register(sv_1);

        String sv_2_name = "sv_2";
        v_1 = regularColumn(SYSTEM_KEYSPACE_NAME, sv_2_name, "v_1", Int32Type.instance);
        v_2 = regularColumn(SYSTEM_KEYSPACE_NAME, sv_2_name, "v_2", Int32Type.instance);

        MapBasedSystemView sv_2 = new MapBasedSystemView(sv_2_name,
                                                         "",
                                                         partitionKeyColumn(SYSTEM_KEYSPACE_NAME, sv_1_name, "pk", UTF8Type.instance, 0),
                                                         v_1,
                                                         v_2);

        Map<String, Integer> values = new HashMap<>();
        values.put("pk1", 1);
        values.put("pk2", 2);
        values.put("pk3", 3);

        sv_2.addRow("pk1",
                    wrap(v_1, () -> ByteBufferUtil.bytes(values.get("pk1")), (b) -> values.put("pk1", b.getInt(0))),
                    wrap(v_2, () -> ByteBufferUtil.bytes(1)));
        sv_2.addRow("pk2",
                    wrap(v_1, () -> ByteBufferUtil.bytes(2)),
                    wrap(v_2, () -> ByteBufferUtil.bytes(2)));
        sv_2.addRow("pk3",
                    wrap(v_1, () -> ByteBufferUtil.bytes(values.get("pk3")), (b) -> values.put("pk3", b.getInt(0))),
                    wrap(v_2, () -> ByteBufferUtil.bytes(3)));

        SystemViewManager.register(sv_2);

        CQLTester.setUpClass();
    }

    @Test
    public void testQueries() throws Throwable
    {
        assertRowsNet(executeNet("SELECT * FROM system.sv_1 WHERE pk = 'UNKNOWN'"));

        assertRowsNet(executeNet("SELECT * FROM system.sv_1 WHERE pk = 'pk1' AND c = 'UNKNOWN'"));

        // Test DISTINCT query
        assertRowsNet(executeNet("SELECT DISTINCT pk FROM system.sv_1"),
                      row("pk1"),
                      row("pk2"));

        assertRowsNet(executeNet("SELECT DISTINCT pk FROM system.sv_1 WHERE token(pk) > token('pk1')"),
                      row("pk2"));

        // Test single partition queries
        assertRowsNet(executeNet("SELECT v1, v2 FROM system.sv_1 WHERE pk = 'pk1' AND c = 'c1'"),
                      row(11, 11L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM system.sv_1 WHERE pk = 'pk1' AND c IN ('c1', 'c2')"),
                      row("c1", 11, 11L),
                      row("c2", 12, 12L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM system.sv_1 WHERE pk = 'pk1' AND c IN ('c2', 'c1') ORDER BY c DESC"),
                      row("c2", 12, 12L),
                      row("c1", 11, 11L));

        // Test multi-partition queries
        assertRows(execute("SELECT * FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row("pk1", "c1", 11, 11L),
                   row("pk1", "c2", 12, 12L),
                   row("pk2", "c1", 21, 21L),
                   row("pk2", "c2", 22, 22L));

        assertRows(execute("SELECT pk, c, v1 FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC"),
                   row("pk1", "c2", 12),
                   row("pk2", "c2", 22),
                   row("pk1", "c1", 11),
                   row("pk2", "c1", 21));

        assertRows(execute("SELECT pk, c, v1 FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC LIMIT 1"),
                   row("pk1", "c2", 12));

        assertRows(execute("SELECT c, v1, v2 FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1' , 'c3') ORDER BY c DESC PER PARTITION LIMIT 1"),
                   row("c3", 13, 13L),
                   row("c3", 23, 23L));

        assertRows(execute("SELECT count(*) FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row(4L));

        for (int pageSize = 1; pageSize < 5; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT pk, c, v1, v2 FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c1", 21, 21L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') LIMIT 2", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM system.sv_1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row(4L));
        }

        // Test range queries
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM system.sv_1 WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM system.sv_1 WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM system.sv_1 WHERE token(pk) <= token('pk2') AND c > 'c1' PER PARTITION LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM system.sv_1 WHERE token(pk) = token('pk2') AND c < 'c3' ALLOW FILTERING", pageSize),
                          row(2L));
        }
    }

    @Test
    public void testModifications() throws Throwable
    {
        assertInvalidMessage("INSERT are not supported on system views",
                             "INSERT INTO system.sv_2 (pk, v_1) VALUES ('pk4', 4)");

        assertInvalidMessage("INSERT are not supported on system views",
                             "UPDATE system.sv_2 SET v_1 = 4 WHERE pk ='pk4'");

        assertInvalidMessage("DELETE are not supported on system views",
                             "DELETE FROM system.sv_2 WHERE pk ='pk1'");

        assertRows(execute("SELECT * FROM system.sv_2 WHERE pk = 'pk1'"),
                   row("pk1", 1, 1));

        execute("UPDATE system.sv_2 SET v_1 = 11 WHERE pk ='pk1'");

        assertRows(execute("SELECT * FROM system.sv_2 WHERE pk = 'pk1'"),
                   row("pk1", 11, 1));

        executeNet("UPDATE system.sv_2 SET v_1 = 111 WHERE pk ='pk1'");

        assertInvalidMessage("Modifications are not supported on column v_1 of system view=system.sv_2, partition key=pk2", 
                             "UPDATE system.sv_2 SET v_1 = 22 WHERE pk ='pk2'");

        assertRows(execute("SELECT * FROM system.sv_2 WHERE pk = 'pk2'"),
                   row("pk2", 2, 2));

        executeNet("INSERT INTO system.sv_2 (pk, v_1) VALUES ('pk1', 11)");

        assertRows(execute("SELECT * FROM system.sv_2 WHERE pk = 'pk1'"),
                   row("pk1", 11, 1));

        assertInvalidMessage("Modifications are not supported on column v_2 of system view=system.sv_2, partition key=pk1",
                             "INSERT INTO system.sv_2 (pk, v_1, v_2) VALUES ('pk1', 111, 111)");

        assertRows(execute("SELECT * FROM system.sv_2 WHERE pk = 'pk1'"),
                   row("pk1", 11, 1));

        assertInvalidMessage("TTLs are not supported on System Views",
                             "INSERT INTO system.sv_2 (pk, v_1) VALUES ('pk1', 11) USING TTL 86400");

        assertInvalidMessage("TTLs are not supported on System Views",
                             "UPDATE system.sv_2  USING TTL 86400 SET v_1 = 11 WHERE pk ='pk1'");

        // Test conditional updates internal and external
        assertRows(execute("UPDATE system.sv_2 SET v_1 = 1 WHERE pk ='pk1' IF EXISTS"),
                   row(true));

        assertRows(execute("UPDATE system.sv_2 SET v_1 = 1 WHERE pk ='pk1' IF v_1 = 11"),
                   row(false, 1));

        assertRows(execute("UPDATE system.sv_2 SET v_1 = 11 WHERE pk ='pk1' IF v_1 = 1"),
                   row(true));

        assertRowsNet(executeNet("UPDATE system.sv_2 SET v_1 = 1 WHERE pk ='pk1' IF EXISTS"),
                      row(true));

        assertRowsNet(executeNet("UPDATE system.sv_2 SET v_1 = 1 WHERE pk ='pk1' IF v_1 = 11"),
                      row(false, 1));

        assertRowsNet(executeNet("UPDATE system.sv_2 SET v_1 = 11 WHERE pk ='pk1' IF v_1 = 1"),
                      row(true));

        // Test Batch updates
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE system.sv_2 SET v_1 = 111 WHERE pk ='pk1';" +
                "UPDATE system.sv_2 SET v_1 = 31 WHERE pk ='pk3';" +
                "APPLY BATCH;");

        assertRows(execute("SELECT * FROM system.sv_2 WHERE pk IN ('pk1','pk3')"),
                   row("pk1", 111, 1),
                   row("pk3", 31, 3));

        executeNet("BEGIN UNLOGGED BATCH " +
                   "UPDATE system.sv_2 SET v_1 = 1 WHERE pk ='pk1';" +
                   "UPDATE system.sv_2 SET v_1 = 3 WHERE pk ='pk3';" +
                   "APPLY BATCH;");

        assertRows(execute("SELECT * FROM system.sv_2 WHERE pk IN ('pk1','pk3')"),
                   row("pk1", 1, 1),
                   row("pk3", 3, 3));

        assertRowsNet(executeNet("BEGIN UNLOGGED BATCH " +
                                 "UPDATE system.sv_2 SET v_1 = 11 WHERE pk ='pk1' IF v_1 = 1;" +
                                "APPLY BATCH;"),
                      row(true));

        assertRowsNet(executeNet("BEGIN UNLOGGED BATCH " +
                                 "UPDATE system.sv_2 SET v_1 = 33 WHERE pk ='pk3' IF v_1 = 2;" +
                                 "APPLY BATCH;"),
                      row(false, "pk3", 3));

        createTable("CREATE TABLE %s (pk text PRIMARY KEY, v_1 int, v_2 int)");

        assertInvalidMessage("Cannot include a system view statement in a logged batch",
                             "BEGIN BATCH " +
                             "UPDATE system.sv_2 SET v_1 = 111 WHERE pk ='pk1'" +
                             "UPDATE %s SET v_1 = 1, v_2 = 2 WHERE pk ='pk1'" +
                             "UPDATE system.sv_2 SET v_1 = 31 WHERE pk ='pk3'" +
                             "APPLY BATCH;");
    }

    @Test
    public void testInvalidOperations() throws Throwable
    {
        assertInvalidMessage("Cannot add already existing table \"sv_1\" to keyspace \"system\"",
                             "CREATE TABLE system.sv_1 (pk int PRIMARY KEY, v int)");

        assertInvalidMessage("Cannot use DROP TABLE on system views",
                             "DROP TABLE system.sv_1");

        assertInvalidMessage("Cannot use ALTER TABLE on system views",
                             "ALTER TABLE system.sv_1 DROP count");

        assertInvalidMessage("Cannot use TRUNCATE TABLE on system views",
                             "TRUNCATE TABLE system.sv_1");

        assertInvalidMessage("Secondary indexes are not supported on system views",
                             "CREATE INDEX ON system.sv_1 (count)");

        assertInvalidMessage("Materialized views cannot be created against system views",
                             "CREATE MATERIALIZED VIEW system.mv AS SELECT c, v1 FROM system.sv_1 WHERE c IS NOT NULL PRIMARY KEY(c)");

        assertInvalidMessage("Cannot CREATE TRIGGER against a system view",
                             "CREATE TRIGGER test_trigger ON system.sv_1 USING '" + TestTrigger.class.getName() + "'");
    }

    /**
     * Noop trigger for audit log testing
     */
    public static class TestTrigger implements ITrigger
    {
        @Override
        public Collection<Mutation> augment(Partition update)
        {
            return null;
        }
    }

    @Test
    public void testMBeansMethods() throws Throwable
    {
        StorageServiceMBean mbean = StorageService.instance;

        final String table = "sv_1";

        // Makes sure that forcing compaction on the keyspace works fine with system views.
        mbean.forceKeyspaceCompaction(false, SchemaConstants.SYSTEM_KEYSPACE_NAME);

        // Makes sure that forcing compaction on a system view is rejected with the proper error.
        try
        {
            mbean.forceKeyspaceCompaction(false, SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that scrubing the keyspace ignore the system views.
        mbean.scrub(true, true, true, true, 1, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        // Makes sure that scrub on a system view is rejected with the proper error.
        try
        {
            mbean.scrub(true, true, true, true, 1, SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that verifying the keyspace ignore the system views.
        mbean.verify(true, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        // Makes sure that verifying a system view is rejected with the proper error.
        try
        {
            mbean.verify(true, SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that upgrading the keyspace SSTables ignore the system views.
        mbean.upgradeSSTables(SchemaConstants.SYSTEM_KEYSPACE_NAME, false, 1);
        // Makes sure that upgrading the SSTables of a system view is rejected with the proper error.
        try
        {
            mbean.upgradeSSTables(SchemaConstants.SYSTEM_KEYSPACE_NAME, false, 1, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that garbage collecting tombstone on the keyspace ignore the system views.
        mbean.garbageCollect("ROW", 1, SchemaConstants.SYSTEM_KEYSPACE_NAME);
        // Makes sure that garbage collecting tombstone on a system view is rejected with the proper error.
        try
        {
            mbean.garbageCollect("ROW", 1, SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that flushing the keyspace ignore the system views.
        mbean.forceKeyspaceFlush(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        // Makes sure that flushing a system view is rejected with the proper error.
        try
        {
            mbean.forceKeyspaceFlush(SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that truncating a system view is rejected with the proper error.
        try
        {
            mbean.truncate(SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that loading new SSTables on a system view is rejected with the proper error.
        try
        {
            mbean.loadNewSSTables(SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }

        // Makes sure that getAutoCompactionStatus ignore the system views.
        Map<String, Boolean> status = mbean.getAutoCompactionStatus(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        assertFalse(status.containsKey(table));
        // Makes sure that getAutoCompactionStatus on a system view is rejected with the proper error.
        try
        {
            mbean.getAutoCompactionStatus(SchemaConstants.SYSTEM_KEYSPACE_NAME, table);
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("The operation is not supported on system view (system.sv_1)", e.getMessage());
        }
    }
}
