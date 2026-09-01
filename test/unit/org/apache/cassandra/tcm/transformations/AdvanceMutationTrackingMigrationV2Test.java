/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeVersion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AdvanceMutationTrackingMigrationV2Test
{
    private static IPartitioner partitioner;
    private static TableId table1Id;
    private static TableId table2Id;

    @BeforeClass
    public static void setup() throws Exception
    {
        CassandraRelevantProperties.PARTITIONER.setString(Murmur3Partitioner.class.getName());
        ServerTestUtils.prepareServerNoRegister();
        partitioner = DatabaseDescriptor.getPartitioner();
        assertTrue(partitioner instanceof Murmur3Partitioner);
        table1Id = TableId.generate();
        table2Id = TableId.generate();
    }

    @Test
    public void testAdvanceMultipleTablesAtOnce()
    {
        Epoch startedAt = Epoch.create(1);
        ClusterMetadata prev = migratingMetadata(startedAt, table1Id, table2Id);

        NormalizedRanges<Token> repaired = NormalizedRanges.normalizedRanges(testRanges());
        Transformation.Result result = advance(repaired(repaired, table1Id, table2Id), startedAt).execute(prev);

        assertTrue(result.isSuccess());
        KeyspaceMigrationInfo info = result.success().metadata.mutationTrackingMigrationState.getKeyspaceInfo("test_ks");
        assertNotNull(info);

        NormalizedRanges<Token> expectedPending = KeyspaceMigrationInfo.fullRing().subtract(repaired);
        assertEquals(expectedPending, info.getPendingRangesForTable(table1Id));
        assertEquals(expectedPending, info.getPendingRangesForTable(table2Id));
        assertEquals(startedAt, info.startedAtEpoch);
    }

    @Test
    public void testAdvanceCompletesMigration()
    {
        Epoch startedAt = Epoch.create(1);
        ClusterMetadata prev = migratingMetadata(startedAt, table1Id, table2Id);

        Transformation.Result result = advance(repaired(KeyspaceMigrationInfo.fullRing(), table1Id, table2Id), startedAt).execute(prev);

        assertTrue(result.isSuccess());
        assertFalse(result.success().metadata.mutationTrackingMigrationState.hasMigratingKeyspaces());
    }

    @Test
    public void testAdvanceForNonMigratingKeyspace()
    {
        ClusterMetadata prev = new ClusterMetadata(partitioner).forceEpoch(Epoch.create(1));

        Transformation.Result result =
            advance(repaired(NormalizedRanges.normalizedRanges(testRanges()), table1Id), Epoch.create(1)).execute(prev);

        assertTrue(result.isRejected());
        assertTrue(result.rejected().reason.contains("not migrating"));
    }

    @Test
    public void testAdvanceForTableWithoutPendingRanges()
    {
        Epoch startedAt = Epoch.create(1);
        ClusterMetadata prev = migratingMetadata(startedAt, table1Id);

        Transformation.Result result =
            advance(repaired(NormalizedRanges.normalizedRanges(testRanges()), TableId.generate()), startedAt).execute(prev);

        assertTrue(result.isRejected());
        assertTrue(result.rejected().reason.contains("no pending ranges intersecting"));
    }

    @Test
    public void testSerializationRoundTrip() throws IOException
    {
        AdvanceMutationTrackingMigrationV2 transformation =
            advance(repaired(NormalizedRanges.normalizedRanges(testRanges()), table1Id, table2Id), Epoch.create(42));

        DataOutputBuffer out = new DataOutputBuffer();
        AdvanceMutationTrackingMigrationV2.serializer.serialize(transformation, out, NodeVersion.CURRENT.serializationVersion());
        assertEquals(out.getLength(),
                     AdvanceMutationTrackingMigrationV2.serializer.serializedSize(transformation, NodeVersion.CURRENT.serializationVersion()));

        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        AdvanceMutationTrackingMigrationV2 deserialized =
            AdvanceMutationTrackingMigrationV2.serializer.deserialize(in, NodeVersion.CURRENT.serializationVersion());

        assertEquals(transformation.keyspace, deserialized.keyspace);
        assertEquals(transformation.allRepairedRanges, deserialized.allRepairedRanges);
        assertEquals(transformation.migrationStartedAtEpoch, deserialized.migrationStartedAtEpoch);
    }

    private static AdvanceMutationTrackingMigrationV2 advance(Map<TableId, NormalizedRanges<Token>> repairedRanges, Epoch startedAt)
    {
        return new AdvanceMutationTrackingMigrationV2("test_ks", repairedRanges, startedAt);
    }

    private static Map<TableId, NormalizedRanges<Token>> repaired(NormalizedRanges<Token> ranges, TableId... tableIds)
    {
        Map<TableId, NormalizedRanges<Token>> repaired = new HashMap<>();
        for (TableId tableId : tableIds)
            repaired.put(tableId, ranges);
        return repaired;
    }

    private static ClusterMetadata migratingMetadata(Epoch startedAt, TableId... tableIds)
    {
        MutationTrackingMigrationState state = MutationTrackingMigrationState.EMPTY
            .withKeyspaceMigrating("test_ks", Arrays.asList(tableIds), startedAt);

        return new ClusterMetadata(partitioner).forceEpoch(startedAt).transformer()
                                               .with(state)
                                               .build().metadata;
    }

    private static Collection<Range<Token>> testRanges()
    {
        Token t1 = partitioner.getTokenFactory().fromString("100");
        Token t2 = partitioner.getTokenFactory().fromString("200");
        Token t3 = partitioner.getTokenFactory().fromString("300");

        return Arrays.asList(new Range<>(t1, t2), new Range<>(t2, t3));
    }
}
