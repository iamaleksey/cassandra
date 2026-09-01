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
package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CollectionSerializers;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.tcm.ClusterMetadata.Transformer;

/**
 * Transformation to mark ranges as migrated for a keyspace.
 *
 * Called by repair coordinator callback to report completed ranges to TCM.
 * Subtracts completed ranges from pendingRangesPerTable and automatically removes
 * keyspace from migration state when all tables are fully repaired (migration complete).
 */
public class AdvanceMutationTrackingMigrationV2 implements Transformation
{
    public static final Serializer serializer = new Serializer();

    @Nonnull
    public final String keyspace;
    @Nonnull
    public final Map<TableId, NormalizedRanges<Token>> allRepairedRanges;
    @Nonnull
    public final Epoch migrationStartedAtEpoch;

    public AdvanceMutationTrackingMigrationV2(String keyspace,
                                              Map<TableId, NormalizedRanges<Token>> allRepairedRanges,
                                              Epoch migrationStartedAtEpoch)
    {
        this.keyspace = keyspace;
        this.allRepairedRanges = allRepairedRanges;
        this.migrationStartedAtEpoch = migrationStartedAtEpoch;
    }

    @Override
    public Kind kind()
    {
        return Kind.ADVANCE_MUTATION_TRACKING_MIGRATION_V2;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        KeyspaceMigrationInfo ksInfo = prev.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);

        if (ksInfo == null)
            return new Rejected(INVALID, String.format("Keyspace %s is not migrating", keyspace));

        // the migration the repairs were validated against is not the migration in progress
        if (!ksInfo.startedAtEpoch.equals(migrationStartedAtEpoch))
        {
            return new Rejected(INVALID, String.format("Keyspace %s migration started at epoch %s, but the repaired ranges " +
                                                       "were validated against the migration started at epoch %s",
                                                       keyspace, ksInfo.startedAtEpoch, migrationStartedAtEpoch));
        }

        Transformer transformer = prev.transformer();

        MutationTrackingMigrationState newState = prev.mutationTrackingMigrationState;

        for (Map.Entry<TableId, NormalizedRanges<Token>> entry : allRepairedRanges.entrySet())
            newState = newState.withRangesRepairedForTable(keyspace, entry.getKey(), entry.getValue(), transformer.epoch());

        if (newState == prev.mutationTrackingMigrationState)
        {
            return new Rejected(INVALID, String.format("Keyspace %s tables have no pending ranges intersecting %s",
                                                       keyspace, allRepairedRanges));
        }

        return Transformation.success(transformer.with(newState), LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public String toString()
    {
        return "AdvanceMutationTrackingMigrationV2{" +
               "keyspace='" + keyspace + '\'' +
               ", allRepairedRanges=" + allRepairedRanges +
               ", migrationStartedAtEpoch=" + migrationStartedAtEpoch +
               '}';
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, AdvanceMutationTrackingMigrationV2>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdvanceMutationTrackingMigrationV2 am = (AdvanceMutationTrackingMigrationV2) t;
            out.writeUTF(am.keyspace);
            CollectionSerializers.serializeMap(am.allRepairedRanges, out, version,
                                               TableId.metadataSerializer,
                                               KeyspaceMigrationInfo.normalizedRangesSerializer);
            Epoch.serializer.serialize(am.migrationStartedAtEpoch, out, version);
        }

        @Override
        public AdvanceMutationTrackingMigrationV2 deserialize(DataInputPlus in, Version version) throws IOException
        {
            String keyspace = in.readUTF();
            Map<TableId, NormalizedRanges<Token>> allRepairedRanges =
                CollectionSerializers.deserializeMap(in, version,
                                                     TableId.metadataSerializer,
                                                     KeyspaceMigrationInfo.normalizedRangesSerializer);
            Epoch migrationStartedAtEpoch = Epoch.serializer.deserialize(in, version);
            return new AdvanceMutationTrackingMigrationV2(keyspace, allRepairedRanges, migrationStartedAtEpoch);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            AdvanceMutationTrackingMigrationV2 am = (AdvanceMutationTrackingMigrationV2) t;
            return TypeSizes.sizeof(am.keyspace)
                   + CollectionSerializers.serializedMapSize(am.allRepairedRanges, version,
                                                             TableId.metadataSerializer,
                                                             KeyspaceMigrationInfo.normalizedRangesSerializer)
                   + Epoch.serializer.serializedSize(am.migrationStartedAtEpoch, version);
        }
    }
}
