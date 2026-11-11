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
package org.apache.cassandra.replication;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.ReplicaGroups;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.tcm.serialization.PartitionerAwareMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkState;

/**
 * TODO (expected): trim obsoleted shards at some point to keep TCM state manageable
 */
public final class MutationTrackingShards implements MetadataValue<MutationTrackingShards>
{
    public static final MutationTrackingShards EMPTY = new MutationTrackingShards(Epoch.EMPTY, ImmutableMap.of());

    private static final Comparator<ShardKey> KEY_COMPARATOR =
        Comparator.<ShardKey, Token>comparing(k -> k.range.left)
                  .thenComparing(k -> k.range.right)
                  .thenComparingLong(k -> k.sinceEpoch);

    public final Epoch lastModified;
    public final ImmutableMap<String, ImmutableMap<ShardKey, ShardValue>> keyspaces;

    public static final class ShardKey
    {
        public final Range<Token> range;
        public final long sinceEpoch;

        public ShardKey(Range<Token> range, long sinceEpoch)
        {
            this.range = range;
            this.sinceEpoch = sinceEpoch;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof ShardKey)) return false;
            ShardKey that = (ShardKey) o;
            return sinceEpoch == that.sinceEpoch && range.equals(that.range);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(range, sinceEpoch);
        }

        @Override
        public String toString()
        {
            return range + "@" + sinceEpoch;
        }
    }

    public static final class ShardValue
    {
        public final Participants participants;

        ShardValue(Participants participants)
        {
            this.participants = participants;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof ShardValue)) return false;
            ShardValue that = (ShardValue) o;
            return participants.equals(that.participants);
        }

        @Override
        public int hashCode()
        {
            return participants.hashCode();
        }

        @Override
        public String toString()
        {
            return "participants=" + participants;
        }
    }

    public MutationTrackingShards(Epoch lastModified, ImmutableMap<String, ImmutableMap<ShardKey, ShardValue>> keyspaces)
    {
        this.lastModified = lastModified;
        this.keyspaces = keyspaces;
    }

    @Override
    public MutationTrackingShards withLastModified(Epoch epoch)
    {
        return new MutationTrackingShards(epoch, keyspaces);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    public MutationTrackingShards withKeyspace(String keyspace, ReplicaGroups writes, Directory directory, Epoch epoch)
    {
        if (keyspaces.containsKey(keyspace))
            throw new AssertionError("MutationTrackingShards already contains shards for keyspace " + keyspace);

        TreeMap<ShardKey, ShardValue> shards = new TreeMap<>(KEY_COMPARATOR);
        for (int i = 0; i < writes.ranges.size(); i++)
            shards.put(new ShardKey(writes.ranges.get(i), epoch.getEpoch()), new ShardValue(participants(keyspace, writes.endpoints.get(i), directory)));
        TreeMap<String, ImmutableMap<ShardKey, ShardValue>> updated = new TreeMap<>(keyspaces);
        updated.put(keyspace, ImmutableSortedMap.copyOfSorted(shards));
        return new MutationTrackingShards(lastModified, ImmutableMap.copyOf(updated));
    }

    public MutationTrackingShards withoutKeyspace(String keyspace)
    {
        if (!keyspaces.containsKey(keyspace))
            throw new AssertionError("MutationTrackingShards doesn't contain shards for keyspace " + keyspace);

        TreeMap<String, ImmutableMap<ShardKey, ShardValue>> updated = new TreeMap<>(keyspaces);
        updated.remove(keyspace);
        return new MutationTrackingShards(lastModified, ImmutableMap.copyOf(updated));
    }

    private static Participants participants(String keyspace, VersionedEndpoints.ForRange endpoints, Directory directory)
    {
        int[] hosts = new int[endpoints.size()];
        int i = 0;
        for (InetAddressAndPort endpoint : endpoints.endpoints())
        {
            NodeId id = directory.peerId(endpoint);
            checkState(id != null, "Write replica %s of keyspace %s is missing from the directory", endpoint, keyspace);
            hosts[i++] = id.id();
        }
        return new Participants(hosts);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MutationTrackingShards)) return false;
        MutationTrackingShards that = (MutationTrackingShards) o;
        return lastModified.equals(that.lastModified) && keyspaces.equals(that.keyspaces);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, keyspaces);
    }

    @Override
    public String toString()
    {
        return "MutationTrackingShards{" +
               "lastModified=" + lastModified +
               ", keyspaces=" + keyspaces +
               '}';
    }

    public static final PartitionerAwareMetadataSerializer<MutationTrackingShards> serializer = new PartitionerAwareMetadataSerializer<>()
    {
        @Override
        public void serialize(MutationTrackingShards t, DataOutputPlus out, IPartitioner partitioner, Version version) throws IOException
        {
            Epoch.serializer.serialize(t.lastModified, out, version);
            out.writeUnsignedVInt32(t.keyspaces.size());
            for (Map.Entry<String, ImmutableMap<ShardKey, ShardValue>> entry : new TreeMap<>(t.keyspaces).entrySet())
            {
                out.writeUTF(entry.getKey());
                out.writeUnsignedVInt32(entry.getValue().size());
                for (Map.Entry<ShardKey, ShardValue> shard : entry.getValue().entrySet())
                {
                    Token.metadataSerializer.serialize(shard.getKey().range.left, out, partitioner, version);
                    Token.metadataSerializer.serialize(shard.getKey().range.right, out, partitioner, version);
                    out.writeVInt(shard.getKey().sinceEpoch);
                    Participants.serializer.serialize(shard.getValue().participants, out);
                }
            }
        }

        @Override
        public MutationTrackingShards deserialize(DataInputPlus in, IPartitioner partitioner, Version version) throws IOException
        {
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            int keyspaceCount = in.readUnsignedVInt32();
            ImmutableMap.Builder<String, ImmutableMap<ShardKey, ShardValue>> keyspaces = ImmutableMap.builderWithExpectedSize(keyspaceCount);
            for (int i = 0; i < keyspaceCount; i++)
            {
                String keyspace = in.readUTF();
                int shardCount = in.readUnsignedVInt32();
                ImmutableSortedMap.Builder<ShardKey, ShardValue> shards = new ImmutableSortedMap.Builder<>(KEY_COMPARATOR);
                for (int j = 0; j < shardCount; j++)
                {
                    Range<Token> range = new Range<>(Token.metadataSerializer.deserialize(in, partitioner, version),
                                                     Token.metadataSerializer.deserialize(in, partitioner, version));
                    long sinceEpoch = in.readVInt();
                    Participants participants = Participants.serializer.deserialize(in);
                    shards.put(new ShardKey(range, sinceEpoch), new ShardValue(participants));
                }
                keyspaces.put(keyspace, shards.build());
            }
            return new MutationTrackingShards(lastModified, keyspaces.build());
        }

        @Override
        public long serializedSize(MutationTrackingShards t, IPartitioner partitioner, Version version)
        {
            long size = Epoch.serializer.serializedSize(t.lastModified, version);
            size += TypeSizes.sizeofUnsignedVInt(t.keyspaces.size());
            for (Map.Entry<String, ImmutableMap<ShardKey, ShardValue>> entry : t.keyspaces.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey());
                size += TypeSizes.sizeofUnsignedVInt(entry.getValue().size());
                for (Map.Entry<ShardKey, ShardValue> shard : entry.getValue().entrySet())
                {
                    size += Token.metadataSerializer.serializedSize(shard.getKey().range.left, partitioner, version);
                    size += Token.metadataSerializer.serializedSize(shard.getKey().range.right, partitioner, version);
                    size += TypeSizes.sizeofVInt(shard.getKey().sinceEpoch);
                    size += Participants.serializer.serializedSize(shard.getValue().participants);
                }
            }
            return size;
        }
    };
}
