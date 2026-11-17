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
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.replication.MutationTrackingShards.ShardKey;
import org.apache.cassandra.replication.MutationTrackingShards.ShardValue;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class MarkShardsSealed implements Transformation
{
    public static final Serializer serializer = new Serializer();

    public final ImmutableMap<String, ImmutableSet<ShardKey>> shards;

    public MarkShardsSealed(Map<String, ? extends Set<ShardKey>> shards)
    {
        ImmutableMap.Builder<String, ImmutableSet<ShardKey>> builder = ImmutableMap.builderWithExpectedSize(shards.size());
        shards.forEach((keyspace, keys) -> builder.put(keyspace, ImmutableSet.copyOf(keys)));
        this.shards = builder.build();
    }

    @Override
    public Kind kind()
    {
        return Kind.MARK_SHARDS_SEALED;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        for (Map.Entry<String, ImmutableSet<ShardKey>> entry : shards.entrySet())
        {
            ImmutableMap<ShardKey, ShardValue> current = prev.mutationTrackingShards.keyspaces.get(entry.getKey());
            if (current == null)
                continue;

            for (ShardKey key : entry.getValue())
            {
                ShardValue value = current.get(key);
                if (value != null && value.state == ShardValue.State.ACTIVE)
                    return new Rejected(INVALID, String.format("Can not mark shard %s of keyspace %s SEALED as it is still ACTIVE", key, entry.getKey()));
            }
        }

        ClusterMetadata.Transformer next =
            prev.transformer()
                .with(prev.mutationTrackingShards.withSealed(shards, prev.nextEpoch()));

        return Transformation.success(next, LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public String toString()
    {
        return "MarkShardsSealed{shards=" + shards + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof MarkShardsSealed)) return false;
        MarkShardsSealed that = (MarkShardsSealed) o;
        return shards.equals(that.shards);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(shards);
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, MarkShardsSealed>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            MarkShardsSealed transformation = (MarkShardsSealed) t;
            out.writeUnsignedVInt32(transformation.shards.size());
            for (Map.Entry<String, ImmutableSet<ShardKey>> entry : transformation.shards.entrySet())
            {
                out.writeUTF(entry.getKey());
                out.writeUnsignedVInt32(entry.getValue().size());
                for (ShardKey key : entry.getValue())
                {
                    Token.metadataSerializer.serialize(key.range.left, out, version);
                    Token.metadataSerializer.serialize(key.range.right, out, version);
                    out.writeVInt(key.sinceEpoch);
                }
            }
        }

        @Override
        public MarkShardsSealed deserialize(DataInputPlus in, Version version) throws IOException
        {
            IPartitioner partitioner = ClusterMetadata.current().partitioner;
            int keyspaceCount = in.readUnsignedVInt32();
            ImmutableMap.Builder<String, ImmutableSet<ShardKey>> shards = ImmutableMap.builderWithExpectedSize(keyspaceCount);
            for (int i = 0; i < keyspaceCount; i++)
            {
                String keyspace = in.readUTF();
                int keyCount = in.readUnsignedVInt32();
                ImmutableSet.Builder<ShardKey> keys = ImmutableSet.builderWithExpectedSize(keyCount);
                for (int j = 0; j < keyCount; j++)
                {
                    Range<Token> range = new Range<>(Token.metadataSerializer.deserialize(in, partitioner, version),
                                                     Token.metadataSerializer.deserialize(in, partitioner, version));
                    keys.add(new ShardKey(range, in.readVInt()));
                }
                shards.put(keyspace, keys.build());
            }
            return new MarkShardsSealed(shards.build());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            MarkShardsSealed transformation = (MarkShardsSealed) t;
            long size = TypeSizes.sizeofUnsignedVInt(transformation.shards.size());
            for (Map.Entry<String, ImmutableSet<ShardKey>> entry : transformation.shards.entrySet())
            {
                size += TypeSizes.sizeof(entry.getKey());
                size += TypeSizes.sizeofUnsignedVInt(entry.getValue().size());
                for (ShardKey key : entry.getValue())
                {
                    size += Token.metadataSerializer.serializedSize(key.range.left, version);
                    size += Token.metadataSerializer.serializedSize(key.range.right, version);
                    size += TypeSizes.sizeofVInt(key.sinceEpoch);
                }
            }
            return size;
        }
    }
}
