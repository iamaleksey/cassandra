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

package org.apache.cassandra.service.tracking;

import java.io.IOException;
import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.transport.Dispatcher;

import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.net.Verb.WRITE_FORWARDING;

/**
 *
 */
public class WriteForwarding
{
    private static final Logger logger = LoggerFactory.getLogger(WriteForwarding.class);

    final Mutation mutation;
    final ImmutableSet<InetAddressAndPort> replicas;
    final InetAddressAndPort replicaCoordinator;
    final InetAddressAndPort clientCoordinator;

    public WriteForwarding(Mutation mutation, ImmutableSet<InetAddressAndPort> replicas, InetAddressAndPort replicaCoordinator, InetAddressAndPort clientCoordinator)
    {
        this.mutation = mutation;
        this.replicas = replicas;
        this.replicaCoordinator = replicaCoordinator;
        this.clientCoordinator = clientCoordinator;
    }

    public WriteForwarding(Mutation mutation, ReplicaPlan.ForWrite plan, InetAddressAndPort replicaCoordinator, InetAddressAndPort clientCoordinator)
    {
        this.mutation = mutation;
        ImmutableSet.Builder<InetAddressAndPort> replicasBuilder = ImmutableSet.builder();
        plan.liveAndDown().forEach(endpoint -> replicasBuilder.add(endpoint.endpoint()));
        this.replicas = replicasBuilder.build();
        this.replicaCoordinator = replicaCoordinator;
        this.clientCoordinator = clientCoordinator;
    }

    public void start(Dispatcher.RequestTime requestTime, RequestCallback<IMutation> responseHandler)
    {
        Message<WriteForwarding> message = Message.outWithFlags(WRITE_FORWARDING, this, requestTime, Collections.emptyList());

        // Replicas will respond directly to the client-coordinator
        for (InetAddressAndPort to : replicas)
            MessagingService.instance().callbacks.addWithExpiration(responseHandler, message, to);

        // Replica-coordinator does not reply to client-coordinator
        // Would be worth adding a separate callback to handle replica-coordinator failures before timeout
        MessagingService.instance().send(message, replicaCoordinator);
    }

    public static final IVersionedSerializer<WriteForwarding> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(WriteForwarding forwarding, DataOutputPlus out, int version) throws IOException
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            Version vers = Version.minCommonSerializationVersion();

            Mutation.serializer.serialize(forwarding.mutation, out, version);
            out.writeInt(forwarding.replicas.size());
            for (InetAddressAndPort replica : forwarding.replicas)
            {
                NodeId node = metadata.directory.peerId(replica);
                NodeId.serializer.serialize(node, out, vers);
            }
            NodeId replicaCoordinator = metadata.directory.peerId(forwarding.replicaCoordinator);
            NodeId.serializer.serialize(replicaCoordinator, out, vers);
            NodeId clientCoordinator = metadata.directory.peerId(forwarding.clientCoordinator);
            NodeId.serializer.serialize(clientCoordinator, out, vers);
        }

        @Override
        public WriteForwarding deserialize(DataInputPlus in, int version) throws IOException
        {
            ClusterMetadata metadata = ClusterMetadata.current();
            Version vers = Version.minCommonSerializationVersion();

            Mutation mutation = Mutation.serializer.deserialize(in, version);
            int numReplicas = in.readInt();
            ImmutableSet.Builder<InetAddressAndPort> replicasBuilder = ImmutableSet.builderWithExpectedSize(numReplicas);
            for (int i = 0; i < numReplicas; i++)
            {
                NodeId node = NodeId.serializer.deserialize(in, vers);
                replicasBuilder.add(metadata.directory.endpoint(node));
            }
            NodeId replicaCoordinator = NodeId.serializer.deserialize(in, vers);
            NodeId clientCoordinator = NodeId.serializer.deserialize(in, vers);

            return new WriteForwarding(mutation, replicasBuilder.build(), metadata.directory.endpoint(replicaCoordinator), metadata.directory.endpoint(clientCoordinator));
        }

        @Override
        public long serializedSize(WriteForwarding forwarding, int version)
        {
            long size = 0;
            ClusterMetadata metadata = ClusterMetadata.current();
            Version vers = Version.minCommonSerializationVersion();

            size += Mutation.serializer.serializedSize(forwarding.mutation, version);
            size += TypeSizes.INT_SIZE;
            for (InetAddressAndPort replica : forwarding.replicas)
            {
                NodeId node = metadata.directory.peerId(replica);
                size += NodeId.serializer.serializedSize(node, vers);
            }
            NodeId replicaCoordinator = metadata.directory.peerId(forwarding.replicaCoordinator);
            size += NodeId.serializer.serializedSize(replicaCoordinator, vers);
            NodeId clientCoordinator = metadata.directory.peerId(forwarding.clientCoordinator);
            size += NodeId.serializer.serializedSize(clientCoordinator, vers);
            return size;
        }
    };

    public static final VerbHandler verbHandler = new VerbHandler();

    public static class VerbHandler implements IVerbHandler<WriteForwarding>
    {
        private static final Logger logger = LoggerFactory.getLogger(VerbHandler.class);

        @Override
        public void doVerb(Message<WriteForwarding> incoming) throws IOException
        {
            // TODO: Check that we're still a replica for this mutation, might not be if token assignments changed,
            // would need to catch up epoch, see AbstractMutationVerbHandler.checkTokenOwnership

            WriteForwarding forwarding = incoming.payload;
            assert incoming.from().equals(forwarding.clientCoordinator);
            logger.debug("Replica-coordinator received incoming forwarding request from {} (clientCoordinator {}) for mutation with key {}", incoming.from(), forwarding.clientCoordinator, forwarding.mutation.key());

            Shard shard = Shards.instance.lookUp(forwarding.mutation.getKeyspaceName(), forwarding.mutation.key().getToken());

            assert forwarding.mutation.id().isNone();
            MutationId id = shard.nextId();
            logger.debug("Assigned ID {} for mutation with key {}", id, forwarding.mutation.key());
            Mutation assignedId = forwarding.mutation.withMutationId(id);

            // TODO: Add expiration
            // TODO: Ensure local error responses go to Param.clientCoordinator
            Message<Mutation> message = Message.builder(MUTATION_REQ, assignedId)
                                            .withId(incoming.id())
                                            .withParam(ParamType.WRITE_FORWARDING, new Param(forwarding.clientCoordinator))
                                            .build();

            Acknowledge acknowledge = new Acknowledge(id, shard);

            for (InetAddressAndPort replica : forwarding.replicas)
            {
                logger.debug("Replica-coordinator forwarding mutation with ID {} to replica {}", id, replica);
                // Even though this sends a mutation, don't use sendWriteWithCallback
                MessagingService.instance().sendWithCallback(message, replica, acknowledge);
            }
        }
    };

    public static class Acknowledge implements RequestCallbackWithFailure<NoPayload>
    {
        MutationId id;
        Shard shard;

        public Acknowledge(MutationId id, Shard shard)
        {
            this.id = id;
            this.shard = shard;
        }

        @Override
        public void onResponse(Message<NoPayload> response)
        {
            logger.debug("Replica-coordinator got response from replica {} for mutation with id {}", response.from(), id);
            ClusterMetadata metadata = ClusterMetadata.current();
            NodeId peerId = metadata.directory.peerId(response.from());
            shard.witnessedMutationRemote(id, peerId.id());
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            logger.debug("Got failure from {} with reason {}", from, failureReason);
        }
    }

    /**
     *
     */
    public static class Param
    {
        public InetAddressAndPort clientCoordinator;

        public Param(InetAddressAndPort clientCoordinator)
        {
            this.clientCoordinator = clientCoordinator;
        }

        public static final IVersionedSerializer<Param> serializer = new IVersionedSerializer<Param>()
        {
            @Override
            public void serialize(Param param, DataOutputPlus out, int version) throws IOException
            {
                ClusterMetadata metadata = ClusterMetadata.current();
                Version vers = Version.minCommonSerializationVersion();

                NodeId coordinatorId = metadata.directory.peerId(param.clientCoordinator);
                NodeId.serializer.serialize(coordinatorId, out, vers);
            }

            @Override
            public Param deserialize(DataInputPlus in, int version) throws IOException
            {
                ClusterMetadata metadata = ClusterMetadata.current();
                Version vers = Version.minCommonSerializationVersion();

                NodeId coordinatorId = NodeId.serializer.deserialize(in, vers);
                return new Param(metadata.directory.endpoint(coordinatorId));
            }

            @Override
            public long serializedSize(Param param, int version)
            {
                long size = 0;
                ClusterMetadata metadata = ClusterMetadata.current();
                Version vers = Version.minCommonSerializationVersion();

                NodeId coordinatorId = metadata.directory.peerId(param.clientCoordinator);
                size += NodeId.serializer.serializedSize(coordinatorId, vers);
                return size;
            }
        };
    }

    public static boolean isLogged(IMutation mutation)
    {
        Keyspace keyspace = Keyspace.open(mutation.getKeyspaceName());
        return keyspace.getMetadata().params.replicationType.isLogged();
    }

    public static Replica selectReplicaCoordinator(ReplicaPlan.ForWrite plan)
    {
        // TODO: Choose based on NodeProximity, see ReplicaPlans.findCounterLeaderReplica
        return plan.live().get(0);
    }
}
