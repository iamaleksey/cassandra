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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ForwardedWriteResponseHandler;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

public class ForwardedWriteRequest
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedWriteRequest.class);

    // For now, just supporting a single mutation to multiple recipients. This will develop in the future for different
    // kinds of mutations that each go to different recipients (see PaxosCommit).
    final Verb verb;
    final Mutation mutation;
    final ConsistencyLevel consistencyLevel;
    final Dispatcher.RequestTime requestTime;

    public ForwardedWriteRequest(Verb verb, Mutation mutation, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        this.verb = verb;
        this.mutation = mutation;
        this.consistencyLevel = consistencyLevel;
        this.requestTime = requestTime;
    }

    private Replica getLeader(ReplicaPlan.ForWrite plan)
    {
        // TODO: Should match ReplicaPlans.findCounterLeaderReplica, including DC-local priority
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        EndpointsForToken replicas = plan.live();
        logger.debug("Finding best leader from replicas {}", replicas);
        replicas = proximity.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
        return replicas.get(0);
    }

    public void send(ForwardedWriteResponseHandler handler)
    {
        String keyspaceName = mutation.getKeyspaceName();
        Keyspace keyspace = Keyspace.open(keyspaceName);
        Token token = mutation.key().getToken();
        ReplicaPlan.ForWrite plan = ReplicaPlans.forWrite(keyspace, consistencyLevel, token, ReplicaPlans.writeNormal);

        Replica leader = getLeader(plan);

        // Add callbacks for replicas to respond directly to coordinator
        Message<ForwardedWriteRequest> toLeader = Message.out(Verb.FORWARDING_WRITE, this);
        for (Replica replica : plan.contacts())
        {
            logger.debug("Adding forwarding callback for response from {} id {}", replica.endpoint(), toLeader.id());
            MessagingService.instance().callbacks.addWithExpiration(handler, toLeader, replica.endpoint());
        }

        MessagingService.instance().send(toLeader, leader.endpoint());
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<ForwardedWriteRequest>
    {
        @Override
        public void serialize(ForwardedWriteRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(request.verb.id);
            Mutation.serializer.serialize(request.mutation, out, version);
            out.writeInt(request.consistencyLevel.code);
        }

        @Override
        public ForwardedWriteRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Verb verb = Verb.fromId(in.readInt());
            Mutation mutation = Mutation.serializer.deserialize(in, version);
            ConsistencyLevel consistencyLevel = ConsistencyLevel.fromCode(in.readInt());
            return new ForwardedWriteRequest(verb, mutation, consistencyLevel, Dispatcher.RequestTime.forImmediateExecution());
        }

        @Override
        public long serializedSize(ForwardedWriteRequest request, int version)
        {
            long size = 0;
            size += TypeSizes.INT_SIZE;
            size += Mutation.serializer.serializedSize(request.mutation, version);
            size += TypeSizes.INT_SIZE;
            return size;
        }
    }

    public static final VerbHandler verbHandler = new VerbHandler();

    public static class VerbHandler implements IVerbHandler<ForwardedWriteRequest>
    {
        @Override
        public void doVerb(Message<ForwardedWriteRequest> incoming)
        {
            logger.debug("Received incoming ForwardedWriteRequest {} id {}", incoming, incoming.id());
            Mutation mutation = incoming.payload.mutation;
            assert mutation.id().isNone();
            ConsistencyLevel consistencyLevel = incoming.payload.consistencyLevel;

            Stage.MUTATION.submit(() -> {
                new TrackedWriteRequest(DirectAcknowledge.toCoordinator(incoming.from(), incoming.id()))
                        .perform(mutation, consistencyLevel, Dispatcher.RequestTime.forImmediateExecution());
            });
        }
    }

    public static class DirectAcknowledge
    {
        public static IVersionedSerializer<DirectAcknowledge> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(DirectAcknowledge ackTo, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(ackTo.coordinator, out, version);
                out.writeLong(ackTo.id);
            }

            @Override
            public DirectAcknowledge deserialize(DataInputPlus in, int version) throws IOException
            {
                InetAddressAndPort coordinator = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
                long id = in.readLong();
                return new DirectAcknowledge(coordinator, id);
            }

            @Override
            public long serializedSize(DirectAcknowledge ackTo, int version)
            {
                long size = 0;
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(ackTo.coordinator, version);
                size += TypeSizes.LONG_SIZE;
                return size;
            }
        };

        public final InetAddressAndPort coordinator;
        public final long id;

        public DirectAcknowledge(InetAddressAndPort coordinator, long id)
        {
            this.coordinator = coordinator;
            this.id = id;
        }

        static DirectAcknowledge toCoordinator(InetAddressAndPort coordinator, long messageId)
        {
            return new DirectAcknowledge(coordinator, messageId);
        }
    }
}
