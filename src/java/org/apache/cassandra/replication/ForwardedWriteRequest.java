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
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.ForwardedWriteResponseHandler;
import org.apache.cassandra.utils.CollectionSerializer;
import org.apache.cassandra.utils.FBUtilities;

public class ForwardedWriteRequest
{
    private static final Logger logger = LoggerFactory.getLogger(ForwardedWriteRequest.class);

    // For now, just supporting a single mutation to multiple recipients. This will develop in the future for different
    // kinds of mutations that each go to different recipients (see PaxosCommit).
    final Verb verb;
    final Mutation mutation;
    final Set<InetAddressAndPort> recipients;

    private ForwardedWriteRequest(Verb verb, Mutation mutation, Set<InetAddressAndPort> recipients)
    {
        this.verb = verb;
        this.mutation = mutation;
        this.recipients = recipients;
    }

    private ForwardedWriteRequest(Verb verb, Mutation mutation)
    {
        this(verb, mutation, new HashSet<>());
    }

    public static Builder builder(Verb verb, Mutation mutation)
    {
        return new Builder(verb, mutation);
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

    public void sendViaLeader(ReplicaPlan.ForWrite plan, ForwardedWriteResponseHandler handler)
    {
        Replica leader = getLeader(plan);

        // Add callbacks for replicas to respond directly to coordinator
        Message<ForwardedWriteRequest> toLeader = Message.out(Verb.FORWARDING_WRITE, this);
        for (InetAddressAndPort recipient : recipients)
        {
            logger.debug("Adding forwarding callback for response from {} id {}", recipient, toLeader.id());
            MessagingService.instance().callbacks.addWithExpiration(handler, toLeader, recipient);
        }
        MessagingService.instance().send(toLeader, leader.endpoint());
    }

    public static class Builder
    {
        final Verb verb;
        final Mutation mutation;
        final Set<InetAddressAndPort> recipients = new HashSet<>();

        public Builder(Verb verb, Mutation mutation)
        {
            this.verb = verb;
            this.mutation = mutation;
        }

        public ForwardedWriteRequest build()
        {
            return new ForwardedWriteRequest(verb, mutation, recipients);
        }

        public void addRecipient(InetAddressAndPort endpoint)
        {
            recipients.add(endpoint);
        }
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<ForwardedWriteRequest>
    {
        @Override
        public void serialize(ForwardedWriteRequest request, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(request.verb.id);
            Mutation.serializer.serialize(request.mutation, out, version);
            CollectionSerializer.serializeCollection(InetAddressAndPort.Serializer.inetAddressAndPortSerializer, request.recipients, out, version);
        }

        @Override
        public ForwardedWriteRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            Verb verb = Verb.fromId(in.readInt());
            Mutation mutation = Mutation.serializer.deserialize(in, version);
            Set<InetAddressAndPort> recipients = CollectionSerializer.deserializeCollection(InetAddressAndPort.Serializer.inetAddressAndPortSerializer, CollectionSerializer.newHashSet(), in, version);
            return new ForwardedWriteRequest(verb, mutation, recipients);
        }

        @Override
        public long serializedSize(ForwardedWriteRequest request, int version)
        {
            long size = 0;
            size += TypeSizes.INT_SIZE;
            size += Mutation.serializer.serializedSize(request.mutation, version);
            size += CollectionSerializer.serializedSizeCollection(InetAddressAndPort.Serializer.inetAddressAndPortSerializer, request.recipients, version);
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
            Verb verb = incoming.payload.verb;
            Mutation mutation = incoming.payload.mutation;
            InetAddressAndPort clientCoordinator = incoming.from();
            for (InetAddressAndPort recipient : incoming.payload.recipients)
            {
                Message.Builder<?> outgoing = Message.builder(verb, mutation);

                // Need to match to incoming ID so client-coordinator callback is invoked
                outgoing.withId(incoming.id());
                outgoing.withParam(ParamType.TRACKED_MUTATION_FORWARDING, new RespondTo(clientCoordinator, FBUtilities.getBroadcastAddressAndPort()));

                // TODO: Separate remote-DC handling
                // String localDataCenter = DatabaseDescriptor.getLocator().local().datacenter;
                // String dc = DatabaseDescriptor.getLocator().location(endpoint).datacenter;

                // Also need to acknowledge leader response callback for the journal, which is duplicative with
                // TrackedWriteRequest.perform

                Message<?> out = outgoing.build();
                logger.debug("Forwarding outgoing message {} id {}", out, out.id());
                MessagingService.instance().send(out, recipient);
            }
        }
    }

    // this is really "additional response"
    public static class RespondTo
    {
        public static IVersionedSerializer<RespondTo> serializer = new IVersionedSerializer<>()
        {
            @Override
            public void serialize(RespondTo respondTo, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(respondTo.coordinator, out, version);
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(respondTo.leader, out, version);
            }

            @Override
            public RespondTo deserialize(DataInputPlus in, int version) throws IOException
            {
                InetAddressAndPort coordinator = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
                InetAddressAndPort leader = InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version);
                return new RespondTo(coordinator, leader);
            }

            @Override
            public long serializedSize(RespondTo respondTo, int version)
            {
                long size = 0;
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(respondTo.coordinator, version);
                size += InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(respondTo.leader, version);
                return size;
            }
        };

        public final InetAddressAndPort coordinator;
        public final InetAddressAndPort leader;

        public RespondTo(InetAddressAndPort coordinator, InetAddressAndPort leader)
        {
            assert !coordinator.equals(leader);
            this.coordinator = coordinator;
            this.leader = leader;
        }
    }
}
