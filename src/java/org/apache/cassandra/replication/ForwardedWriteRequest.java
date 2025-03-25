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
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.utils.FBUtilities;

public class ForwardedWriteRequest
{
    final Map<InetAddressAndPort, Message<?>> receipents = new HashMap<>();

    public static Builder builder()
    {
        return new Builder();
    }

    private Replica getLeader(ReplicaPlan.ForWrite plan)
    {
        // TODO: Should match ReplicaPlans.findCounterLeaderReplica, including DC-local priority
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        EndpointsForToken replicas = plan.liveUncontacted();
        replicas = proximity.sortedByProximity(FBUtilities.getBroadcastAddressAndPort(), replicas);
        return replicas.get(0);
    }

    public void sendViaLeader(ReplicaPlan.ForWrite plan, ForwardedWriteResponseHandler handler)
    {
        Replica leader = getLeader(plan);

        // Add callbacks for replicas to respond directly to coordinator
        for (Map.Entry<InetAddressAndPort, Message<?>> receipient : receipents.entrySet())
        {
            InetAddressAndPort peer = receipient.getKey();
            Message<?> forwarding = receipient.getValue();
            MessagingService.instance().callbacks.addWithExpiration(handler, forwarding, peer);
        }

        Message<ForwardedWriteRequest> message = Message.out(Verb.FORWARDING_WRITE, this);
        MessagingService.instance().send(message, leader.endpoint());
    }

    public static class Builder
    {
        final Map<InetAddressAndPort, Message<?>> receipents = new HashMap<>();

        public ForwardedWriteRequest build()
        {
            return new ForwardedWriteRequest();
        }

        public void addRecipient(InetAddressAndPort endpoint, Message<?> message)
        {
            // String localDataCenter = DatabaseDescriptor.getLocator().local().datacenter;
            // String dc = DatabaseDescriptor.getLocator().location(endpoint).datacenter;
            receipents.put(endpoint, message);
        }
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<ForwardedWriteRequest>
    {
        @Override
        public void serialize(ForwardedWriteRequest t, DataOutputPlus out, int version) throws IOException
        {

        }

        @Override
        public ForwardedWriteRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            return null;
        }

        @Override
        public long serializedSize(ForwardedWriteRequest t, int version)
        {
            return 0;
        }
    }

    public static final VerbHandler verbHandler = new VerbHandler();

    public static class VerbHandler implements IVerbHandler<ForwardedWriteRequest>
    {
        @Override
        public void doVerb(Message<ForwardedWriteRequest> incoming)
        {
            ForwardedWriteRequest request = incoming.payload;
            InetAddressAndPort clientCoordinator = incoming.from();
            for (Map.Entry<InetAddressAndPort, Message<?>> entry : request.receipents.entrySet())
            {
                InetAddressAndPort peer = entry.getKey();
                Message<?> forwarding = entry.getValue();
                Message.Builder<?> outgoing = Message.builder(forwarding);

                // Need to match to incoming ID so client-coordinator callback is invoked
                outgoing.withId(incoming.id());
                outgoing.withParam(ParamType.RESPOND_TO, new RespondTo(clientCoordinator, FBUtilities.getBroadcastAddressAndPort()));

                MessagingService.instance().send(outgoing.build(), peer);
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

            }

            @Override
            public RespondTo deserialize(DataInputPlus in, int version) throws IOException
            {
                return null;
            }

            @Override
            public long serializedSize(RespondTo respondTo, int version)
            {
                return 0;
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
