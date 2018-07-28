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
package org.apache.cassandra.db;

import java.util.Iterator;

import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tracing.Tracing;

public class MutationVerbHandler implements IVerbHandler<Mutation>
{
    public static final MutationVerbHandler instance = new MutationVerbHandler();

    private void respond(Message<?> respondTo, InetAddressAndPort respondToAddress)
    {
        Tracing.trace("Enqueuing response to {}", respondToAddress);
        MessagingService.instance().sendResponse(respondTo.emptyResponse(), respondToAddress);
    }

    private void failed()
    {
        Tracing.trace("Payload application resulted in WriteTimeout, not replying");
    }

    public void doVerb(Message<Mutation> message)
    {
        // Check if there were any forwarding headers in this message
        InetAddressAndPort from = message.forwardedFrom();
        InetAddressAndPort respondToAddress;
        if (from == null)
        {
            respondToAddress = message.from;
            ForwardToContainer forwardTo = message.forwardTo();
            if (forwardTo != null)
                forwardToLocalNodes(message.payload, message.verb, forwardTo, message.from);
        }
        else
        {

            respondToAddress = from;
        }

        try
        {
            message.payload.applyFuture().thenAccept(o -> respond(message, respondToAddress)).exceptionally(wto -> {
                failed();
                return null;
            });
        }
        catch (WriteTimeoutException wto)
        {
            failed();
        }
    }

    private static void forwardToLocalNodes(Mutation mutation, Verb verb, ForwardToContainer forwardTo, InetAddressAndPort from)
    {
        // tell the recipients who to send their ack to
        Message<Mutation> message = Message.outWithParam(verb, mutation, ParamType.FORWARDED_FROM, from);
        Iterator<InetAddressAndPort> iterator = forwardTo.targets.iterator();
        // Send a message to each of the addresses on our Forward List
        for (int i = 0; i < forwardTo.targets.size(); i++)
        {
            InetAddressAndPort address = iterator.next();
            Tracing.trace("Enqueuing forwarded write to {}", address);
            MessagingService.instance().sendOneWay(message.withId(forwardTo.messageIds[i]), address);
        }
    }
}
