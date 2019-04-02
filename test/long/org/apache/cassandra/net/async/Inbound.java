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

package org.apache.cassandra.net.async;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.locator.InetAddressAndPort;

class Inbound
{
    final Map<InetAddressAndPort, InboundMessageHandlers> handlers;
    final InboundSockets sockets;

    Inbound(List<InetAddressAndPort> endpoints, GlobalInboundSettings settings, InboundMessageHandler.MessageProcessor process, Function<MessageCallbacks, MessageCallbacks> callbacks)
    {
        ResourceLimits.Limit globalLimit = new ResourceLimits.Concurrent(settings.globalReserveLimit);
        InboundMessageHandler.WaitQueue globalWaitQueue = new InboundMessageHandler.WaitQueue(globalLimit);
        Map<InetAddressAndPort, InboundMessageHandlers> handlers = new HashMap<>();
        for (InetAddressAndPort endpoint : endpoints)
            handlers.put(endpoint, new InboundMessageHandlers(endpoint, settings.queueCapacity, settings.endpointReserveLimit, globalLimit, globalWaitQueue, process, callbacks));
        this.handlers = handlers;
        InboundConnectionSettings template = settings.template.withHandlers(handlers::get);
        this.sockets = new InboundSockets(endpoints.stream()
                                                   .map(template::withBindAddress)
                                                   .collect(Collectors.toList()));
    }
}
