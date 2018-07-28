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
package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.Tracing;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.INTERNAL_RSP;
import static org.apache.cassandra.net.ParamType.TRACE_SESSION;

// TOOD: These tests don't really do much?
public class MessageOutTest
{
    private static InetAddressAndPort endpoint;

    @BeforeClass
    public static void before() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        endpoint = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", 0);
    }

    @Test
    public void captureTracingInfo_ForceException()
    {
        Message message = Message.outWithParam(0, INTERNAL_RSP, 0, noPayload, TRACE_SESSION, new byte[9]);
        Tracing.instance.traceOutgoingMessage(message, endpoint);
    }

    @Test
    public void captureTracingInfo_UnknownSession()
    {
        UUID uuid = UUID.randomUUID();
        Message message = Message.outWithParam(0, INTERNAL_RSP, 0, noPayload, TRACE_SESSION, uuid);
        Tracing.instance.traceOutgoingMessage(message, endpoint);
    }

    @Test
    public void captureTracingInfo_KnownSession()
    {
        Tracing.instance.newSession(new HashMap<>());
        Message message = Message.outWithParam(0, Verb.REQUEST_RSP, 0, noPayload, null, null);
        Tracing.instance.traceOutgoingMessage(message, endpoint);
    }
}
