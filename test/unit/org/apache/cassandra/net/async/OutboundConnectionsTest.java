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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.BackPressureState;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.PingMessage;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.OutboundConnection.Type;

public class OutboundConnectionsTest
{
    static final InetAddressAndPort LOCAL_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.1"), 9476);
    static final InetAddressAndPort REMOTE_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 9476);
    private static final InetAddressAndPort RECONNECT_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.3"), 9476);
    private static final List<Type> INTERNODE_MESSAGING_CONN_TYPES = ImmutableList.of(Type.URGENT, Type.LARGE_MESSAGE, Type.SMALL_MESSAGE);

    private OutboundConnections pool;

    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        BackPressureState backPressureState = DatabaseDescriptor.getBackPressureStrategy().newState(REMOTE_ADDR);
        pool = new OutboundConnections(new OutboundConnectionSettings(REMOTE_ADDR), backPressureState);
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException
    {
        if (pool != null)
            pool.close(false).get(10L, TimeUnit.SECONDS);
    }

    @Test
    public void getConnection_Gossip()
    {
        GossipDigestSyn syn = new GossipDigestSyn("cluster", "partitioner", new ArrayList<>(0));
        Message<GossipDigestSyn> message = Message.out(Verb.GOSSIP_DIGEST_SYN, syn);
        Assert.assertEquals(Type.URGENT, pool.connectionFor(message).type());
    }

    @Test
    public void getConnection_SmallMessage()
    {
        Message message = Message.out(Verb.PING_REQ, PingMessage.forSmall);
        Assert.assertEquals(Type.SMALL_MESSAGE, pool.connectionFor(message).type());
    }

    @Test
    public void getConnection_LargeMessage() throws NoSuchFieldException, IllegalAccessException
    {
        // just need a serializer to report a size, as fake as it may be
        IVersionedSerializer<Object> serializer = new IVersionedSerializer<Object>()
        {
            public void serialize(Object o, DataOutputPlus out, int version)
            {

            }

            public Object deserialize(DataInputPlus in, int version)
            {
                return null;
            }

            public long serializedSize(Object o, int version)
            {
                return OutboundConnections.LARGE_MESSAGE_THRESHOLD + 1;
            }
        };
        Verb._TEST_2.unsafeSetSerializer(() -> serializer);
        Message message = Message.out(Verb._TEST_2, "payload");
        Assert.assertEquals(Type.LARGE_MESSAGE, pool.connectionFor(message).type());
    }

    @Test
    public void close_SoftClose() throws ExecutionException, InterruptedException, TimeoutException
    {
        for (Type type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertFalse(pool.connectionFor(type).isClosed());
        pool.close(true).get(10L, TimeUnit.SECONDS);
        for (Type type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertTrue(pool.connectionFor(type).isClosed());
    }

    @Test
    public void close_NotSoftClose() throws ExecutionException, InterruptedException, TimeoutException
    {
        for (Type type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertFalse(pool.connectionFor(type).isClosed());
        pool.close(false).get(10L, TimeUnit.SECONDS);
        for (Type type : INTERNODE_MESSAGING_CONN_TYPES)
            Assert.assertTrue(pool.connectionFor(type).isClosed());
    }

    @Test
    public void reconnectWithNewIp() throws InterruptedException
    {
        for (Type type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(REMOTE_ADDR, pool.connectionFor(type).settings().connectTo);
        }

        pool.reconnectWithNewIp(RECONNECT_ADDR).await();

        for (Type type : INTERNODE_MESSAGING_CONN_TYPES)
        {
            Assert.assertEquals(RECONNECT_ADDR, pool.connectionFor(type).settings().connectTo);
        }
    }

//    @Test
//    public void timeoutCounter()
//    {
//        long originalValue = pool.getTimeouts();
//        pool.incrementTimeout();
//        Assert.assertEquals(originalValue + 1, pool.getTimeouts());
//    }
}
