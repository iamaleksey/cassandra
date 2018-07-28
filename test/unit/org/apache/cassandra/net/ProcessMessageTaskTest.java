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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.net.Verb.MUTATION_REQ;
import static org.apache.cassandra.net.Verb._TEST_1;
import static org.apache.cassandra.net.Verb._TEST_2;

public class ProcessMessageTaskTest
{
    private static final MockVerbHandler VERB_HANDLER = new MockVerbHandler();

    @BeforeClass
    public static void before() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.daemonInitialization();
        _TEST_2.unsafeSetHandler(() -> VERB_HANDLER);
    }

    @AfterClass
    public static void after() throws NoSuchFieldException, IllegalAccessException
    {
        _TEST_2.unsafeSetHandler(() -> null);
    }

    @Before
    public void setUp()
    {
        VERB_HANDLER.reset();
    }

    @Test
    public void process_HappyPath() throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        Message msg = Message.builder(_TEST_2, null)
                             .from(addr)
                             .withId(42)
                             .build();
        ProcessMessageTask task = new ProcessMessageTask(msg);
        Assert.assertTrue(task.process());
        Assert.assertEquals(1, VERB_HANDLER.invocationCount);
    }

    @Test
    public void process_NullVerb() throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        Message msg = Message.builder(null, null)
                             .from(addr)
                             .withId(42)
                             .build();
        ProcessMessageTask task = new ProcessMessageTask(msg);
        Assert.assertFalse(task.process());
    }

    @Test
    public void process_NoHandler() throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        Message msg = Message.builder(_TEST_1, null)
                             .from(addr)
                             .withId(42)
                             .build();
        ProcessMessageTask task = new ProcessMessageTask(msg);
        Assert.assertFalse(task.process());
    }

    @Test
    public void process_ExpiredDroppableMessage() throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");

        // we need any droppable verb, so just grab it from the enum itself rather than hard code a value
        Message msg = Message.builder(MUTATION_REQ, null)
                             .from(addr)
                             .withCreationTime(0)
                             .build();
        ProcessMessageTask task = new ProcessMessageTask(msg);
        Assert.assertFalse(task.process());
    }

    // non-droppable message should still be processed even if they are expired
    @Test
    public void process_ExpiredMessage() throws UnknownHostException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        Message msg = Message.builder(_TEST_2, null)
                             .from(addr)
                             .withCreationTime(0)
                             .withId(42)
                             .build();
        ProcessMessageTask task = new ProcessMessageTask(msg);
        Assert.assertTrue(task.process());
        Assert.assertEquals(1, VERB_HANDLER.invocationCount);
    }

    private static class MockVerbHandler implements IVerbHandler<Object>
    {
        private int invocationCount;

        @Override
        public void doVerb(Message<Object> message)
        {
            invocationCount++;
        }

        void reset()
        {
            invocationCount = 0;
        }
    }
}
