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

package org.apache.cassandra.service.reads.repair;

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.reads.ReadCallback;

public class AsyncReadRepairTest extends AbstractReadRepairTest
{
    private static class InstrumentedAsyncReadRepair extends AsyncReadRepair implements InstrumentedReadRepair
    {
        public InstrumentedAsyncReadRepair(ReadCommand command, long queryStartNanoTime, ConsistencyLevel consistency)
        {
            super(command, queryStartNanoTime, consistency);
        }

        Set<InetAddressAndPort> readCommandRecipients = new HashSet<>();
        ReadCallback readCallback = null;

        @Override
        void sendReadCommand(InetAddressAndPort to, ReadCallback callback)
        {
            assert readCallback == null || readCallback == callback;
            readCommandRecipients.add(to);
            readCallback = callback;
        }

        @Override
        Iterable<InetAddressAndPort> getCandidatesForToken(Token token)
        {
            return targets;
        }

        @Override
        public Set<InetAddressAndPort> getReadRecipients()
        {
            return readCommandRecipients;
        }

        @Override
        public ReadCallback getReadCallback()
        {
            return readCallback;
        }
    }

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        configureClass(ReadRepairStrategy.ASYNC);
    }

    @Override
    public InstrumentedReadRepair createInstrumentedReadRepair(ReadCommand command, long queryStartNanoTime, ConsistencyLevel consistency)
    {
        return new InstrumentedAsyncReadRepair(command, queryStartNanoTime, consistency);
    }
}
