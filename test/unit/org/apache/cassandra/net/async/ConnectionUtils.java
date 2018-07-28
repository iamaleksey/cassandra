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

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;

public class ConnectionUtils
{

    public static class OutboundCountChecker
    {
        private final OutboundConnection connection;
        private long submitted;
        private long pending, pendingBytes;
        private long sent, sentBytes;
        private long overload, overloadBytes;
        private long expired, expiredBytes;
        private long error, errorBytes;
        private boolean checkSubmitted, checkPending, checkSent, checkOverload, checkExpired, checkError;

        private OutboundCountChecker(OutboundConnection connection)
        {
            this.connection = connection;
        }

        public OutboundCountChecker submitted(long count)
        {
            submitted = count;
            checkSubmitted = true;
            return this;
        }

        public OutboundCountChecker pending(long count, long bytes)
        {
            pending = count;
            pendingBytes = bytes;
            checkPending = true;
            return this;
        }

        public OutboundCountChecker sent(long count, long bytes)
        {
            sent = count;
            sentBytes = bytes;
            checkSent = true;
            return this;
        }

        public OutboundCountChecker overload(long count, long bytes)
        {
            overload = count;
            overloadBytes = bytes;
            checkOverload = true;
            return this;
        }

        public OutboundCountChecker expired(long count, long bytes)
        {
            expired = count;
            expiredBytes = bytes;
            checkExpired = true;
            return this;
        }

        public OutboundCountChecker error(long count, long bytes)
        {
            error = count;
            errorBytes = bytes;
            checkError = true;
            return this;
        }

        public void check()
        {
            if (checkSubmitted)
            {
                Assert.assertEquals(submitted, connection.submittedCount());
            }
            if (checkPending)
            {
                Assert.assertEquals(pending, connection.pendingCount());
                Assert.assertEquals(pendingBytes, connection.pendingBytes());
            }
            if (checkSent)
            {
                Assert.assertEquals(sent, connection.sentCount());
                Assert.assertEquals(sentBytes, connection.sentBytes());
            }
            if (checkOverload)
            {
                Assert.assertEquals(overload, connection.overloadCount());
                Assert.assertEquals(overloadBytes, connection.overloadBytes());
            }
            if (checkExpired)
            {
                Assert.assertEquals(expired, connection.expiredCount());
                Assert.assertEquals(expiredBytes, connection.expiredBytes());
            }
            if (checkError)
            {
                Assert.assertEquals(error, connection.errorCount());
                Assert.assertEquals(errorBytes, connection.errorBytes());
            }
        }
    }

    public static class InboundCountChecker
    {
        private final InboundMessageHandlers connection;
        private long pending, pendingBytes;
        private long received, receivedBytes;
        private long processed, processedBytes;
        private long expired, expiredBytes;
        private long error, errorBytes;
        private boolean checkPending, checkReceived, checkProcessed, checkExpired, checkError;

        private InboundCountChecker(InboundMessageHandlers connection)
        {
            this.connection = connection;
        }

        public InboundCountChecker pending(long count, long bytes)
        {
            pending = count;
            pendingBytes = bytes;
            checkPending = true;
            return this;
        }

        public InboundCountChecker received(long count, long bytes)
        {
            received = count;
            receivedBytes = bytes;
            checkReceived = true;
            return this;
        }

        public InboundCountChecker processed(long count, long bytes)
        {
            processed = count;
            processedBytes = bytes;
            checkProcessed = true;
            return this;
        }

        public InboundCountChecker expired(long count, long bytes)
        {
            expired = count;
            expiredBytes = bytes;
            checkExpired = true;
            return this;
        }

        public InboundCountChecker error(long count, long bytes)
        {
            error = count;
            errorBytes = bytes;
            checkError = true;
            return this;
        }

        public void check()
        {
            if (checkReceived)
            {
                Assert.assertEquals(received, connection.receivedCount());
                Assert.assertEquals(receivedBytes, connection.receivedBytes());
            }
            if (checkProcessed)
            {
                Assert.assertEquals(processed, connection.processedCount());
                Assert.assertEquals(processedBytes, connection.processedBytes());
            }
            if (checkExpired)
            {
                Assert.assertEquals(expired, connection.expiredCount());
                Assert.assertEquals(expiredBytes, connection.expiredBytes());
            }
            if (checkError)
            {
                Assert.assertEquals(error, connection.errorCount());
                Assert.assertEquals(errorBytes, connection.errorBytes());
            }
            if (checkPending)
            {
                // pending cannot relied upon to not race with completion of the task,
                // so if it is currently above the value we expect, sleep for a bit
                if (pending < connection.pendingCount())
                    for (int i = 0 ; i < 10 && pending < connection.pendingCount() ; ++i)
                        Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.MILLISECONDS);
                Assert.assertEquals(pending, connection.pendingCount());
                Assert.assertEquals(pendingBytes, connection.pendingBytes());
            }
        }
    }

    public static OutboundCountChecker check(OutboundConnection outbound)
    {
        return new OutboundCountChecker(outbound);
    }

    public static InboundCountChecker check(InboundMessageHandlers inbound)
    {
        return new InboundCountChecker(inbound);
    }

}
