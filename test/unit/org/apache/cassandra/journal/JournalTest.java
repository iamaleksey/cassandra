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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;

public class JournalTest
{
    private static final Set<Integer> SENTINEL_HOSTS = Collections.singleton(0);

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Test
    public void testSimpleReadWrite() throws IOException
    {
        File directory = new File(Files.createTempDirectory("JournalTest"));
        directory.deleteRecursiveOnExit();

        Journal<TimeUUID, Long> journal =
            new Journal<>("TestJournal", directory, TestParams.INSTANCE, TimeUUIDKeySupport.INSTANCE, LongSerializer.INSTANCE);

        journal.start();

        TimeUUID id1 = nextTimeUUID();
        TimeUUID id2 = nextTimeUUID();
        TimeUUID id3 = nextTimeUUID();
        TimeUUID id4 = nextTimeUUID();

        journal.blockingWrite(id1, 1L, Collections.singleton(1));
        journal.blockingWrite(id2, 2L, Collections.singleton(1));
        journal.blockingWrite(id3, 3L, Collections.singleton(1));
        journal.blockingWrite(id4, 4L, Collections.singleton(1));

        assertEquals(1L, (long) journal.readFirst(id1));
        assertEquals(2L, (long) journal.readFirst(id2));
        assertEquals(3L, (long) journal.readFirst(id3));
        assertEquals(4L, (long) journal.readFirst(id4));

        journal.shutdown();

        journal = new Journal<>("TestJournal", directory, TestParams.INSTANCE, TimeUUIDKeySupport.INSTANCE, LongSerializer.INSTANCE);
        journal.start();

        assertEquals(1L, (long) journal.readFirst(id1));
        assertEquals(2L, (long) journal.readFirst(id2));
        assertEquals(3L, (long) journal.readFirst(id3));
        assertEquals(4L, (long) journal.readFirst(id4));

        journal.shutdown();
    }

    @Test
    public void segmentCompactionTest() throws IOException
    {
        File directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();

        Journal<TimeUUID, ByteBuffer> journal = new Journal<>("TestJournal", directory, new TestParams() {
            public int segmentSize()
            {
                return 1024 * 1024;
            }
        }, TimeUUIDKeySupport.INSTANCE, ByteBufferSerializer.INSTANCE);
        journal.start();

        Map<TimeUUID, List<ByteBuffer>> uuids = new HashMap<>();

        int count = 0;
        for (int i = 0; i < 1024; i++)
        {
            TimeUUID uuid = nextTimeUUID();
            for (long j = 0; j < 5; j++)
            {
                ByteBuffer buf = ByteBuffer.allocate(1024);
                for (int k = 0; k < 1024; k++)
                    buf.put((byte) count);
                count++;
                buf.rewind();
                uuids.computeIfAbsent(uuid, (k) -> new ArrayList<>())
                     .add(buf);
                journal.asyncWrite(uuid, buf, SENTINEL_HOSTS);
            }
        }

        journal.closeCurrentSegmentForTesting();
        Runnable checkAll = () -> {
            for (Map.Entry<TimeUUID, List<ByteBuffer>> e : uuids.entrySet())
            {
                List<ByteBuffer> expected = e.getValue();
                List<ByteBuffer> actual = journal.readAll(e.getKey());
                for (int i = 0; i < actual.size(); i++)
                {
                    if (!actual.get(i).equals(expected.get(i)))
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Actual:\n");
                        for (ByteBuffer bb : actual)
                            sb.append(ByteBufferUtil.bytesToHex(bb)).append("\n");
                        sb.append("Expected:\n");
                        for (ByteBuffer bb : expected)
                            sb.append(ByteBufferUtil.bytesToHex(bb)).append("\n");
                        throw new AssertionError(sb.toString());
                    }
                }
            }
        };

        checkAll.run();
        journal.compactStaticSegments();
        checkAll.run();
    }

    static class ByteBufferSerializer implements ValueSerializer<TimeUUID, ByteBuffer>
    {
        static final ByteBufferSerializer INSTANCE = new ByteBufferSerializer();

        public int serializedSize(TimeUUID key, ByteBuffer value, int userVersion)
        {
            return Integer.BYTES + value.capacity();
        }

        public void serialize(TimeUUID key, ByteBuffer value, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeInt(value.capacity());
            out.write(value);
        }

        public ByteBuffer deserialize(TimeUUID key, DataInputPlus in, int userVersion) throws IOException
        {
            byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);
            return ByteBuffer.wrap(bytes);
        }
    }

    static class LongSerializer implements ValueSerializer<TimeUUID, Long>
    {
        static final LongSerializer INSTANCE = new LongSerializer();

        public int serializedSize(TimeUUID key, Long value, int userVersion)
        {
            return Long.BYTES;
        }

        public void serialize(TimeUUID key, Long value, DataOutputPlus out, int userVersion) throws IOException
        {
            out.writeLong(value);
        }

        public Long deserialize(TimeUUID key, DataInputPlus in, int userVersion) throws IOException
        {
            return in.readLong();
        }
    }
}
