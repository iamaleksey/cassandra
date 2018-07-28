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

package org.apache.cassandra.db.streaming;

import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.serializers.SerializationUtils;

import static org.junit.Assert.assertNotEquals;

public class ComponentManifestTest
{
    @Test
    public void testSerialization()
    {
        ComponentManifest expected = new ComponentManifest(new LinkedHashMap<Component, Long>() {{ put(Component.DATA, 100L); }});
        SerializationUtils.assertSerializationCycle(expected, ComponentManifest.serializer);
    }

    @Test(expected = EOFException.class)
    public void testSerialization_FailsOnBadBytes() throws IOException
    {
        ByteBuf buf = Unpooled.buffer(512);
        ComponentManifest expected = new ComponentManifest(new LinkedHashMap<Component, Long>() {{ put(Component.DATA, 100L); }});

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ComponentManifest.serializer.serialize(expected, out, MessagingService.VERSION_40);

            buf.setInt(0, -100);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            ComponentManifest actual = ComponentManifest.serializer.deserialize(in, MessagingService.VERSION_40);
            assertNotEquals(expected, actual);
        }
    }
}
