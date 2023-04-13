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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.channels.FileChannel;

import org.junit.Test;

import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

public class MappedBufferTest
{
    @Test
    public void test() throws IOException
    {
        File dir = new File("/tmp/test-" + System.nanoTime());
        dir.createDirectoriesIfNotExists();
        dir.deleteRecursiveOnExit();

        File readFile = new File(dir, "real");
        File mockedFile = new File(dir, "mocked");

        MappedBuffer real = MappedBuffer.open(MappedBuffer.Type.MAPPED, readFile.newReadWriteChannel(), FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);
        MappedBuffer mocked = MappedBuffer.open(MappedBuffer.Type.MOCKED, mockedFile.newReadWriteChannel(), FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);

        for (int i = 0; i < 100; i++)
        {
            real.putInt(i);
            mocked.putInt(i);
        }
        real.syncForce();
        real.clean();
        mocked.syncForce();
        mocked.clean();

        real = MappedBuffer.open(MappedBuffer.Type.MAPPED, readFile.newReadChannel(), FileChannel.MapMode.READ_ONLY, 0, readFile.length());
        mocked = MappedBuffer.open(MappedBuffer.Type.MOCKED, mockedFile.newReadChannel(), FileChannel.MapMode.READ_ONLY, 0, mockedFile.length());

        DataInputBuffer realIn = real.duplicate().in();
        DataInputBuffer mockedIn = mocked.duplicate().in();

        for (int i = 0; i < 100; i++)
        {
            assertThat(realIn.readInt()).isEqualTo(i);
            assertThat(mockedIn.readInt()).isEqualTo(i);
        }
    }
}