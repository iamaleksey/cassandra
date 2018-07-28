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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Math.*;

// TODO: test corruption
// TODO: use a different random seed each time
// TODO: use quick theories
public class LZ4Test
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static class SequenceOfFrames
    {
        final List<byte[]> uncompressed;
        final int[] boundaries;
        final ByteBuffer frames;

        private SequenceOfFrames(List<byte[]> uncompressed, int[] boundaries, ByteBuffer frames)
        {
            this.uncompressed = uncompressed;
            this.boundaries = boundaries;
            this.frames = frames;
        }
    }

    @Test
    public void testSome() throws Exception
    {
        Random random = new Random(0);
        for (int i = 0 ; i < 1000 ; ++i)
            testTwoRandom(random);
    }

    private void testTwoRandom(Random random) throws Exception
    {
        SequenceOfFrames sequenceOfFrames = pairOfFrames(random);
        LZ4Decoder decoder = LZ4Decoder.fast();

        List<byte[]> uncompressed = sequenceOfFrames.uncompressed;
        ByteBuffer frames = sequenceOfFrames.frames;
        int[] boundaries = sequenceOfFrames.boundaries;

        int end = frames.limit();
        List<Object> out = new ArrayList<>();
        int prevBoundary = -1;
        for (int i = 0 ; i < end ; )
        {
            int limit = i + random.nextInt(1 + end - i);
            decoder.decode(null, Unpooled.wrappedBuffer((ByteBuffer) sequenceOfFrames.frames.duplicate().position(i).limit(limit)), out);
            int boundary = Arrays.binarySearch(boundaries, limit);
            if (boundary < 0) boundary = -2 -boundary;

            while (prevBoundary < boundary)
            {
                ++prevBoundary;
                Assert.assertTrue(out.size() >= 1 + prevBoundary);
                verify(uncompressed.get(prevBoundary), (ByteBuf) out.get(prevBoundary));
            }
            i = limit;
        }
    }

    private static void verify(byte[] expect, ByteBuf actual)
    {
        byte[] fetch = new byte[expect.length];
        actual.getBytes(actual.readerIndex(), fetch);
        Assert.assertArrayEquals(expect, fetch);
        actual.release();
    }

    private static SequenceOfFrames pairOfFrames(Random random)
    {
        int frameCount = 1 + random.nextInt(8);
        List<byte[]> uncompressed = new ArrayList<>();
        List<ByteBuffer> compressed = new ArrayList<>();
        int[] cumulativeCompressedLength = new int[frameCount];
        for (int i = 0 ; i < frameCount ; ++i)
        {
            byte[] bytes = randomishBytes(random);
            uncompressed.add(bytes);

            ByteBuffer buffer = LZ4Encoder.fastInstance.encode(ByteBuffer.wrap(bytes));
            compressed.add(buffer);
            cumulativeCompressedLength[i] = (i == 0 ? 0 : cumulativeCompressedLength[i - 1]) + buffer.limit();
        }

        ByteBuffer frames = BufferPool.get(cumulativeCompressedLength[frameCount - 1], BufferType.OFF_HEAP);
        for (ByteBuffer buffer : compressed)
        {
            frames.put(buffer);
            BufferPool.put(buffer);
        }
        frames.flip();
        return new SequenceOfFrames(uncompressed, cumulativeCompressedLength, frames);
    }

    private static byte[] randomishBytes(Random random)
    {
        byte[] bytes = new byte[1 + random.nextInt(1 << 15)];
        int runLength = 1 + random.nextInt(255);
        for (int i = 0 ; i < bytes.length ; i += runLength)
        {
            byte b = (byte) random.nextInt(256);
            Arrays.fill(bytes, i, min(bytes.length, i + runLength), b);
        }
        return bytes;
    }

}
