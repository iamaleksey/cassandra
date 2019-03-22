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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Math.*;
import static org.apache.cassandra.net.async.SharedBytes.wrap;

// TODO: test corruption
// TODO: use a different random seed each time
// TODO: use quick theories
public class FramingTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static class SequenceOfFrames
    {
        final List<byte[]> original;
        final int[] boundaries;
        final SharedBytes frames;

        private SequenceOfFrames(List<byte[]> original, int[] boundaries, ByteBuffer frames)
        {
            this.original = original;
            this.boundaries = boundaries;
            this.frames = wrap(frames);
        }
    }

    @Test
    public void testRandomLZ4() throws Exception
    {
        testSome(FrameEncoderLZ4.fastInstance, FrameDecoderLZ4.fast());
    }

    @Test
    public void testRandomCrc() throws Exception
    {
        testSome(FrameEncoderCrc.instance, FrameDecoderCrc.create());
    }

    public void testSome(FrameEncoder encoder, FrameDecoder decoder) throws Exception
    {
        Random random = new Random(0);
        for (int i = 0 ; i < 1000 ; ++i)
            testTwoRandom(random, encoder, decoder);
    }

    private void testTwoRandom(Random random, FrameEncoder encoder, FrameDecoder decoder) throws Exception
    {
        SequenceOfFrames sequenceOfFrames = pairOfFrames(random, encoder);

        List<byte[]> uncompressed = sequenceOfFrames.original;
        SharedBytes frames = sequenceOfFrames.frames;
        int[] boundaries = sequenceOfFrames.boundaries;

        int end = frames.get().limit();
        List<FrameDecoder.Frame> out = new ArrayList<>();
        int prevBoundary = -1;
        for (int i = 0 ; i < end ; )
        {
            int limit = i + random.nextInt(1 + end - i);
            decoder.decode(out, frames.slice(i, limit));
            int boundary = Arrays.binarySearch(boundaries, limit);
            if (boundary < 0) boundary = -2 -boundary;

            while (prevBoundary < boundary)
            {
                ++prevBoundary;
                Assert.assertTrue(out.size() >= 1 + prevBoundary);
                verify(uncompressed.get(prevBoundary), ((FrameDecoder.IntactFrame) out.get(prevBoundary)).contents);
            }
            i = limit;
        }
        for (FrameDecoder.Frame frame : out)
            frame.release();
        frames.release();
    }

    private static void verify(byte[] expect, SharedBytes actual)
    {
        byte[] fetch = new byte[expect.length];
        actual.get().get(fetch);
        Assert.assertArrayEquals(expect, fetch);
    }

    private static SequenceOfFrames pairOfFrames(Random random, FrameEncoder encoder)
    {
        int frameCount = 1 + random.nextInt(8);
        List<byte[]> uncompressed = new ArrayList<>();
        List<ByteBuf> compressed = new ArrayList<>();
        int[] cumulativeCompressedLength = new int[frameCount];
        for (int i = 0 ; i < frameCount ; ++i)
        {
            byte[] bytes = randomishBytes(random);
            uncompressed.add(bytes);

            FrameEncoder.Payload payload = encoder.allocator().allocate(true, bytes.length);
            payload.buffer.put(bytes);
            payload.finish();

            ByteBuf buffer = encoder.encode(true, payload.buffer);
            compressed.add(buffer);
            cumulativeCompressedLength[i] = (i == 0 ? 0 : cumulativeCompressedLength[i - 1]) + buffer.readableBytes();
        }

        ByteBuffer frames = BufferPool.getAtLeast(cumulativeCompressedLength[frameCount - 1], BufferType.OFF_HEAP);
        for (ByteBuf buffer : compressed)
        {
            frames.put(buffer.internalNioBuffer(buffer.readerIndex(), buffer.readableBytes()));
            buffer.release();
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
