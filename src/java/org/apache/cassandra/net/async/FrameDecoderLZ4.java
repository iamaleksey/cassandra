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
import java.nio.ByteOrder;
import java.util.List;
import java.util.zip.CRC32;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.net.async.Crc.crc24;
import static org.apache.cassandra.net.async.Crc.crc32;
import static org.apache.cassandra.net.async.Crc.updateCrc32;

final class FrameDecoderLZ4 extends FrameDecoder
{
    public static FrameDecoderLZ4 fast()
    {
        return new FrameDecoderLZ4(LZ4Factory.fastestInstance().fastDecompressor());
    }

    private static final int HEADER_LENGTH = 8;
    private static final int TRAILER_LENGTH = 4;
    private static final int HEADER_AND_TRAILER_LENGTH = 12;

    private static int compressedLength(long header8b)
    {
        return ((int) header8b) & 0x1FFFF;
    }
    private static int uncompressedLength(long header8b)
    {
        return ((int) (header8b >>> 17)) & 0x1FFFF;
    }
    private static IsSelfContained isSelfContained(long header8b)
    {
        if (0 == (header8b & (1L << 34)))
            return IsSelfContained.NO;
        return IsSelfContained.YES;
    }
    private static int headerCrc(long header8b)
    {
        return ((int) (header8b >>> 40)) & 0xFFFFFF;
    }

    private final LZ4FastDecompressor decompressor;
    public FrameDecoderLZ4(LZ4FastDecompressor decompressor)
    {
        this.decompressor = decompressor;
    }

    final long readHeader(ByteBuffer frame, int begin)
    {
        long header8b = frame.getLong(begin);
        if (frame.order() == ByteOrder.BIG_ENDIAN)
            header8b = Long.reverseBytes(header8b);
        return header8b;
    }

    final CorruptFrame verifyHeader(long header8b)
    {
        int computeLengthCrc = crc24(header8b, 5);
        int readLengthCrc = headerCrc(header8b);

        return readLengthCrc == computeLengthCrc ? null : CorruptFrame.unrecoverable(readLengthCrc, computeLengthCrc);
    }

    final int frameLength(long header8b)
    {
        return compressedLength(header8b) + HEADER_AND_TRAILER_LENGTH;
    }

    final Frame unpackFrame(Slice slice, int begin, int end, long header8b)
    {
        ByteBuffer input = slice.contents;

        IsSelfContained isSelfContained = isSelfContained(header8b);
        int uncompressedLength = uncompressedLength(header8b);

        CRC32 crc = crc32();
        int readFullCrc = input.getInt(end - TRAILER_LENGTH);
        if (input.order() == ByteOrder.BIG_ENDIAN)
            readFullCrc = Integer.reverseBytes(readFullCrc);

        updateCrc32(crc, input, begin + HEADER_LENGTH, end - TRAILER_LENGTH);
        int computeFullCrc = (int) crc.getValue();

        if (readFullCrc != computeFullCrc)
            return CorruptFrame.recoverable(isSelfContained, uncompressedLength, readFullCrc, computeFullCrc);

        if (uncompressedLength == 0)
        {
            return new IntactFrame(isSelfContained, sliceIfRemaining(slice, begin + HEADER_LENGTH, end - TRAILER_LENGTH));
        }
        else
        {
            ByteBuffer out = BufferPool.get(uncompressedLength, BufferType.OFF_HEAP);
            try
            {
                decompressor.decompress(input, begin + HEADER_LENGTH, out, 0, uncompressedLength);
                return new IntactFrame(isSelfContained, Slice.wrap(out));
            }
            catch (Throwable t)
            {
                BufferPool.put(out);
                throw t;
            }
        }
    }

    protected void decode(ChannelHandlerContext ctx, Slice slice, List<Object> output)
    {
        // TODO: confirm in assembly output that we inline the relevant nested method calls
        decode(slice, HEADER_LENGTH, output);
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderLZ4", this);
    }
}
