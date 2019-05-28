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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Integer.reverseBytes;
import static java.lang.Math.min;

/**
 * LZ4 {@link FrameEncoder} implementation for compressed legacy (3.0, 3.11) connections.
 *
 * Netty's provided implementation - {@link io.netty.handler.codec.compression.Lz4FrameEncoder} couldn't be reused
 * for two reasons:
 *   1. It notifies flushes as successful when they may not be, by flushing an empty buffer ahead
 *      of the compressed buffer
 *   2. It has very poor performance when coupled with xxhash, which we use for legacy connections -
 *      allocating a single-byte array and making a JNI call <em>for every byte of the payload</em>
 */
@ChannelHandler.Sharable
class FrameEncoderLegacyLZ4 extends FrameEncoder
{
    static final FrameEncoderLegacyLZ4 instance =
        new FrameEncoderLegacyLZ4(XXHashFactory.fastestInstance().hash32(),
                                  LZ4Factory.fastestInstance().fastCompressor());

    private final XXHash32 xxhash;
    private final LZ4Compressor compressor;

    private FrameEncoderLegacyLZ4(XXHash32 xxhash, LZ4Compressor compressor)
    {
        this.xxhash = xxhash;
        this.compressor = compressor;
    }

    private static final int MAX_BLOCK_LENGTH = 1 << 15;

    @Override
    ByteBuf encode(boolean isSelfContained, ByteBuffer payload)
    {
        ByteBuffer frame = null;
        try
        {
            frame = BufferPool.getAtLeast(calculateMaxFrameLength(payload), BufferType.OFF_HEAP);

            int   frameOffset = 0;
            int payloadOffset = 0;

            int payloadLength = payload.remaining();
            while (payloadOffset < payloadLength)
            {
                int blockLength = min(MAX_BLOCK_LENGTH, payloadLength - payloadOffset);
                frameOffset += compressBlock(frame, frameOffset, payload, payloadOffset, blockLength);
                payloadOffset += blockLength;
            }

            frame.limit(frameOffset);
            BufferPool.putUnusedPortion(frame, false);

            return GlobalBufferPoolAllocator.wrap(frame);
        }
        catch (Throwable t)
        {
            if (null != frame)
                BufferPool.put(frame, false);
            throw t;
        }
        finally
        {
            BufferPool.put(payload, false);
        }
    }

    private static final int LEGACY_LZ4_HASH_SEED = 0x9747B28C;

    private int compressBlock(ByteBuffer frame, int frameOffset, ByteBuffer payload, int payloadOffset, int blockLength)
    {
        int frameBytesRemaining = frame.limit() - (frameOffset + HEADER_LENGTH);
        int compressedLength = compressor.compress(payload, payloadOffset, blockLength, frame, frameOffset + HEADER_LENGTH, frameBytesRemaining);
        if (compressedLength >= blockLength)
        {
            ByteBufferUtil.copyBytes(payload, payloadOffset, frame, frameOffset + HEADER_LENGTH, blockLength);
            compressedLength = blockLength;
        }
        int checksum = xxhash.hash(payload, payloadOffset, blockLength, LEGACY_LZ4_HASH_SEED) & 0xFFFFFFF;
        writeHeader(frame, frameOffset, compressedLength, blockLength, checksum);
        return HEADER_LENGTH + compressedLength;
    }

    private static final long MAGIC_NUMBER =
        (long) 'L' << 56 | (long) 'Z' << 48 | (long) '4' << 40 | (long) 'B' << 32 | 'l' << 24 | 'o' << 16 | 'c' << 8  | 'k';

    private static final byte TOKEN_NON_COMPRESSED = 0x15;
    private static final byte TOKEN_COMPRESSED     = 0x25;

    // magic number, token, compressed length, uncompressed length, checksum
    private static final int HEADER_LENGTH = 8 + 1 + 4 + 4 + 4;

    private static void writeHeader(ByteBuffer frame, int frameOffset, int compressedLength, int uncompressedLength, int checksum)
    {
        byte token = compressedLength == uncompressedLength
                   ? TOKEN_NON_COMPRESSED
                   : TOKEN_COMPRESSED;

        //noinspection PointlessArithmeticExpression
        frame.putLong(frameOffset + 0,  MAGIC_NUMBER                    );
        frame.put    (frameOffset + 8,  token                           );
        frame.putInt (frameOffset + 9,  reverseBytes(compressedLength)  );
        frame.putInt (frameOffset + 13, reverseBytes(uncompressedLength));
        frame.putInt (frameOffset + 17, reverseBytes(checksum)          );
    }

    private int calculateMaxFrameLength(ByteBuffer payload)
    {
        int payloadLength = payload.remaining();
        int blockCount = payloadLength / MAX_BLOCK_LENGTH + (payloadLength % MAX_BLOCK_LENGTH != 0 ? 1 : 0);
        return compressor.maxCompressedLength(payloadLength) + HEADER_LENGTH * blockCount;
    }
}
