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
import io.netty.channel.ChannelPipeline;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Integer.reverseBytes;
import static java.lang.Math.min;

@ChannelHandler.Sharable
class FrameEncoderLegacyLZ4 extends FrameEncoder
{
    static final FrameEncoderLegacyLZ4 instance =
        new FrameEncoderLegacyLZ4(XXHashFactory.fastestInstance().hash32(),
                                  LZ4Factory.fastestInstance().fastCompressor());

    // magic number of LZ4 block
    private static final long MAGIC_NUMBER =
        (long) 'L' << 56 | (long) 'Z' << 48 | (long) '4' << 40 | (long) 'B' << 32 | 'l' << 24 | 'o' << 16 | 'c' << 8  | 'k';

    // full length of LZ4 block header
    private static final int HEADER_LENGTH = 8  // magic number
                                           + 1  // token
                                           + 4  // compressed length
                                           + 4  // uncompressed length
                                           + 4; // checksum

    private static final int MAGIC_NUMBER_OFFSET        = 0;
    private static final int TOKEN_OFFSET               = 8;
    private static final int COMPRESSED_LENGTH_OFFSET   = 9;
    private static final int UNCOMPRESSED_LENGTH_OFFSET = 13;
    private static final int CHECKSUM_OFFSET            = 17;

    private static final  int MAX_BLOCK_LENGTH     = 1  << 15;
    private static final byte TOKEN_NON_COMPRESSED = 0x10 | 5;
    private static final byte TOKEN_COMPRESSED     = 0x20 | 5;

    private static final int LEGACY_LZ4_HASH_SEED = 0x9747B28C;

    private final XXHash32 xxhash;
    private final LZ4Compressor compressor;

    private FrameEncoderLegacyLZ4(XXHash32 xxhash, LZ4Compressor compressor)
    {
        this.xxhash = xxhash;
        this.compressor = compressor;
    }

    private static void writeHeader(ByteBuffer frame, int frameOffset, int compressedLength, int uncompressedLength, int checksum)
    {
        byte token = compressedLength == uncompressedLength
                   ? TOKEN_NON_COMPRESSED
                   : TOKEN_COMPRESSED;

        frame.putLong(frameOffset + MAGIC_NUMBER_OFFSET,        MAGIC_NUMBER                    );
        frame.put    (frameOffset + TOKEN_OFFSET,               token                           );
        frame.putInt (frameOffset + COMPRESSED_LENGTH_OFFSET,   reverseBytes(compressedLength)  );
        frame.putInt (frameOffset + UNCOMPRESSED_LENGTH_OFFSET, reverseBytes(uncompressedLength));
        frame.putInt (frameOffset + CHECKSUM_OFFSET,            reverseBytes(checksum)          );
    }

    @Override
    ByteBuf encode(boolean isSelfContained, ByteBuffer payload)
    {
        ByteBuffer frame = null;
        try
        {
            frame = BufferPool.getAtLeast(calculateMaxFrameLength(payload), BufferType.OFF_HEAP);

            int frameOffset   = 0;
            int payloadOffset = 0;

            int payloadLength = payload.remaining();
            while (payloadOffset < payloadLength)
            {
                int blockLength = min(MAX_BLOCK_LENGTH, payloadLength - payloadOffset);
                frameOffset   += compressBlock(frame, frameOffset, payload, payloadOffset, blockLength);
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

    private int compressBlock(ByteBuffer frame, int frameOffset, ByteBuffer payload, int payloadOffset, int blockLength)
    {
        int maxCompressedLength = compressor.maxCompressedLength(blockLength);
        int    compressedLength = compressor.compress(payload, payloadOffset, blockLength, frame, frameOffset + HEADER_LENGTH, maxCompressedLength);
        if (compressedLength >= blockLength)
        {
            ByteBufferUtil.copyBytes(payload, payloadOffset, frame, frameOffset + HEADER_LENGTH, blockLength);
            compressedLength = blockLength;
        }
        int checksum = xxhash.hash(payload, payloadOffset, blockLength, LEGACY_LZ4_HASH_SEED) & 0xFFFFFFF;
        writeHeader(frame, frameOffset, compressedLength, blockLength, checksum);
        return HEADER_LENGTH + compressedLength;
    }

    private int calculateMaxFrameLength(ByteBuffer payload)
    {
        int payloadLength = payload.remaining();

        int quotient  = payloadLength / MAX_BLOCK_LENGTH;
        int remainder = payloadLength - MAX_BLOCK_LENGTH * quotient;

        int maxLength = (HEADER_LENGTH + compressor.maxCompressedLength(MAX_BLOCK_LENGTH)) * quotient;
        if (remainder != 0)
            maxLength += HEADER_LENGTH + compressor.maxCompressedLength(remainder);
        return maxLength;
    }

    @Override
    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameEncoderLegacyLZ4", this);
    }
}
