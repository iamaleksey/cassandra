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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

import static org.apache.cassandra.net.async.Crc.crc24;
import static org.apache.cassandra.net.async.Crc.updateCrc32;

public final class FrameDecoderCrc extends FrameDecoder
{
    public static FrameDecoderCrc create()
    {
        return new FrameDecoderCrc();
    }

    private static final int HEADER_LENGTH = 6;
    private static final int TRAILER_LENGTH = 4;
    private static final int HEADER_AND_TRAILER_LENGTH = 10;

    private static boolean isSelfContained(long header6b)
    {
        return 0 != (header6b & (1L << 17));
    }

    private static int payloadLength(long header6b)
    {
        return ((int) header6b) & 0x1FFFF;
    }

    private static int headerCrc(long header6b)
    {
        return ((int) (header6b >>> 24)) & 0xFFFFFF;
    }

    final long readHeader(ByteBuffer frame, int begin)
    {
        long header6b;
        if (frame.limit() - begin >= 8)
        {
            header6b = frame.getLong(begin);
            if (frame.order() == ByteOrder.BIG_ENDIAN)
                header6b = Long.reverseBytes(header6b);
            header6b &= 0xffffffffffffL;
        }
        else
        {
            header6b = 0;
            for (int i = 0 ; i < HEADER_LENGTH ; ++i)
                header6b |= (0xffL & frame.get(begin + i)) << (8 * i);
        }
        return header6b;
    }

    final CorruptFrame verifyHeader(long header6b)
    {
        int computeLengthCrc = crc24(header6b, 3);
        int readLengthCrc = headerCrc(header6b);

        return readLengthCrc == computeLengthCrc ? null : CorruptFrame.unrecoverable(readLengthCrc, computeLengthCrc);
    }

    final int frameLength(long header6b)
    {
        return payloadLength(header6b) + HEADER_AND_TRAILER_LENGTH;
    }

    final Frame unpackFrame(Slice slice, int begin, int end, long header6b)
    {
        ByteBuffer in = slice.contents;
        boolean isSelfContained = isSelfContained(header6b);

        CRC32 crc = Crc.crc32();
        int readFullCrc = in.getInt(end - TRAILER_LENGTH);
        if (in.order() == ByteOrder.BIG_ENDIAN)
            readFullCrc = Integer.reverseBytes(readFullCrc);

        updateCrc32(crc, in, begin + HEADER_LENGTH, end - TRAILER_LENGTH);
        int computeFullCrc = (int) crc.getValue();

        if (readFullCrc != computeFullCrc)
            return CorruptFrame.recoverable(isSelfContained, (end - begin) - HEADER_AND_TRAILER_LENGTH, readFullCrc, computeFullCrc);

        return new IntactFrame(isSelfContained, sliceIfRemaining(slice, begin + HEADER_LENGTH, end - TRAILER_LENGTH));
    }

    void decode(ChannelHandlerContext ctx, Slice slice, List<Object> output)
    {
        decode(slice, HEADER_LENGTH, output);
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderCrc", this);
    }
}
