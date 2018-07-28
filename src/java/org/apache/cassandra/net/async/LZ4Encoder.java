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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.net.async.NettyFactory.crc24;
import static org.apache.cassandra.net.async.NettyFactory.crc32;

@ChannelHandler.Sharable
public class LZ4Encoder extends MessageToMessageEncoder<ByteBuf>
{
    public static final LZ4Encoder fastInstance = new LZ4Encoder(LZ4Factory.fastestInstance().fastCompressor());

    private final LZ4Compressor compressor;

    public LZ4Encoder(LZ4Compressor compressor)
    {
        this.compressor = compressor;
    }

    private static final int HEADER_LENGTH = 8;
    private static final int HEADER_AND_TRAILER_LENGTH = 12;

    private static long encodeHeaderWithoutCrc(long compressedLength, long uncompressedLength)
    {
        return compressedLength | (uncompressedLength << 20);
    }

    private static void writeHeaderWithCrc(ByteBuffer frame, long header5b, long crc)
    {
        long header8b = header5b | (crc << 40);;
        if (frame.order() == ByteOrder.BIG_ENDIAN)
            header8b = Long.reverseBytes(header8b);
        frame.putLong(0, header8b);
    }

    public ByteBuffer encode(ByteBuffer in)
    {
        int uncompressedLength = in.remaining();
        if (uncompressedLength >= 1 << 20)
            throw new IllegalArgumentException("Maximum uncompressed payload size is 1MiB");

        int maxOutputLength = compressor.maxCompressedLength(uncompressedLength);
        ByteBuffer frame = BufferPool.get(HEADER_AND_TRAILER_LENGTH + maxOutputLength, BufferType.OFF_HEAP);
        try
        {
            int compressedLength = compressor.compress(in, in.position(), uncompressedLength, frame, HEADER_LENGTH, maxOutputLength);

            if (compressedLength >= uncompressedLength)
            {
                ByteBufferUtil.copyBytes(in, in.position(), frame, HEADER_LENGTH, uncompressedLength);
                compressedLength = uncompressedLength;
                uncompressedLength = 0;
            }

            long header5b = encodeHeaderWithoutCrc(compressedLength, uncompressedLength);
            writeHeaderWithCrc(frame, header5b, crc24(header5b, 5));

            CRC32 crc = crc32();
            frame.position(HEADER_LENGTH);
            frame.limit(compressedLength + HEADER_LENGTH);
            crc.update(frame);

            int frameCrc = (int) crc.getValue();
            if (frame.order() == ByteOrder.BIG_ENDIAN)
                frameCrc = Integer.reverseBytes(frameCrc);
            int frameLength = compressedLength + HEADER_AND_TRAILER_LENGTH;

            frame.limit(frameLength);
            frame.putInt(frameCrc);
            frame.position(0);

            BufferPool.putUnusedPortion(frame);
            return frame;
        }
        catch (Throwable t)
        {
            BufferPool.put(frame);
            throw t;
        }
    }

    protected void encode(ChannelHandlerContext ctx, ByteBuf input, List<Object> output)
    {
        if (!input.isReadable())
        {
            output.add(Unpooled.EMPTY_BUFFER);
            return;
        }

        ByteBuffer in = input.internalNioBuffer(input.readerIndex(), input.readableBytes());
        ByteBuffer out = encode(in);
        output.add(BufferPoolAllocator.wrapUnshared(out));
    }
}
