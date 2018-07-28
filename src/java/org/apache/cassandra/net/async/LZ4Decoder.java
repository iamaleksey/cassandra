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
import io.netty.handler.codec.MessageToMessageDecoder;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.net.async.NettyFactory.crc24;
import static org.apache.cassandra.net.async.NettyFactory.crc32;
import static org.apache.cassandra.net.async.NettyFactory.updateCrc32;
import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

public class LZ4Decoder extends MessageToMessageDecoder<ByteBuf>
{
    private static final int HEADER_LENGTH = 8;
    private static final int TRAILER_LENGTH = 4;
    private static final int HEADER_AND_TRAILER_LENGTH = 12;

    static final class Corruption
    {
        final int frameSize, readCRC, computedCRC;

        Corruption(int frameSize, int readCRC, int computedCRC)
        {
            this.frameSize = frameSize;
            this.readCRC = readCRC;
            this.computedCRC = computedCRC;
        }

        private static Corruption recoverable(int frameSize, int readCRC, int computedCRC)
        {
            return new Corruption(frameSize, readCRC, computedCRC);
        }

        private static Corruption unrecoverable(int readCRC, int computedCRC)
        {
            return new Corruption(Integer.MIN_VALUE, readCRC, computedCRC);
        }

        boolean isRecoverable()
        {
            return frameSize != Integer.MIN_VALUE;
        }
    }

    private final LZ4FastDecompressor decompressor;

    private ByteBuffer stash;

    public LZ4Decoder(LZ4FastDecompressor decompressor)
    {
        this.decompressor = decompressor;
    }

    public static LZ4Decoder fast()
    {
        return new LZ4Decoder(LZ4Factory.fastestInstance().fastDecompressor());
    }

    private static boolean copyToSize(ByteBuffer in, ByteBuffer out, int toOutPosition)
    {
        int bytesToSize = toOutPosition - out.position();
        if (bytesToSize <= 0)
            return true;

        if (bytesToSize > in.remaining())
        {
            out.put(in);
            return false;
        }
        copyBytes(in, in.position(), out, out.position(), bytesToSize);
        in.position(in.position() + bytesToSize);
        out.position(toOutPosition);
        return true;
    }

    private static ByteBuffer ensureCapacity(ByteBuffer buffer, int capacity)
    {
        if (buffer.capacity() >= capacity)
            return buffer;

        ByteBuffer newBuffer = BufferPool.get(capacity, BufferType.OFF_HEAP);
        buffer.flip();
        newBuffer.put(buffer);
        BufferPool.put(buffer);
        return newBuffer;
    }

    private long readHeader(ByteBuffer frame, int begin)
    {
        long header8b = frame.getLong(begin);
        if (frame.order() == ByteOrder.BIG_ENDIAN)
            header8b = Long.reverseBytes(header8b);
        return header8b;
    }
    private Corruption verifyHeader(long header8b)
    {
        int computeLengthCrc = crc24(header8b, 5);
        int readLengthCrc = lengthCrc(header8b);

        return readLengthCrc == computeLengthCrc ? null : Corruption.unrecoverable(readLengthCrc, computeLengthCrc);
    }

    private static int compressedLength(long header8b)
    {
        return ((int) header8b) & 0xFFFFF;
    }

    private static int uncompressedLength(long header8b)
    {
        return ((int) (header8b >> 20)) & 0xFFFFF;
    }

    private static int lengthCrc(long header8b)
    {
        return ((int) (header8b >> 40)) & 0xFFFFFF;
    }

    private Object decompress(ByteBuffer frame, int begin, int end, int uncompressedLength)
    {
        CRC32 crc = crc32();
        ByteBuffer out = null;
        try
        {
            int readFullCrc = frame.getInt(end - TRAILER_LENGTH);
            if (frame.order() == ByteOrder.BIG_ENDIAN)
                readFullCrc = Integer.reverseBytes(readFullCrc);

            updateCrc32(crc, frame, begin + HEADER_LENGTH, end - TRAILER_LENGTH);
            int computeFullCrc = (int) crc.getValue();

            if (readFullCrc != computeFullCrc)
                return Corruption.recoverable(uncompressedLength, readFullCrc, computeFullCrc);

            if (uncompressedLength == 0)
            {
                int compressedLength = end - begin - HEADER_AND_TRAILER_LENGTH;
                out = BufferPool.get(compressedLength, BufferType.OFF_HEAP);
                ByteBufferUtil.copyBytes(frame, begin + HEADER_LENGTH, out, 0, compressedLength);
            }
            else
            {
                out = BufferPool.get(uncompressedLength, BufferType.OFF_HEAP);
                decompressor.decompress(frame, begin + HEADER_LENGTH, out, 0, uncompressedLength);
            }
            return BufferPoolAllocator.wrap(out);
        }
        catch (Throwable t)
        {
            if (out != null)
                BufferPool.put(out);
            throw t;
        }
    }

    private void stash(ByteBuffer in, int stashLength, int begin, int length)
    {
        stash = BufferPool.get(stashLength, BufferType.OFF_HEAP);
        copyBytes(in, begin, stash, 0, length);
        stash.position(length);
    }

    protected void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> output)
    {
        ByteBuffer in = input.internalNioBuffer(input.readerIndex(), input.readableBytes());

        if (stash != null)
        {
            if (!copyToSize(in, stash, HEADER_LENGTH))
                return;

            long header8b = readHeader(stash, 0);
            Corruption c = verifyHeader(header8b);
            if (c != null)
            {
                output.add(c);
                BufferPool.put(stash);
                stash = null;
                return;
            }

            int compressedLength = compressedLength(header8b);
            int frameLength = compressedLength + HEADER_AND_TRAILER_LENGTH;
            stash = ensureCapacity(stash, frameLength);

            if (!copyToSize(in, stash, frameLength))
                return;

            ByteBuffer buffer = stash;
            stash = null;
            try
            {
                buffer.flip();
                output.add(decompress(buffer, 0, frameLength, uncompressedLength(header8b)));
            }
            finally
            {
                BufferPool.put(buffer);
            }
        }

        int begin = in.position();
        int limit = in.limit();
        while (begin < limit)
        {

            int remaining = limit - begin;
            if (remaining < HEADER_LENGTH)
            {
                stash(in, HEADER_LENGTH, begin, remaining);
                return;
            }

            long header8b = readHeader(in, begin);
            Corruption c = verifyHeader(header8b);
            if (c != null)
            {
                output.add(c);
                return;
            }

            int compressedLength = compressedLength(header8b);
            int frameLength = compressedLength + HEADER_AND_TRAILER_LENGTH;
            if (remaining < frameLength)
            {
                stash(in, frameLength, begin, remaining);
                return;
            }

            int end = begin + frameLength;
            output.add(decompress(in, begin, end, uncompressedLength(header8b)));

            begin = end;
        }
    }

    public void handlerRemoved(ChannelHandlerContext ctx)
    {
        if (stash != null)
            BufferPool.put(stash);
    }

}
