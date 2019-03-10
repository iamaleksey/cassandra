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
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;
import sun.nio.ch.DirectBuffer;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

abstract class FrameDecoder extends MessageToMessageDecoder<ByteBuf>
{
    enum IsSelfContained { YES, NO, NOT_SPECIFIED, CORRUPTED }

    static class Frame {}

    final static class IntactFrame extends Frame
    {
        final IsSelfContained isSelfContained;
        final ByteBuf contents;

        IntactFrame(IsSelfContained isSelfContained, ByteBuf contents)
        {
            this.isSelfContained = isSelfContained;
            this.contents = contents;
        }
    }

    final static class CorruptFrame extends Frame
    {
        final IsSelfContained isSelfContained;
        final int frameSize, readCRC, computedCRC;

        CorruptFrame(IsSelfContained isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            this.isSelfContained = isSelfContained;
            this.frameSize = frameSize;
            this.readCRC = readCRC;
            this.computedCRC = computedCRC;
        }

        static CorruptFrame recoverable(IsSelfContained isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            return new CorruptFrame(isSelfContained, frameSize, readCRC, computedCRC);
        }

        static CorruptFrame unrecoverable(int readCRC, int computedCRC)
        {
            return new CorruptFrame(IsSelfContained.CORRUPTED, Integer.MIN_VALUE, readCRC, computedCRC);
        }

        boolean isRecoverable()
        {
            return frameSize != Integer.MIN_VALUE;
        }
    }

    private ByteBuffer stash;

    abstract long readHeader(ByteBuffer in, int begin);
    abstract CorruptFrame verifyHeader(long header);
    abstract int frameLength(long header);
    abstract Frame unpackFrame(ByteBuf owner, ByteBuffer in, int begin, int end, long header);

    @Inline
    protected final void decode(ByteBuf nettyIn, int headerLength, List<Object> output)
    {
        ByteBuffer in = nettyIn.internalNioBuffer(nettyIn.readerIndex(), nettyIn.readableBytes());

        if (stash != null)
        {
            if (!copyToSize(in, stash, headerLength))
                return;

            long header = readHeader(stash, 0);
            CorruptFrame c = verifyHeader(header);
            if (c != null)
            {
                output.add(c);
                reset();
                return;
            }

            int frameLength = frameLength(header);
            stash = ensureCapacity(stash, frameLength);

            if (!copyToSize(in, stash, frameLength))
                return;

            try
            {
                output.add(unpackFrame(null, stash, 0, frameLength, header));
            }
            finally
            {
                reset();
            }
        }

        int begin = in.position();
        int limit = in.limit();
        while (begin < limit)
        {

            int remaining = limit - begin;
            if (remaining < headerLength)
            {
                stash(in, headerLength, begin, remaining);
                return;
            }

            long header = readHeader(in, begin);
            CorruptFrame c = verifyHeader(header);
            if (c != null)
            {
                output.add(c);
                return;
            }

            int frameLength = frameLength(header);
            if (remaining < frameLength)
            {
                stash(in, frameLength, begin, remaining);
                return;
            }

            int end = begin + frameLength;
            output.add(unpackFrame(nettyIn, in, begin, end, header));

            begin = end;
        }
    }

    @VisibleForTesting
    protected abstract void decode(ChannelHandlerContext ctx, ByteBuf nettyIn, List<Object> output);

    /**
     * Efficiently slice the {@code input} buffer into a ByteBuf with the given bounds.
     *
     * If the input buffer is the stash, we simply move ownership into a ByteBuf and return it.
     *
     * If the input buffer is sliced from a ByteBuf, we instead retain() a handle to the ByteBuf
     * and modify its bounds, returning this new ByteBuf.
     */
    protected ByteBuf slice(ByteBuf owner, ByteBuffer input, int begin, int end)
    {
        if (input == stash)
        {
            assert owner == null;
            input.position(begin);
            input.limit(end);
            ByteBuf result = BufferPoolAllocator.wrap(input);
            stash = null;
            return result;
        }
        else
        {
            return owner.retainedDuplicate()
                        .readerIndex(begin)
                        .writerIndex(end);
        }

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

    private void stash(ByteBuffer in, int stashLength, int begin, int length)
    {
        stash = BufferPool.get(stashLength, BufferType.OFF_HEAP);
        copyBytes(in, begin, stash, 0, length);
        stash.position(length);
    }

    private void reset()
    {
        ByteBuffer put = stash;
        if (put != null)
        {
            stash = null;
            BufferPool.put(put);
        }
    }

    public void handlerRemoved(ChannelHandlerContext ctx)
    {
        reset();
    }

    abstract void addLastTo(ChannelPipeline pipeline);
}
