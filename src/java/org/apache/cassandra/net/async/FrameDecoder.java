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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

// TODO: extend ChannelInboundHandlerAdapter
abstract class FrameDecoder extends MessageToMessageDecoder<ByteBuf>
{
    enum IsSelfContained { YES, NO, NOT_SPECIFIED, CORRUPTED }

    private static class SharedByteBuffer
    {
        final ByteBuffer buffer;
        private volatile int owners = UNSHARED;

        private static final int UNSHARED = -1;
        private static final int RELEASED = 0;
        private static final AtomicIntegerFieldUpdater<SharedByteBuffer> ownersUpdater = AtomicIntegerFieldUpdater.newUpdater(SharedByteBuffer.class, "owners");

        public SharedByteBuffer(ByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        public void release()
        {
            if (owners == UNSHARED || RELEASED == ownersUpdater.decrementAndGet(this))
            {
                ownersUpdater.lazySet(this, RELEASED);
                BufferPool.put(buffer);
            }
        }

        public boolean tryAdopt()
        {
            if (owners == UNSHARED || RELEASED == ownersUpdater.updateAndGet(this, v -> v == 1 ? RELEASED : v))
            {
                ownersUpdater.lazySet(this, RELEASED);
                return true;
            }
            return false;
        }

        public void retain()
        {
            if (owners == UNSHARED)
            {
                owners = 2;
            }
            else
            {
                ownersUpdater.updateAndGet(this, v -> {
                    if (v == RELEASED)
                        throw new IllegalStateException("Attempted to reference an already released SharedByteBuffer");
                    return v + 1;
                });
            }
        }
    }

    static class Slice
    {
        public static final Slice EMPTY = new Slice(null, ByteBufferUtil.EMPTY_BYTE_BUFFER)
        {
            void retain() {}
            void release() {}
        };

        final SharedByteBuffer owner;
        final ByteBuffer contents;

        Slice(SharedByteBuffer owner, ByteBuffer contents)
        {
            this.owner = owner;
            this.contents = contents;
        }

        boolean isReadable()
        {
            return contents.hasRemaining();
        }

        int readableBytes()
        {
            return contents.remaining();
        }

        void skipBytes(int skipBytes)
        {
            contents.position(contents.position() + skipBytes);
        }

        void retain()
        {
            owner.retain();
        }

        void release()
        {
            owner.release();
        }

        ByteBuffer tryAdopt()
        {
            if (owner.buffer == contents && owner.tryAdopt())
                return owner.buffer;
            return null;
        }

        /**
         * Create a slice over the next {@code length} bytes, and consume them from our buffer
         */
        Slice sliceAndConsume(int length)
        {
            int begin = contents.position();
            int end = begin + length;
            Slice result = slice(begin, end);
            contents.position(end);
            return result;
        }

        Slice slice(int begin, int end)
        {
            ByteBuffer slice = contents.duplicate();
            slice.position(begin).limit(end);
            retain();
            return new Slice(owner, slice);
        }

        static Slice wrap(ByteBuffer buffer)
        {
            return new Slice(new SharedByteBuffer(buffer), buffer);
        }
    }

    static class Frame
    {
        static void release(Object msg)
        {
            if (msg instanceof IntactFrame)
                ((IntactFrame) msg).contents.release();
        }
    }

    final static class IntactFrame extends Frame
    {
        final IsSelfContained isSelfContained;
        final Slice contents;

        IntactFrame(IsSelfContained isSelfContained, Slice contents)
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
    abstract Frame unpackFrame(Slice slice, int begin, int end, long header);

    public static Slice sliceIfRemaining(Slice slice, int begin, int end)
    {
        if (end == slice.contents.limit())
        {
            slice.contents.position(begin);
            return slice;
        }

        return slice.slice(begin, end);
    }

    @Inline
    protected final void decode(Slice slice, int headerLength, List<Object> output)
    {
        ByteBuffer in = slice.contents;
        try
        {
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

                stash.flip();
                Slice stashed = Slice.wrap(stash);
                stash = null;
                try
                {
                    output.add(unpackFrame(stashed, 0, frameLength, header));
                }
                catch (Throwable t)
                {
                    stashed.release();
                    throw t;
                }
            }

            int begin = in.position();
            int limit = in.limit();
            while (begin < limit)
            {
                int remaining = limit - begin;
                if (remaining < headerLength)
                {
                    stash(slice, headerLength, begin, remaining);
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
                    stash(slice, frameLength, begin, remaining);
                    return;
                }

                int end = begin + frameLength;
                output.add(unpackFrame(slice, begin, end, header));

                begin = end;
            }
        }
        catch (Throwable t)
        {
            for (Object object : output)
                ((Slice) object).release();
            slice.release();
            throw t;
        }
    }

    abstract void decode(ChannelHandlerContext ctx, Slice slice, List<Object> output);

    @VisibleForTesting
    protected void decode(ChannelHandlerContext ctx, ByteBuf nettyIn, List<Object> output)
    {
        Slice slice = Slice.wrap(((BufferPoolAllocator.Wrapped) nettyIn).adopt());
        decode(ctx, slice, output);
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

    private void stash(Slice in, int stashLength, int begin, int length)
    {
        try
        {
            stash = BufferPool.get(stashLength, BufferType.OFF_HEAP);
            copyBytes(in.contents, begin, stash, 0, length);
            stash.position(length);
        }
        finally
        {
            in.release();
        }
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
