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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

abstract class FrameDecoder extends ChannelInboundHandlerAdapter
{
    /**
     * A wrapper for possibly sharing portions of a single, BufferPool managed, ByteBuffer;
     * optimised for the case where no sharing is necessary
     */
    static class SharedBytes
    {
        public static final SharedBytes EMPTY = new SharedBytes(ByteBufferUtil.EMPTY_BYTE_BUFFER)
        {
            void retain() {}
            void release() {}
        };

        private final ByteBuffer bytes;
        private final SharedBytes parent;
        private volatile int owners;

        private static final int OWNED_BY_PARENT = Integer.MIN_VALUE;
        private static final int UNSHARED = -1;
        private static final int RELEASED = 0;
        private static final AtomicIntegerFieldUpdater<SharedBytes> ownersUpdater = AtomicIntegerFieldUpdater.newUpdater(SharedBytes.class, "owners");

        SharedBytes(ByteBuffer bytes)
        {
            this.owners = UNSHARED;
            this.parent = this;
            this.bytes = bytes;
        }

        SharedBytes(SharedBytes parent, ByteBuffer bytes)
        {
            this.owners = OWNED_BY_PARENT;
            this.parent = parent;
            this.bytes = bytes;
        }

        ByteBuffer get()
        {
            return bytes;
        }

        boolean isReadable()
        {
            return bytes.hasRemaining();
        }

        int readableBytes()
        {
            return bytes.remaining();
        }

        void skipBytes(int skipBytes)
        {
            bytes.position(bytes.position() + skipBytes);
        }

        private SharedBytes owner()
        {
            return parent == null ? this : parent;
        }

        void retain()
        {
            SharedBytes owner = owner();
            int owners = owner.owners;
            if (owners == UNSHARED)
            {
                owner.owners = 2;
            }
            else
            {
                ownersUpdater.updateAndGet(owner, v -> {
                    if (v == RELEASED)
                        throw new IllegalStateException("Attempted to reference an already released SharedByteBuffer");
                    return v + 1;
                });
            }
        }

        void release()
        {
            SharedBytes owner = owner();
            int owners = owner.owners;

            if (owners == UNSHARED || RELEASED == ownersUpdater.decrementAndGet(owner))
            {
                if (owners == UNSHARED)
                    ownersUpdater.lazySet(owner, RELEASED);
                BufferPool.put(bytes, false);
            }
        }

        /**
         * Create a slice over the next {@code length} bytes, and consume them from our buffer
         */
        SharedBytes sliceAndConsume(int length)
        {
            int begin = bytes.position();
            int end = begin + length;
            SharedBytes result = slice(begin, end);
            bytes.position(end);
            return result;
        }

        /**
         * Create a new slice, incrementing the number of owners (making it shared if it was previously unshared)
         */
        SharedBytes slice(int begin, int end)
        {
            ByteBuffer slice = bytes.duplicate();
            slice.position(begin).limit(end);
            retain();
            return new SharedBytes(owner(), slice);
        }

        static SharedBytes wrap(ByteBuffer buffer)
        {
            return new SharedBytes(buffer);
        }
    }

    static class Frame
    {
        /**
         * If the provided Object is a Frame, release any associated resources it owns
         */
        static void release(Object msg)
        {
            if (msg instanceof IntactFrame)
                ((IntactFrame) msg).contents.release();
        }
    }

    final static class IntactFrame extends Frame
    {
        final boolean isSelfContained;
        final SharedBytes contents;

        IntactFrame(boolean isSelfContained, SharedBytes contents)
        {
            this.isSelfContained = isSelfContained;
            this.contents = contents;
        }
    }

    final static class CorruptFrame extends Frame
    {
        final boolean isSelfContained;
        final int frameSize, readCRC, computedCRC;

        CorruptFrame(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            this.isSelfContained = isSelfContained;
            this.frameSize = frameSize;
            this.readCRC = readCRC;
            this.computedCRC = computedCRC;
        }

        static CorruptFrame recoverable(boolean isSelfContained, int frameSize, int readCRC, int computedCRC)
        {
            return new CorruptFrame(isSelfContained, frameSize, readCRC, computedCRC);
        }

        static CorruptFrame unrecoverable(int readCRC, int computedCRC)
        {
            return new CorruptFrame(false, Integer.MIN_VALUE, readCRC, computedCRC);
        }

        boolean isRecoverable()
        {
            return frameSize != Integer.MIN_VALUE;
        }
    }

    ByteBuffer stash;
    private ChannelHandlerContext ctx;
    private Consumer<Frame> consumer;

    abstract long readHeader(ByteBuffer in, int begin);
    abstract CorruptFrame verifyHeader(long header);
    abstract int frameLength(long header);

    /**
     * Extract a frame known to cover the given range.
     * If {@code transferOwnership}, the method is responsible for ensuring bytes.release() is invoked at some future point.
     */
    abstract Frame unpackFrame(SharedBytes bytes, int begin, int end, long header, boolean transferOwnership);

    /**
     * If the new logical slice would reach the end of the provided slice, cannibalise it
     */
    public static SharedBytes slice(SharedBytes bytes, int sliceBegin, int sliceEnd, boolean transferOwnership)
    {
        if (transferOwnership)
        {
            bytes.get().position(sliceBegin)
                       .limit(sliceEnd);
            return bytes;
        }

        return bytes.slice(sliceBegin, sliceEnd);
    }

    @Inline
    protected final void decode(Consumer<Frame> consumer, SharedBytes bytes, int headerLength)
    {
        ByteBuffer in = bytes.get();

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
                    reset();
                    consumer.accept(c);
                    return;
                }

                int frameLength = frameLength(header);
                stash = ensureCapacity(stash, frameLength);

                if (!copyToSize(in, stash, frameLength))
                    return;

                stash.flip();
                SharedBytes stashed = SharedBytes.wrap(stash);
                stash = null;

                Frame frame;
                try
                {
                    frame = unpackFrame(stashed, 0, frameLength, header, true);
                }
                catch (Throwable t)
                {
                    stashed.release();
                    throw t;
                }
                consumer.accept(frame);
            }

            int begin = in.position();
            int limit = in.limit();
            while (begin < limit)
            {
                int remaining = limit - begin;
                if (remaining < headerLength)
                {
                    stash(bytes, headerLength, begin, remaining);
                    return;
                }

                long header = readHeader(in, begin);
                CorruptFrame c = verifyHeader(header);
                if (c != null)
                {
                    consumer.accept(c);
                    return;
                }

                int frameLength = frameLength(header);
                if (remaining < frameLength)
                {
                    stash(bytes, frameLength, begin, remaining);
                    return;
                }

                int end = begin + frameLength;
                SharedBytes unpack = bytes;
                if (end == limit)
                    bytes = null;
                Frame frame = unpackFrame(unpack, begin, end, header, end == limit);

                consumer.accept(frame);
                begin = end;
            }
        }
        catch (Throwable t)
        {
            if (bytes != null)
                bytes.release();
            throw t;
        }
    }

    public void handlerAdded(ChannelHandlerContext ctx)
    {
        this.ctx = ctx;
        this.consumer = ctx::fireChannelRead;
    }

    public void channelUnregistered(ChannelHandlerContext ctx)
    {
        this.ctx = null;
        this.consumer = null;
    }

    abstract void decode(Consumer<Frame> consumer, SharedBytes bytes);

    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        assert ctx == this.ctx;
        decode(consumer, SharedBytes.wrap(((BufferPoolAllocator.Wrapped) msg).adopt()));
    }

    // visible only for legacy/none decode
    static boolean copyToSize(ByteBuffer in, ByteBuffer out, int toOutPosition)
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

    // visible only for legacy/none decode
    static ByteBuffer ensureCapacity(ByteBuffer buffer, int capacity)
    {
        if (buffer.capacity() >= capacity)
            return buffer;

        ByteBuffer newBuffer = BufferPool.get(capacity, BufferType.OFF_HEAP);
        buffer.flip();
        newBuffer.put(buffer);
        BufferPool.put(buffer, false);
        return newBuffer;
    }

    ByteBuffer unstash()
    {
        ByteBuffer result = stash;
        stash = null;
        return result;
    }

    // visible only for legacy/none decode
    void stash(SharedBytes in, int stashLength, int begin, int length)
    {
        stash = BufferPool.get(stashLength, BufferType.OFF_HEAP);
        copyBytes(in.get(), begin, stash, 0, length);
        stash.position(length);
    }

    // visible only for legacy/none decode
    void reset()
    {
        ByteBuffer put = stash;
        if (put != null)
        {
            stash = null;
            BufferPool.put(put, false);
        }
    }

    public void handlerRemoved(ChannelHandlerContext ctx)
    {
        reset();
    }

    abstract void addLastTo(ChannelPipeline pipeline);
}
