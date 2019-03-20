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
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

abstract class FrameDecoder extends ChannelInboundHandlerAdapter implements InboundMessageHandler.Button
{
    /**
     * A wrapper for possibly sharing portions of a single, BufferPool managed, ByteBuffer;
     * optimised for the case where no sharing is necessary
     */
    static class SharedBytes
    {
        public static final SharedBytes EMPTY = new SharedBytes(ByteBufferUtil.EMPTY_BYTE_BUFFER)
        {
            SharedBytes retain() { return EMPTY; }
            void release() {}
        };

        private final ByteBuffer bytes;
        private final SharedBytes owner;
        private volatile int count;

        private static final int UNSHARED = -1;
        private static final int RELEASED = 0;
        private static final AtomicIntegerFieldUpdater<SharedBytes> countUpdater = AtomicIntegerFieldUpdater.newUpdater(SharedBytes.class, "count");

        SharedBytes(ByteBuffer bytes)
        {
            this.count = UNSHARED;
            this.owner = this;
            this.bytes = bytes;
        }

        SharedBytes(SharedBytes owner, ByteBuffer bytes)
        {
            this.owner = owner;
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

        /**
         * Ensure this SharedBytes will use atomic operations for updating its count from now on.
         * The first invocation must occur while the calling thread has exclusive access (though there may be more
         * than one 'owner', these must all either be owned by the calling thread or otherwise not being used)
         */
        SharedBytes atomic()
        {
            int count = owner.count;
            if (count < 0)
                owner.count = -count;
            return this;
        }

        SharedBytes retain()
        {
            owner.doRetain();
            return this;
        }

        private void doRetain()
        {
            int count = this.count;
            if (count < 0)
            {
                countUpdater.lazySet(this, count - 1);
            }
            else
            {
                while (true)
                {
                    if (count == RELEASED)
                        throw new IllegalStateException("Attempted to reference an already released SharedByteBuffer");

                    if (countUpdater.compareAndSet(this, count, count + 1))
                        return;

                    count = this.count;
                }
            }
        }

        void release()
        {
            owner.doRelease();
        }

        private void doRelease()
        {
            int count = this.count;

            if (count < 0)
                countUpdater.lazySet(this, count += 1);
            else if (count > 0)
                count = countUpdater.decrementAndGet(this);
            else
                throw new IllegalStateException("Already released");

            if (count == RELEASED)
                BufferPool.put(bytes, false);
        }

        /**
         * Create a slice over the next {@code length} bytes, and consume them from our buffer
         *
         * Does NOT increase the retention count; may need to be prefixed by a call to retain
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
            return new SharedBytes(owner.retain(), slice);
        }

        static SharedBytes wrap(ByteBuffer buffer)
        {
            return new SharedBytes(buffer);
        }
    }

    abstract static class Frame
    {
        abstract void release();
        abstract boolean isConsumed();
        public static void release(Object object)
        {
            if (object instanceof Frame)
                ((Frame) object).release();
        }
    }

    /**
     * The payload bytes of a complete frame, i.e. a frame stripped of its headers and trailers,
     * with any verification supported by the protocol confirmed.
     *
     * If {@code isSelfContained} the payload contains one or more {@link Message}, all of which
     * may be parsed entirely from the bytes provided.  Otherwise, only a part of exactly one
     * {@link Message} is contained in the payload; it can be relied upon that this partial {@link Message}
     * will only be delivered in its own unique {@link Frame}.
     */
    final static class IntactFrame extends Frame
    {
        final boolean isSelfContained;
        final SharedBytes contents;

        IntactFrame(boolean isSelfContained, SharedBytes contents)
        {
            this.isSelfContained = isSelfContained;
            this.contents = contents;
        }

        void release()
        {
            contents.release();
        }

        boolean isConsumed()
        {
            return !contents.isReadable();
        }
    }

    /**
     * A corrupted frame was encountered; this represents the knowledge we have about this frame,
     * and whether or not the stream is recoverable.
     */
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

        void release() { }

        boolean isConsumed()
        {
            return true;
        }
    }


    ByteBuffer stash;
    private final Deque<Frame> frames = new ArrayDeque<>();
    private boolean active = true;
    private ChannelHandlerContext ctx;
    private ChannelConfig config;

    abstract void decode(Collection<Frame> into, SharedBytes bytes);
    abstract void addLastTo(ChannelPipeline pipeline);

    public void resume()
    {
        if (!active)
        {
            active = true;
            // previously inactive; trigger either a decode or autoread
            deliver(ctx);
            config.setAutoRead(true);
        }
    }

    public void pause()
    {
        if (active)
        {
            active = false;
            config.setAutoRead(false);
        }
    }

    public void stop()
    {
        discard();
    }

    void stash(SharedBytes in, int stashLength, int begin, int length)
    {
        ByteBuffer out = BufferPool.get(stashLength, BufferType.OFF_HEAP);
        copyBytes(in.get(), begin, out, 0, length);
        out.position(length);
        stash = out;
    }

    // visible only for legacy/none decode
    void discard()
    {
        ctx = null;
        config = null;
        active = false;
        if (stash != null)
        {
            ByteBuffer bytes = stash;
            stash = null;
            BufferPool.put(bytes);
        }
        while (!frames.isEmpty())
            frames.poll().release();
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        ByteBuffer buf;
        if (!(msg instanceof BufferPoolAllocator.Wrapped))
        {
            ByteBuf in = (ByteBuf) msg;
            buf = BufferPool.get(in.readableBytes());
            buf.limit(in.readableBytes());
            in.readBytes(buf);
            buf.flip();
        }
        else
        {
            buf = ((BufferPoolAllocator.Wrapped) msg).adopt();
        }

        decode(frames, SharedBytes.wrap(buf));
        deliver(ctx);
    }

    private void deliver(ChannelHandlerContext ctx)
    {
        while (!frames.isEmpty())
        {
            Frame frame = frames.peek();
            ctx.fireChannelRead(frame);

            if (!active)
            {
                if (frame.isConsumed())
                {
                    frames.poll();
//                    frame.release();
                }
                return;
            }

            frames.poll();

            // TODO enable once InboundMessageHandler API matches ours
//            assert frame.isConsumed();
//            frame.release();
        }
    }

    public void handlerAdded(ChannelHandlerContext ctx)
    {
        this.ctx = ctx;
        this.config = ctx.channel().config();
    }

    public void channelUnregistered(ChannelHandlerContext ctx)
    {
        discard();
    }

    public void handlerRemoved(ChannelHandlerContext ctx)
    {
        discard();
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

    static ByteBuffer ensureCapacity(ByteBuffer in, int capacity)
    {
        if (in.capacity() >= capacity)
            return in;

        ByteBuffer out = BufferPool.get(capacity, BufferType.OFF_HEAP);
        in.flip();
        out.put(in);
        BufferPool.put(in);
        return out;
    }
}
