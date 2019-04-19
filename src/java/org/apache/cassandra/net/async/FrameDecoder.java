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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.utils.ByteBufferUtil.copyBytes;

abstract class FrameDecoder extends ChannelInboundHandlerAdapter
{
    abstract static class Frame
    {
        abstract void release();
        abstract boolean isConsumed();
    }

    interface FrameProcessor
    {
        /**
         * Frame processor that the frames should be handed off to.
         *
         * @return true if more frames can be taken by the processor, false if the decoder should pause until
         * it's explicitly resumed.
         */
        boolean process(Frame frame) throws IOException;
    }

    private FrameProcessor processor;

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
    private final Deque<Frame> frames = new ArrayDeque<>(4);
    private boolean active = false;
    private ChannelHandlerContext ctx;
    private ChannelConfig config;

    abstract void decode(Collection<Frame> into, SharedBytes bytes);
    abstract void addLastTo(ChannelPipeline pipeline);

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to permanently
     * stop receiving frames, e.g. because of an exception caught.
     */
    void stop()
    {
        active = false;
        config.setAutoRead(false);
    }

    /**
     * For use by InboundMessageHandler (or other upstream handlers) that want to resume
     * receiving frames after previously indicating that processing should be paused.
     */
    void resume(FrameProcessor processor)
    {
        this.processor = processor;
        active = true;
        deliver(ctx);
        if (active) // we could have paused again before delivery completed
            config.setAutoRead(true);
    }

    /**
     * Called by Netty pipeline when a new message arrives; we anticipate in normal operation
     * this will receive messages of type {@link BufferPoolAllocator.Wrapped}
     *
     * These buffers are unwrapped and passed to {@link #decode(Collection, SharedBytes)},
     * which collects decoded frames into {@link #frames}, which we send upstream in {@link #deliver}
     */
    public void channelRead(ChannelHandlerContext ctx, Object msg)
    {
        ByteBuffer buf;
        if (msg instanceof BufferPoolAllocator.Wrapped)
        {
            buf = ((BufferPoolAllocator.Wrapped) msg).adopt();
            BufferPool.putUnusedPortion(buf, false); // netty will probably have mis-predicted the space needed
        }
        else
        {
            // this is only necessary for pre40, which uses the legacy LZ4,
            // which sometimes allocates on heap explicitly for some reason
            ByteBuf in = (ByteBuf) msg;
            buf = BufferPool.get(in.readableBytes());
            in.readBytes(buf);
            buf.flip();
        }

        decode(frames, SharedBytes.wrap(buf));

        deliver(ctx);
        if (!active)
            config.setAutoRead(false);
    }

    /**
     * Deliver any waiting frames, including those that were incompletely read last time
     */
    private void deliver(ChannelHandlerContext ctx)
    {
        try
        {
            while (active && !frames.isEmpty())
            {
                Frame frame = frames.peek();
                active = processor.process(frame);

                assert !active || frame.isConsumed();
                if (active || frame.isConsumed())
                {
                    frames.poll();
                    frame.release();
                }
            }
        }
        catch (IOException e)
        {
            ctx.fireExceptionCaught(e);
        }
    }

    void stash(SharedBytes in, int stashLength, int begin, int length)
    {
        ByteBuffer out = BufferPool.getAtLeast(stashLength, BufferType.OFF_HEAP);
        copyBytes(in.get(), begin, out, 0, length);
        out.position(length);
        stash = out;
    }

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

    public void handlerAdded(ChannelHandlerContext ctx)
    {
        this.ctx = ctx;
        this.config = ctx.channel().config();

        config.setAutoRead(false);
    }

    public void channelInactive(ChannelHandlerContext ctx)
    {
        discard();
    }

    public void handlerRemoved(ChannelHandlerContext ctx)
    {
        discard();
    }

    /**
     * Utility: fill {@code out} from {@code in} up to {@code toOutPosition},
     * updating the position of both buffers with the result
     * @return true if there were sufficient bytes to fill to {@code toOutPosition}
     */
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

    /**
     * @return {@code in} if has sufficient capacity, otherwise
     *         a replacement from {@code BufferPool} that {@code in} is copied into
     */
    static ByteBuffer ensureCapacity(ByteBuffer in, int capacity)
    {
        if (in.capacity() >= capacity)
            return in;

        ByteBuffer out = BufferPool.getAtLeast(capacity, BufferType.OFF_HEAP);
        in.flip();
        out.put(in);
        BufferPool.put(in);
        return out;
    }
}
