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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Math.min;

/**
 * A trivial wrapper around BufferPool for integrating with Netty, but retaining ownership of pooling behaviour
 * that is integrated into Cassandra's other pooling
 */
public class BufferPoolAllocator extends AbstractByteBufAllocator
{
    public static final BufferPoolAllocator instance = new BufferPoolAllocator();
    private BufferPoolAllocator() { super(true); }

    protected ByteBuf newHeapBuffer(int minCapacity, int maxCapacity)
    {
        // for pre40; Netty LZ4 decoder sometimes allocates on heap explicitly for some reason
        return Unpooled.buffer(minCapacity, maxCapacity);
    }

    protected ByteBuf newDirectBuffer(int minCapacity, int maxCapacity)
    {
        ByteBuf result = wrap(BufferPool.getAtLeast(minCapacity, BufferType.OFF_HEAP));
        result.clear();
        return result;
    }

    public static ByteBuf wrap(ByteBuffer buffer)
    {
        return new Wrapped(buffer);
    }

    /**
     * A simple extension to UnpooledUnsafeDirectByteBuf that returns buffers to BufferPool on deallocate,
     * and permits extracting the buffer from it to take ownership and use directly,
     * which is used in {@link FrameDecoder#channelRead(ChannelHandlerContext, Object)}
     */
    static class Wrapped extends UnpooledUnsafeDirectByteBuf
    {
        private ByteBuffer wrapped;
        public Wrapped(ByteBuffer wrap)
        {
            super(instance, wrap, wrap.capacity());
            wrapped = wrap;
        }

        public void deallocate()
        {
            if (wrapped != null)
                BufferPool.put(wrapped, false);
        }

        ByteBuffer adopt()
        {
            if (refCnt() > 1)
                throw new IllegalStateException();
            ByteBuffer adopt = wrapped;
            adopt.position(readerIndex()).limit(writerIndex());
            wrapped = null;
            return adopt;
        }
    }

    public boolean isDirectBufferPooled()
    {
        return true;
    }
}
