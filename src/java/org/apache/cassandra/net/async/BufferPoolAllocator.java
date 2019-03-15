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

import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
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
        throw new UnsupportedOperationException();
    }

    protected ByteBuf newDirectBuffer(int minCapacity, int maxCapacity)
    {
        ByteBuf result = wrap(BufferPool.get(minCapacity, BufferType.OFF_HEAP));
        result.clear();
        return result;
    }

    public static ByteBuf wrap(ByteBuffer buffer)
    {
        return new UnpooledUnsafeDirectByteBuf(instance, buffer, buffer.capacity()) {
            public void deallocate()
            {
                BufferPool.put(buffer);
            }
        };
    }

    public static ByteBuf wrapUnshared(ByteBuffer buffer)
    {
        return new UnpooledUnsafeDirectByteBuf(instance, buffer, buffer.capacity()) {
            public void deallocate()
            {
                throw new IllegalStateException();
            }
            public ByteBuf retain()
            {
                throw new UnsupportedOperationException();
            }
            public ByteBuf retain(int count)
            {
                throw new UnsupportedOperationException();
            }
            public boolean release()
            {
                BufferPool.put(buffer);
                return true;
            }
            public boolean release(int count)
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    public boolean isDirectBufferPooled()
    {
        return true;
    }
}
