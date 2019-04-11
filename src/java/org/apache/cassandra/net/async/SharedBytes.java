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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * A wrapper for possibly sharing portions of a single, BufferPool managed, ByteBuffer;
 * optimised for the case where no sharing is necessary.
 */
class SharedBytes
{
    public static final class Empty extends SharedBytes
    {
        public static final Empty instance = new Empty();
        public Empty() { super(ByteBufferUtil.EMPTY_BYTE_BUFFER); }
        SharedBytes atomic() { return this; }
        SharedBytes retain() { return this; }
        void release() {}
    }

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
     * Create a slice over the next {@code length} bytes, consuming them from our buffer, and incrementing the owner count
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

