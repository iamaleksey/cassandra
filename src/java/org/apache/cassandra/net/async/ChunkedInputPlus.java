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

import java.io.EOFException;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import org.apache.cassandra.io.util.RebufferingInputStream;

/**
 * A specialised {@link org.apache.cassandra.io.util.DataInputPlus} implementation for deserializing large messages
 * that are split over multiple {@link org.apache.cassandra.net.async.FrameDecoder.Frame}s.
 *
 * Ensures that every underlying {@link SharedBytes} frame is released, and promptly so, as frames are consumed.
 *
 * {@link #close()} <em>MUST</em> be invoked in the end.
 */
class ChunkedInputPlus extends RebufferingInputStream
{
    private final PeekingIterator<SharedBytes> iter;

    private ChunkedInputPlus(PeekingIterator<SharedBytes> iter)
    {
        super(iter.peek().get());
        this.iter = iter;
    }

    /**
     * Creates a {@link ChunkedInputPlus} from the provided {@link SharedBytes} buffers.
     *
     * The provided iterable <em>must</em> contain at least one buffer.
     */
    static ChunkedInputPlus of(Iterable<SharedBytes> buffers)
    {
        PeekingIterator<SharedBytes> iter = Iterators.peekingIterator(buffers.iterator());
        if (!iter.hasNext())
            throw new IllegalArgumentException();
        return new ChunkedInputPlus(iter);
    }

    @Override
    protected void reBuffer() throws EOFException
    {
        buffer = null;
        iter.peek().release();
        iter.next();

        if (!iter.hasNext())
            throw new EOFException();

        buffer = iter.peek().get();
    }

    @Override
    public void close()
    {
        buffer = null;
        iter.forEachRemaining(SharedBytes::release);
    }

    /**
     * Returns the number of unconsumed bytes. Will release any outstanding buffers and consume the underlying iterator.
     *
     * Should only be used for sanity checking, once the input is no longer needed, as it will implicitly close the input.
     */
    int remainder()
    {
        buffer = null;

        int bytes = 0;
        while (iter.hasNext())
        {
            SharedBytes chunk = iter.peek();
            bytes += chunk.readableBytes();
            chunk.release();
            iter.next();
        }
        return bytes;
    }
}
