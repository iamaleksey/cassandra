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
import java.util.Queue;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntConsumer;

import org.apache.cassandra.io.util.RebufferingInputStream;
import org.jctools.queues.SpscUnboundedArrayQueue;

import static org.apache.cassandra.net.async.FrameDecoder.*;

class AsyncInputPlus extends RebufferingInputStream
{
    /**
     * Exception thrown when closure was explicitly requested.
     */
    static final class InputClosedException extends EOFException
    {
    }

    // EMPTY_BUFFER is used to signal AsyncInputPlus that it should be closed
    private static final SharedBytes CLOSE_INPUT = SharedBytes.EMPTY;

    private final Queue<SharedBytes> queue;

    private final IntConsumer onReleased;

    private SharedBytes current;
    private int currentSize;

    private volatile boolean isClosed;
    private volatile Thread parkedThread;

    AsyncInputPlus(IntConsumer onReleased)
    {
        super(SharedBytes.EMPTY.get());
        this.current = SharedBytes.EMPTY;
        this.currentSize = 0;

        this.queue = new SpscUnboundedArrayQueue<>(16);
        this.onReleased = onReleased;
    }

    @Override
    protected void reBuffer() throws InputClosedException
    {
        releaseCurrentBuf();

        SharedBytes next = pollBlockingly();
        if (next == CLOSE_INPUT)
        {
            isClosed = true;
            throw new InputClosedException();
        }

        current = next;
        currentSize = next.readableBytes();
        buffer = next.get();
    }

    @Override
    public void close()
    {
        if (isClosed)
            return;

        if (null != current)
            releaseCurrentBuf();

        SharedBytes nextSlice;
        while ((nextSlice = pollBlockingly()) != CLOSE_INPUT)
        {
            onReleased.accept(nextSlice.readableBytes());
            nextSlice.release();
        }

        isClosed = true;
    }

    void supplyAndCloseWithoutSignaling(SharedBytes bytes)
    {
        if (isClosed)
            throw new IllegalStateException("Cannot supply a buffer to a closed AsyncInputPlus");

        queue.add(bytes);
        queue.add(CLOSE_INPUT);
    }

    void supply(SharedBytes bytes)
    {
        if (isClosed)
            throw new IllegalStateException("Cannot supply a buffer to a closed AsyncInputPlus");

        queue.add(bytes);
        maybeUnpark();
    }

    private void releaseCurrentBuf()
    {
        current.release();
        if (currentSize > 0)
            onReleased.accept(currentSize);
        current = null;
        currentSize = 0;
        buffer = null;
    }

    void requestClosure()
    {
        if (isClosed)
            throw new IllegalStateException("Cannot close an already closed AsyncInputPlus");

        queue.add(CLOSE_INPUT);
        maybeUnpark();
    }

    private SharedBytes pollBlockingly()
    {
        SharedBytes buf = queue.poll();
        if (null != buf)
            return buf;

        parkedThread = Thread.currentThread();
        while ((buf = queue.poll()) == null)
            LockSupport.park();
        parkedThread = null;
        return buf;
    }

    private void maybeUnpark()
    {
        Thread thread = parkedThread;
        if (null != thread)
            LockSupport.unpark(thread);
    }
}
