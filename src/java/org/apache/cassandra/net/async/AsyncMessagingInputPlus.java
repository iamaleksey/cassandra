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

/**
 * A {@link RebufferingInputStream} that takes its input asynchronously from a Netty event loop thread.
 *
 * A producer thread (Netty event loop) supplies the {@link SharedBytes} it receives to the {@link AsyncMessagingInputPlus},
 * non-blockingly; they are later consumed - blockingly - by a consumer thread outside of the event loop.
 *
 * {@link #supply(SharedBytes)}, {@link #requestClosure()}, and {@link #supplyAndRequestClosure(SharedBytes)} methods
 * MUST only be invoked by the producer thread. {@link #close()} method MUST only be invoked by the consumer thread.
 *
 * The producer thread MUST invoke {@link #requestClosure()} or {@link #supplyAndRequestClosure(SharedBytes)} in the end,
 * otherwise the consumer thread might be blocked forever.
 *
 * The consumer thread MUST invoke {@link #close()} in the end, to guarantee that under any circumstancies, the last
 * supplied {@link SharedBytes} are released.
 */
class AsyncMessagingInputPlus extends RebufferingInputStream
{
    /**
     * Exception thrown when closure was explicitly requested.
     */
    private static final class InputClosedException extends EOFException {}

    // used to signal AsyncMessagingInputPlus that it should close itself
    private static final SharedBytes CLOSE_INPUT = new SharedBytes.Empty();

    private final Queue<SharedBytes> queue;
    // SpscUnboundedArrayQueue does not provide volatile write semantics, so we write to this instead
    @SuppressWarnings("unused")
    private volatile boolean writeBarrier;

    private final IntConsumer onReleased;

    private SharedBytes current;
    private int currentSize;

    private volatile boolean isClosed;
    private volatile Thread parkedThread;

    AsyncMessagingInputPlus(IntConsumer onReleased)
    {
        super(SharedBytes.Empty.instance.get());
        this.current = SharedBytes.Empty.instance;
        this.currentSize = 0;

        this.queue = new SpscUnboundedArrayQueue<>(16);
        this.onReleased = onReleased;
    }

    /*
     * Methods intended to be invoked by the producer thread only
     */

    /**
     * Supply a {@link SharedBytes} buffer to the consumer.
     *
     * Intended to be invoked by the producer thread only.
     */
    void supply(SharedBytes bytes)
    {
        if (isClosed)
            throw new IllegalStateException("Cannot supply a buffer to a closed AsyncMessagingInputPlus");

        queue.add(bytes);
        writeBarrier = true;

        maybeUnpark();
    }

    /**
     * Ask {@link AsyncMessagingInputPlus} to close itself on the consumer thread.
     *
     * Intended to be invoked by the producer thread only.
     */
    void requestClosure()
    {
        if (isClosed)
            throw new IllegalStateException("Cannot close an already closed AsyncMessagingInputPlus");

        queue.add(CLOSE_INPUT);
        writeBarrier = true;

        maybeUnpark();
    }

    /**
     * Supply a {@link SharedBytes} buffer to {@link AsyncMessagingInputPlus} and ask it to close itself on the consumer thread.
     *
     * Intended to be invoked by the producer thread only.
     */
    void supplyAndRequestClosure(SharedBytes bytes)
    {
        if (isClosed)
            throw new IllegalStateException("Cannot supply a buffer to a closed AsyncMessagingInputPlus");

        queue.add(bytes);
        queue.add(CLOSE_INPUT);
        writeBarrier = true;

        maybeUnpark();
    }

    private void maybeUnpark()
    {
        Thread thread = parkedThread;
        if (null != thread)
            LockSupport.unpark(thread);
    }

    /*
     * Methods intended to be invoked by the consumer thread only
     */

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

    /**
     * Will block and wait until {@link #requestClosure} is invoked by the producer thread, skipping over and releasing
     * any buffers encountered while waiting for an input-close marker.
     *
     * Must be invoked by the owning thread only.
     */
    @Override
    public void close()
    {
        if (isClosed)
            return;

        releaseCurrentBuf();

        SharedBytes next;
        while ((next = pollBlockingly()) != CLOSE_INPUT)
        {
            onReleased.accept(next.readableBytes());
            next.release();
        }

        isClosed = true;
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
}
