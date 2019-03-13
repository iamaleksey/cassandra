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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.net.async.SharedDefaultFileRegion.SharedFileChannel;
import org.apache.cassandra.streaming.StreamManager.StreamRateLimiter;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A {@link DataOutputStreamPlus} that writes ASYNCHRONOUSLY to a Netty Channel.
 *
 * The close() and flush() methods synchronously wait for pending writes, and will propagate any exceptions
 * encountered in writing them to the wire.
 *
 * The correctness of this class depends on the ChannelPromise we create against a Channel always being completed,
 * which appears to be a guarantee provided by Netty so long as the event loop is running.
 */
public class AsyncChannelOutputPlus extends BufferedDataOutputStreamPlus
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncChannelOutputPlus.class);

    private final Channel channel;
    private final int bufferSize;

    /**
     * the maximum {@link #highWaterMark} and minimum {@link #lowWaterMark} number of bytes we have flushing
     * during which we should still be writing to the channel.
     *
     * i.e., if we are at or below the {@link #lowWaterMark} we should definitely start writing again;
     *       if we are at or above the {@link #highWaterMark} we should definitely stop writing;
     *       if we are inbetween, it is OK to either write or not write
     *
     * note that we consider the bytes we are about to write to our high water mark, but not our low.
     * i.e., we will not begin a write that would take us over our high water mark, unless not doing so would
     * take us below our low water mark.
     *
     * This is somewhat arbitrary accounting, and a meaningless distinction for flushes of a consistent size.
     */
    private final int lowWaterMark;
    private final int highWaterMark;

    /** the number of bytes we have begun flushing, written exclusively by the writer */
    private volatile long flushing;
    /** the number of bytes we have finished flushing, written exclusively by the channel eventLoop */
    private volatile long flushed;
    /** any error that has been thrown during a flush */
    private volatile Throwable flushFailed;

    /**
     * state for pausing until flushing has caught up - store the number of bytes we need to be flushed before
     * we should be signalled, and store ourselves in {@link #waiting}; once the flushing thread exceeds this many
     * total bytes flushed, any Thread stored in waiting will be signalled.
     *
     * This works exactly like using a WaitQueue, except that we only need to manage a single waiting thread.
     */
    private volatile long signalWhenFlushed;
    private volatile Thread waiting;

    public AsyncChannelOutputPlus(Channel channel, int bufferSize)
    {
        super(null, null);
        this.channel = channel;
        this.bufferSize = bufferSize;
        this.highWaterMark = channel.config().getWriteBufferHighWaterMark();
        this.lowWaterMark = channel.config().getWriteBufferLowWaterMark();
        allocateBuffer();
    }

    private void allocateBuffer()
    {
        buffer = BufferPool.get(bufferSize, BufferType.OFF_HEAP);
    }

    /**
     * Create a ChannelPromise for a flush of the given size.
     *
     * This method will not return until the write is permitted by the present rate limiter,
     * and on its completion will mark the requested bytes flushed.
     *
     * If this method returns normally, the ChannelPromise MUST be writtenAndFlushed, or else completed exceptionally.
     */
    private ChannelPromise beginFlush(int byteCount) throws IOException
    {
        return beginFlush(byteCount, lowWaterMark, highWaterMark);
    }

    private ChannelPromise beginFlush(int byteCount, int lowWaterMark, int highWaterMark) throws IOException
    {
        waitForSpace(byteCount, lowWaterMark, highWaterMark);

        return AsyncChannelPromise.withListener(channel, future -> {
            if (future.isSuccess() && null == flushFailed)
            {
                releaseSpace(byteCount);
            }
            else if (null == flushFailed)
            {
                Throwable cause = future.cause();
                if (cause == null)
                {
                    cause = new IOException("Flush failed for unknown reason");
                    cause.fillInStackTrace();
                }
                flushFailed = cause;
                releaseSpace(flushing - flushed);
            }
            else
            {
                assert flushing == flushed;
            }
        });
    }

    /**
     * Imposes our lowWaterMark/highWaterMark constraints, and propagates any exceptions thrown by prior flushes.
     *
     * If we currently have lowWaterMark or fewer bytes flushing, we are good to go.
     * If our new write will not take us over our highWaterMark, we are good to go.
     * Otherwise we wait until either of these conditions are met.
     *
     * This may only be invoked by the writer thread, never by the eventLoop.
     *
     * @throws IOException if a prior asynchronous flush failed
     */
    private void waitForSpace(int bytesToWrite, int lowWaterMark, int highWaterMark) throws IOException
    {
        // decide when we would be willing to carry on writing
        // we are always writable if we have lowWaterMark or fewer bytes, no matter how many bytes we are flushing
        // our callers should not be supplying more than (highWaterMark - lowWaterMark) bytes, but we must work correctly if they do
        int wakeUpWhenFlushing = highWaterMark - bytesToWrite;
        waitUntilFlushed(lowWaterMark, max(lowWaterMark, wakeUpWhenFlushing));
        flushing += bytesToWrite;
    }

    /**
     * Implementation of waitForSpace, which calculates what flushed points we need to wait for,
     * parks if necessary and propagates flush failures.
     *
     * This may only be invoked by the writer thread, never by the eventLoop.
     */
    private void waitUntilFlushed(int signalWhenExcessBytesWritten, int wakeUpWhenExcessBytesWritten) throws IOException
    {
        // we assume that we are happy to wake up at least as early as we will be signalled; otherwise we will never exit
        assert signalWhenExcessBytesWritten <= wakeUpWhenExcessBytesWritten;
        // flushing shouldn't change during this method invocation, so our calculations for signal and flushed are consistent
        long wakeUpWhenFlushed = flushing - wakeUpWhenExcessBytesWritten;
        if (flushed < wakeUpWhenFlushed)
            parkUntilFlushed(wakeUpWhenFlushed, flushing - signalWhenExcessBytesWritten);
        propagateFailedFlush();
    }

    /**
     * Utility method for waitUntilFlushed, which actually parks the current thread until the necessary
     * number of bytes have been flushed
     *
     * This may only be invoked by the writer thread, never by the eventLoop.
     */
    private void parkUntilFlushed(long wakeUpWhenFlushed, long signalWhenFlushed)
    {
        assert wakeUpWhenFlushed <= signalWhenFlushed;
        assert waiting == null;
        this.waiting = Thread.currentThread();
        this.signalWhenFlushed = signalWhenFlushed;

        while (flushed < wakeUpWhenFlushed)
            LockSupport.park();
        waiting = null;
    }

    /**
     * Update our flushed count, and signal any waiters.
     *
     * This may only be invoked by the eventLoop, never by the writer thread.
     */
    private void releaseSpace(long bytesFlushed)
    {
        long newFlushed = flushed + bytesFlushed;
        long minWritten = newFlushed + lowWaterMark;
        flushed = newFlushed;

        if (flushing > minWritten)
            return;

        Thread thread = waiting;
        if (thread != null && signalWhenFlushed <= newFlushed)
            LockSupport.unpark(thread);
    }

    private void propagateFailedFlush() throws IOException
    {
        Throwable t = flushFailed;
        if (t != null)
        {
            if (t instanceof ClosedChannelException)
                throw new IOException("The channel this output stream was writing to has been closed", t);
            throw new IOException("This output stream is in an unsafe state after an asynchronous flush failed", t);
        }
    }

    @Override
    protected void doFlush(int count) throws IOException
    {
        if (!channel.isOpen())
            throw new ClosedChannelException();

        // flush the current backing write buffer only if there's any pending data
        if (buffer.position() == 0)
            return;

        ByteBuffer flush = buffer;
        int byteCount = flush.position();
        flush.flip();
        BufferPool.putUnusedPortion(flush);
        ChannelPromise promise = beginFlush(byteCount);
        channel.writeAndFlush(BufferPoolAllocator.wrapUnshared(flush), promise);
        allocateBuffer();
    }

    public long position()
    {
        return flushing + buffer.position();
    }

    public long flushed()
    {
        // external flushed (that which has had flush() invoked implicitly or otherwise) == internal flushing
        return flushing;
    }

    public long flushedToNetwork()
    {
        return flushed;
    }

    /**
     * Perform an asynchronous flush, then waits until all outstanding flushes have completed
     *
     * @throws IOException if any flush fails
     */
    @Override
    public void flush() throws IOException
    {
        doFlush(0);
        waitUntilFlushed(0, 0);
    }

    public interface BufferSupplier
    {
        /**
         * Request a buffer with at least the given capacity.
         * This method may only be invoked once, and the lifetime of buffer it returns will be managed
         * by the AsyncChannelOutputPlus it was created for.
         */
        ByteBuffer get(int capacity) throws IOException;
    }

    public interface Write
    {
        /**
         * Write to a buffer, and flush its contents to the channel.
         *
         * The lifetime of the buffer will be managed by the AsyncChannelOutputPlus you issue this Write to.
         * If the method exits successfully, the contents of the buffer will be written to the channel, otherwise
         * the buffer will be cleaned and the exception propagated to the caller.
         */
        void write(BufferSupplier supplier) throws IOException;
    }

    /**
     * Provide a lambda that can request a buffer of suitable size, then fill the buffer and have
     * that buffer written and flushed to the underlying channel, without having to handle buffer
     * allocation, lifetime or cleanup, including in case of exceptions.
     *
     * Any exception thrown by the Write will be propagated to the caller, after any buffer is cleaned up.
     */
    public int writeToChannel(Write write, StreamRateLimiter limiter) throws IOException
    {
        doFlush(0);
        class Holder
        {
            ChannelPromise promise;
            ByteBuffer buffer;
        }
        Holder holder = new Holder();

        try
        {
            write.write(size -> {
                if (holder.buffer != null)
                    throw new IllegalStateException("Can only allocate one ByteBuffer");
                limiter.acquire(size);
                holder.promise = beginFlush(size);
                holder.buffer = BufferPool.get(size, BufferType.OFF_HEAP);
                return holder.buffer;
            });
        }
        catch (Throwable t)
        {
            // we don't currently support cancelling the flush, but at this point we are recoverable if we want
            if (holder.buffer != null)
                BufferPool.put(holder.buffer);
            if (holder.promise != null)
                holder.promise.tryFailure(t);
            throw t;
        }

        ByteBuffer buffer = holder.buffer;
        BufferPool.putUnusedPortion(buffer);

        int length = buffer.limit();
        channel.writeAndFlush(BufferPoolAllocator.wrapUnshared(buffer), holder.promise);
        return length;
    }

    /**
     * TODO: this should probably live in a different class
     *
     * Writes all data in file channel to stream, 1MiB at a time, with at most 2MiB in flight at once.
     * This method takes ownership of the provided {@code FileChannel}.
     *
     * WARNING: this method blocks only for permission to write to the netty channel; it exits before
     * the write is flushed to the network.
     */
    public long writeFileToChannel(FileChannel file, StreamRateLimiter limiter) throws IOException
    {
        // write files in 1MiB chunks, since there may be blocking work performed to fetch it from disk,
        // the data is never brought in process and is gated by the wire anyway
        return writeFileToChannel(file, limiter, 1 << 20, 1 << 20, 2 << 20);
    }

    public long writeFileToChannel(FileChannel file, StreamRateLimiter limiter, int batchSize, int lowWaterMark, int highWaterMark) throws IOException
    {
        final long length = file.size();
        long bytesTransferred = 0;

        final SharedFileChannel sharedFile = SharedDefaultFileRegion.share(file);
        try
        {
            while (bytesTransferred < length)
            {
                int toWrite = (int) min(batchSize, length - bytesTransferred);

                limiter.acquire(toWrite);
                ChannelPromise promise = beginFlush(toWrite, lowWaterMark, highWaterMark);

                SharedDefaultFileRegion fileRegion = new SharedDefaultFileRegion(sharedFile, bytesTransferred, toWrite);
                channel.writeAndFlush(fileRegion, promise);

                if (logger.isTraceEnabled())
                    logger.trace("Writing {} bytes at position {} of {}", toWrite, bytesTransferred, length);
                bytesTransferred += toWrite;
            }

            return bytesTransferred;
        }
        finally
        {
            sharedFile.release();
        }
    }

    /**
     * Flush any remaining writes, and release any buffers.
     *
     * The channel is not closed, as it is assumed to be managed externally.
     *
     * WARNING: This method requires mutual exclusivity with all other producer methods to run safely.
     * It should only be invoked by the owning thread, never the eventLoop; the eventLoop should propagate
     * errors to {@link #flushFailed}, which will propagate them to the producer thread no later than its
     * final invocation to {@link #close()} or {@link #flush()} (that must not be followed by any further writes).
     */
    @Override
    public void close() throws IOException
    {
        try
        {
            flush();
        }
        finally
        {
            discard();
        }
    }

    /**
     * Discard any buffered data, and the buffers that contain it.
     * May be invoked instead of {@link #close()} if we terminate exceptionally.
     */
    public void discard()
    {
        if (buffer != null)
        {
            BufferPool.put(buffer);
            buffer = null;
        }
    }

    @Override
    protected WritableByteChannel newDefaultChannel()
    {
        throw new UnsupportedOperationException();
    }

}