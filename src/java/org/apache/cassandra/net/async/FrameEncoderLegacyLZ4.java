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

import java.util.ArrayList;
import java.util.List;
import java.util.zip.Checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.PromiseNotifier;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;

@ChannelHandler.Sharable
class FrameEncoderLegacyLZ4 extends FrameEncoderLegacy
{
    public static final FrameEncoderLegacyLZ4 instance = new FrameEncoderLegacyLZ4();

    private static final int LEGACY_COMPRESSION_BLOCK_SIZE = OutboundConnection.LargeMessageDelivery.DEFAULT_BUFFER_SIZE;
    private static final int LEGACY_LZ4_HASH_SEED = 0x9747b28c;

    // Netty's Lz4FrameEncoder notifies flushes as successful when they may not be, by flushing an empty buffer ahead of the compressed buffer
    // in reality, they need to be notified _afterwards_, so here we:
    //   * Buffer the promises we want to notify in a list
    //   * Swallow any normal flush invocations in FlushDeferrer
    //   * Extend Lz4FrameEncoder to override flush() so that we can:
    //   * - After writing compressed, but before flushing, insert a new promise notifier for our waiting promises
    //   * - Then request an actual flush of our FlushDeferrer with its special value

    private static class WaitingPromiseWriter extends ChannelOutboundHandlerAdapter
    {
        final List<Promise<?>> waiting = new ArrayList<>();
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
        {
            ctx.write(ReferenceCountUtil.retain(msg), promise);
            if (!waiting.isEmpty())
            {
                ChannelPromise waiting = AsyncChannelPromise.withListener(ctx.channel(), new PromiseNotifier<>(this.waiting.toArray(new Promise[0])));
                this.waiting.clear();
                ctx.write(Unpooled.EMPTY_BUFFER, waiting);
            }
        }

        void add(Promise<?> promise)
        {
            waiting.add(promise);
        }
    }

    private static class Lz4FrameEncoderWithAccurateFlushNotification extends Lz4FrameEncoder
    {
        private final WaitingPromiseWriter promises = new WaitingPromiseWriter();

        Lz4FrameEncoderWithAccurateFlushNotification(LZ4Factory factory, boolean highCompressor, int blockSize, Checksum checksum)
        {
            super(factory, highCompressor, blockSize, checksum);
        }

        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
        {
            try
            {
                ByteBuf in  = (ByteBuf) msg;
                ByteBuf out = allocateBuffer(ctx, in, isPreferDirect());
                try
                {
                    encode(ctx, in, out);
                    promises.add(promise);
                    if (out.isReadable())
                        ctx.write(out);
                }
                finally
                {
                    out.release();
                }
            }
            catch (EncoderException e)
            {
                throw e;
            }
            catch (Throwable e)
            {
                throw new EncoderException(e);
            }
        }
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        Lz4FrameEncoderWithAccurateFlushNotification encoder = new Lz4FrameEncoderWithAccurateFlushNotification(LZ4Factory.fastestInstance(), false, LEGACY_COMPRESSION_BLOCK_SIZE, XXHashFactory.fastestInstance().newStreamingHash32(LEGACY_LZ4_HASH_SEED).asChecksum());
        pipeline.addLast("promisefix", encoder.promises);
        pipeline.addLast("legacyLz4", encoder);
        pipeline.addLast("frameEncoderNone", this);
    }
}
