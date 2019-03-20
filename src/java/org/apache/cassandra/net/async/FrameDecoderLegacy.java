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
import java.util.function.Consumer;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import org.apache.cassandra.net.Message;

import static java.lang.Math.max;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

@ChannelHandler.Sharable
class FrameDecoderLegacy extends FrameDecoder
{
    long readHeader(ByteBuffer in, int begin)
    {
        throw new UnsupportedOperationException();
    }
    CorruptFrame verifyHeader(long header)
    {
        throw new UnsupportedOperationException();
    }
    int frameLength(long header)
    {
        throw new UnsupportedOperationException();
    }
    Frame unpackFrame(SharedBytes bytes, int begin, int end, long header, boolean transferOwnership)
    {
        throw new UnsupportedOperationException();
    }

    private final int messagingVersion;
    private int remainingBytesInLargeMessage = 0;

    FrameDecoderLegacy(int messagingVersion) { this.messagingVersion = messagingVersion; }

    void decode(Consumer<Frame> consumer, SharedBytes bytes)
    {
        boolean ownsBytes = true;
        ByteBuffer in = bytes.get();
        try
        {
            if (remainingBytesInLargeMessage > 0)
            {
                if (remainingBytesInLargeMessage >= bytes.readableBytes())
                {
                    remainingBytesInLargeMessage -= bytes.readableBytes();
                    ownsBytes = false;
                    consumer.accept(new IntactFrame(false, bytes));
                    return;
                }
                else
                {
                    Frame frame = new IntactFrame(false, bytes.sliceAndConsume(remainingBytesInLargeMessage));
                    remainingBytesInLargeMessage = 0;
                    consumer.accept(frame);
                }
            }

            if (stash != null)
            {
                int length = Message.serializer.messageSize(stash, 0, stash.position(), messagingVersion);
                while (length < 0)
                {
                    if (!in.hasRemaining())
                        return;

                    if (stash.position() == stash.capacity())
                        stash = ensureCapacity(stash, stash.capacity() * 2);
                    copyToSize(in, stash, stash.capacity());

                    length = Message.serializer.messageSize(stash, 0, stash.position(), messagingVersion);
                    if (length >= 0 && length < stash.limit())
                    {
                        int excess = stash.limit() - length;
                        in.position(in.position() - excess);
                    }
                }

                if (length > stash.limit() && length <= LARGE_MESSAGE_THRESHOLD)
                {
                    stash = ensureCapacity(stash, length);
                    if (!copyToSize(in, stash, length))
                        return;
                }

                boolean isSelfContained = true;
                if (length > LARGE_MESSAGE_THRESHOLD)
                {
                    isSelfContained = false;
                    remainingBytesInLargeMessage = length - stash.limit();
                }

                stash.flip();
                Frame frame = new IntactFrame(isSelfContained, SharedBytes.wrap(stash));
                stash = null;
                consumer.accept(frame);
            }

            int begin = in.position();
            int end = begin;
            int limit = in.limit();
            while (true)
            {
                int length = Message.serializer.messageSize(in, end, limit, messagingVersion);

                if (length < 0 || end + length >= limit)
                {
                    if (begin < end)
                        consumer.accept(new IntactFrame(true, bytes.slice(begin, end)));

                    if (length > LARGE_MESSAGE_THRESHOLD)
                    {
                        in.position(end);
                        remainingBytesInLargeMessage = length - (limit - end);
                        Frame frame = new IntactFrame(false, bytes);
                        ownsBytes = false;
                        consumer.accept(frame);
                    }
                    else
                    {
                        stash(bytes, length < 0 ? max(64, limit - end) : length, end, limit);
                    }
                    break;
                }

                end += length;
            }
        }
        catch (Message.InvalidLegacyProtocolMagic e)
        {
            bytes.release();
            reset();
            consumer.accept(CorruptFrame.unrecoverable(e.read, Message.PROTOCOL_MAGIC));
            assert ownsBytes;
        }
        finally
        {
            if (ownsBytes)
                bytes.release();
        }
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderNone", this);
    }
}
