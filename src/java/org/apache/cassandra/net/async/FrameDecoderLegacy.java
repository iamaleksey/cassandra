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
import java.util.Collection;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import org.apache.cassandra.net.Message;

import static java.lang.Math.max;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

@ChannelHandler.Sharable
class FrameDecoderLegacy extends FrameDecoder
{
    private final int messagingVersion;

    private int remainingBytesInLargeMessage = 0;

    FrameDecoderLegacy(int messagingVersion) { this.messagingVersion = messagingVersion; }

    final void decode(Collection<Frame> into, SharedBytes newBytes)
    {
        ByteBuffer in = newBytes.get();
        try
        {
            if (remainingBytesInLargeMessage > 0)
            {
                if (remainingBytesInLargeMessage >= newBytes.readableBytes())
                {
                    remainingBytesInLargeMessage -= newBytes.readableBytes();
                    into.add(new IntactFrame(false, newBytes.sliceAndConsume(newBytes.readableBytes())));
                    return;
                }
                else
                {
                    Frame frame = new IntactFrame(false, newBytes.sliceAndConsume(remainingBytesInLargeMessage));
                    remainingBytesInLargeMessage = 0;
                    into.add(frame);
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
                    if (length >= 0 && length < stash.position())
                    {
                        int excess = stash.position() - length;
                        in.position(in.position() - excess);
                        stash.position(length);
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
                SharedBytes stashed = SharedBytes.wrap(stash);
                into.add(new IntactFrame(isSelfContained, stashed));
                stash = null;
            }

            int begin = in.position();
            int end = begin;
            int limit = in.limit();
            while (true)
            {
                int length = Message.serializer.messageSize(in, end, limit, messagingVersion);

                if (length >= 0 && end + length < limit)
                {
                    end += length;
                    continue;
                }

                if (end + length == limit)
                {
                    end = limit;
                    length = 0;
                }

                if (begin < end)
                    into.add(new IntactFrame(true, newBytes.slice(begin, end)));

                if (length < 0)
                {
                    stash(newBytes, max(64, limit - end), end, limit - end);
                }
                else if (length > LARGE_MESSAGE_THRESHOLD)
                {
                    remainingBytesInLargeMessage = length - (limit - end);
                    Frame frame = new IntactFrame(false, newBytes.slice(end, limit));
                    into.add(frame);
                }
                else if (length > 0)
                {
                    stash(newBytes, length, end, limit - end);
                }
                break;
            }
        }
        catch (Message.InvalidLegacyProtocolMagic e)
        {
            discard();
            into.add(CorruptFrame.unrecoverable(e.read, Message.PROTOCOL_MAGIC));
        }
        finally
        {
            newBytes.release();
        }
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderNone", this);
    }
}
