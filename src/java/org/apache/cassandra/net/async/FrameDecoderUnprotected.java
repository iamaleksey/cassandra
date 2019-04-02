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

import io.netty.channel.ChannelPipeline;

import static org.apache.cassandra.net.async.FrameDecoderCrc.HEADER_LENGTH;
import static org.apache.cassandra.net.async.FrameDecoderCrc.isSelfContained;
import static org.apache.cassandra.net.async.FrameDecoderCrc.payloadLength;
import static org.apache.cassandra.net.async.FrameDecoderCrc.readHeader6b;
import static org.apache.cassandra.net.async.FrameDecoderCrc.verifyHeader6b;

/**
 * A frame decoder for unprotected frames, i.e. those without any modification or payload protection.
 * This is non-standard, and useful for systems that have a trusted transport layer that want
 * to avoid incurring the (very low) cost of computing a CRC.  All we do is accumulate the bytes
 * of the frame, verify the frame header, and pass through the bytes stripped of the header.
 */
final class FrameDecoderUnprotected extends FrameDecoderWith8bHeader
{
    public static FrameDecoderUnprotected create()
    {
        return new FrameDecoderUnprotected();
    }

    final long readHeader(ByteBuffer frame, int begin)
    {
        return readHeader6b(frame, begin);
    }

    final CorruptFrame verifyHeader(long header6b)
    {
        return verifyHeader6b(header6b);
    }

    final int frameLength(long header6b)
    {
        return payloadLength(header6b) + HEADER_LENGTH;
    }

    final Frame unpackFrame(SharedBytes bytes, int begin, int end, long header6b)
    {
        boolean isSelfContained = isSelfContained(header6b);
        return new IntactFrame(isSelfContained, bytes.slice(begin + HEADER_LENGTH, end));
    }

    void decode(Collection<Frame> into, SharedBytes bytes)
    {
        decode(into, bytes, HEADER_LENGTH);
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameDecoderUnprotected", this);
    }
}
