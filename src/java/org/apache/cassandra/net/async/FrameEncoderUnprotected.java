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
import java.nio.ByteOrder;
import java.util.zip.CRC32;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelPipeline;
import org.apache.cassandra.utils.memory.BufferPool;

import static org.apache.cassandra.net.async.Crc.crc24;
import static org.apache.cassandra.net.async.Crc.crc32;
import static org.apache.cassandra.net.async.FrameEncoderCrc.HEADER_LENGTH;
import static org.apache.cassandra.net.async.FrameEncoderCrc.writeHeader;

/**
 * A frame encoder that writes frames, just without any modification or payload protection.
 * This is non-standard, and useful for systems that have a trusted transport layer that want
 * to avoid incurring the (very low) cost of computing a CRC.
 */
@ChannelHandler.Sharable
public class FrameEncoderUnprotected extends FrameEncoder
{
    public static final FrameEncoderUnprotected instance = new FrameEncoderUnprotected();
    public static final PayloadAllocator allocator = (isSelfContained, capacity) ->
        new Payload(isSelfContained, capacity, HEADER_LENGTH, 0);

    PayloadAllocator allocator()
    {
        return allocator;
    }

    ByteBuf encode(boolean isSelfContained, ByteBuffer frame)
    {
        try
        {
            int frameLength = frame.remaining();
            int dataLength = frameLength - HEADER_LENGTH;
            if (dataLength >= 1 << 17)
                throw new IllegalArgumentException("Maximum uncompressed payload size is 128KiB");

            writeHeader(frame, isSelfContained, dataLength);
            return BufferPoolAllocator.wrap(frame);
        }
        catch (Throwable t)
        {
            BufferPool.put(frame, false);
            throw t;
        }
    }

    void addLastTo(ChannelPipeline pipeline)
    {
        pipeline.addLast("frameEncoderCrc", this);
    }
}
