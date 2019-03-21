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

import net.nicoulaj.compilecommand.annotations.Inline;

abstract class FrameDecoderWith8bHeader extends FrameDecoder
{
    /**
     * Read a header that is 8 bytes or shorter, without modifying the buffer position.
     * If your header is longer than this, you will need to implement your own {@link #decode}
     */
    abstract long readHeader(ByteBuffer in, int begin);
    /**
     * Verify the header, and return an unrecoverable CorruptFrame if it is corrupted
     * @return null or CorruptFrame.unrecoverable
     */
    abstract CorruptFrame verifyHeader(long header);

    /**
     * Calculate the full frame length from info provided by the header, including the length of the header and any triler
     */
    abstract int frameLength(long header);

    /**
     * Extract a frame known to cover the given range.
     * If {@code transferOwnership}, the method is responsible for ensuring bytes.release() is invoked at some future point.
     */
    abstract Frame unpackFrame(SharedBytes bytes, int begin, int end, long header);

    /**
     * Decode a number of frames using the above abstract method implementations.
     * It is expected for this method to be invoked by the implementing class' {@link #decode(Collection, SharedBytes)}
     * so that this implementation will be inlined, and all of the abstract method implementations will also be inlined.
     * TODO verify this in assembly
     */
    @Inline
    protected void decode(Collection<Frame> into, SharedBytes newBytes, int headerLength)
    {
        ByteBuffer in = newBytes.get();

        try
        {
            if (stash != null)
            {
                if (!copyToSize(in, stash, headerLength))
                    return;

                long header = readHeader(stash, 0);
                CorruptFrame c = verifyHeader(header);
                if (c != null)
                {
                    discard();
                    into.add(c);
                    return;
                }

                int frameLength = frameLength(header);
                stash = ensureCapacity(stash, frameLength);

                if (!copyToSize(in, stash, frameLength))
                    return;

                stash.flip();
                SharedBytes stashed = SharedBytes.wrap(stash);
                stash = null;

                try
                {
                    into.add(unpackFrame(stashed, 0, frameLength, header));
                }
                finally
                {
                    stashed.release();
                }
            }

            int begin = in.position();
            int limit = in.limit();
            while (begin < limit)
            {
                int remaining = limit - begin;
                if (remaining < headerLength)
                {
                    stash(newBytes, headerLength, begin, remaining);
                    return;
                }

                long header = readHeader(in, begin);
                CorruptFrame c = verifyHeader(header);
                if (c != null)
                {
                    into.add(c);
                    return;
                }

                int frameLength = frameLength(header);
                if (remaining < frameLength)
                {
                    stash(newBytes, frameLength, begin, remaining);
                    return;
                }

                into.add(unpackFrame(newBytes, begin, begin + frameLength, header));
                begin += frameLength;
            }
        }
        finally
        {
            newBytes.release();
        }
    }

}
