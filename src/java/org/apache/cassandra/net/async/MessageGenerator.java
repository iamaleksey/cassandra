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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;

abstract class MessageGenerator
{
    final long seed;
    final Random random;

    private MessageGenerator(long seed)
    {
        this.seed = seed;
        this.random = new Random();
    }

    Message.Builder<Object> builder(long id)
    {
        random.setSeed(id ^ seed);
        return Message.builder(Verb._TEST_2, null)
                      .withExpiresAt(System.nanoTime() + TimeUnit.DAYS.toNanos(1L)); // don't expire for now
    }
    abstract Message<?> generate(long id);
    abstract MessageGenerator copy();

    static final class UniformPayloadGenerator extends MessageGenerator
    {
        final int minSize;
        final int maxSize;
        final byte[] fillWithBytes;
        UniformPayloadGenerator(long seed, int minSize, int maxSize)
        {
            super(seed);
            this.minSize = minSize;
            this.maxSize = maxSize;
            this.fillWithBytes = new byte[32];
            random.setSeed(seed);
            random.nextBytes(fillWithBytes);
        }

        Message<?> generate(long id)
        {
            Message.Builder<Object> builder = builder(id);
            byte[] payload = new byte[minSize + random.nextInt(maxSize - minSize)];
            ByteBuffer wrapped = ByteBuffer.wrap(payload);
            while (wrapped.hasRemaining())
                wrapped.put(fillWithBytes, 0, Math.min(fillWithBytes.length, wrapped.remaining()));
            builder.withPayload(payload);
            return builder.build();
        }

        MessageGenerator copy()
        {
            return new UniformPayloadGenerator(seed, minSize, maxSize);
        }
    }

}

