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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AsyncMessagingInputPlusTest
{
    @Before
    public void setUp()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testConsumesFully()
    {
        int k = 89;
        int j = 97;

        byte[] produced = new byte[k * j];
        byte[] consumed = new byte[j * k];

        new Random().nextBytes(produced);

        AtomicInteger released = new AtomicInteger();
        AsyncMessagingInputPlus input = new AsyncMessagingInputPlus(released::addAndGet);

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread consumer = new Thread(() ->
        {
            try
            {
                // consume j bytes k times
                for (int i = 0; i < k; i++)
                    input.readFully(consumed, i * j, j);
            }
            catch (Throwable t)
            {
                thrown.set(t);
            }
            finally
            {
                input.close();
            }
        });
        consumer.start();

        // produce k bytes j times
        for (int i = 0; i < j; i++)
            input.supply(SharedBytes.wrap(ByteBuffer.wrap(produced, i * k, k)));
        input.requestClosure();

        Uninterruptibles.joinUninterruptibly(consumer);

        assertNull(thrown.get());
        assertEquals(k * j, released.get());
        assertArrayEquals(produced, consumed);
    }

    @Test
    public void testUnderConsumes()
    {
        int k = 89;
        int j = 97;

        byte[] produced = new byte[k * j];
        byte[] consumed = new byte[j * (k - 1)];

        new Random().nextBytes(produced);

        AtomicInteger released = new AtomicInteger();
        AsyncMessagingInputPlus input = new AsyncMessagingInputPlus(released::addAndGet);

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread consumer = new Thread(() ->
        {
            try
            {
                // consume j bytes k - 1 times
                for (int i = 0; i < k - 1; i++)
                    input.readFully(consumed, i * j, j);
            }
            catch (Throwable t)
            {
                thrown.set(t);
            }
            finally
            {
                input.close();
            }
        });
        consumer.start();

        // produce k bytes j times
        for (int i = 0; i < j; i++)
            input.supply(SharedBytes.wrap(ByteBuffer.wrap(produced, i * k, k)));
        input.requestClosure();

        Uninterruptibles.joinUninterruptibly(consumer);

        assertNull(thrown.get());
        assertEquals(k * j, released.get());

        for (int i = 0; i < (k - 1) * j; i++)
            assertEquals(produced[i], consumed[i]);
    }

    @Test
    public void testOverConsumes()
    {
        int k = 89;
        int j = 97;

        byte[] produced = new byte[k * j];
        byte[] consumed = new byte[j * (k + 1)];

        new Random().nextBytes(produced);

        AtomicInteger released = new AtomicInteger();
        AsyncMessagingInputPlus input = new AsyncMessagingInputPlus(released::addAndGet);

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread consumer = new Thread(() ->
        {
            try
            {
                // consume j bytes k + 1 times
                for (int i = 0; i < k + 1; i++)
                    input.readFully(consumed, i * j, j);
            }
            catch (Throwable t)
            {
                thrown.set(t);
            }
            finally
            {
                input.close();
            }
        });
        consumer.start();

        // produce k bytes j times
        for (int i = 0; i < j; i++)
            input.supply(SharedBytes.wrap(ByteBuffer.wrap(produced, i * k, k)));
        input.requestClosure();

        Uninterruptibles.joinUninterruptibly(consumer);

        assertTrue(thrown.get() instanceof EOFException);
        assertEquals(k * j, released.get());

        for (int i = 0; i < k * j; i++)
            assertEquals(produced[i], consumed[i]);
    }

    @Test
    public void testSupplyAndRequestClosure()
    {
        int k = 89;
        int j = 97;

        byte[] produced = new byte[k * j];
        byte[] consumed = new byte[j * k];

        new Random().nextBytes(produced);

        AtomicInteger released = new AtomicInteger();
        AsyncMessagingInputPlus input = new AsyncMessagingInputPlus(released::addAndGet);

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread consumer = new Thread(() ->
        {
            try
            {
                // consume j bytes k times
                for (int i = 0; i < k; i++)
                    input.readFully(consumed, i * j, j);
            }
            catch (Throwable t)
            {
                thrown.set(t);
            }
            finally
            {
                input.close();
            }
        });
        consumer.start();

        input.supplyAndRequestClosure(SharedBytes.wrap(ByteBuffer.wrap(produced)));

        Uninterruptibles.joinUninterruptibly(consumer);

        assertNull(thrown.get());
        assertEquals(k * j, released.get());
        assertArrayEquals(produced, consumed);
    }
}
