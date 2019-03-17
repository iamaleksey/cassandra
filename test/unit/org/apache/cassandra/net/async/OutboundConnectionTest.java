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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.EmptyMessage;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ApproximateTime;

import static org.apache.cassandra.net.EmptyMessage.emptyMessage;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.OutboundConnection.Type.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnection.Type.SMALL_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

public class OutboundConnectionTest
{
    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        NettyFactory.instance.close();
    }

    interface SendTest
    {
        void accept(InboundMessageHandlers inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
    }

    static class Settings
    {
        static final Settings SMALL = new Settings(SMALL_MESSAGE);
        static final Settings LARGE = new Settings(LARGE_MESSAGE);
        final OutboundConnection.Type type;
        final Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound;
        final Function<InboundConnectionSettings, InboundConnectionSettings> inbound;
        Settings(OutboundConnection.Type type)
        {
            this(type, Function.identity(), Function.identity());
        }
        Settings(OutboundConnection.Type type, Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound,
                 Function<InboundConnectionSettings, InboundConnectionSettings> inbound)
        {
            this.type = type;
            this.outbound = outbound;
            this.inbound = inbound;
        }
        Settings outbound(Function<OutboundConnectionSettings, OutboundConnectionSettings> outbound)
        {
            return new Settings(type, outbound, inbound);
        }
        Settings inbound(Function<InboundConnectionSettings, InboundConnectionSettings> inbound)
        {
            return new Settings(type, outbound, inbound);
        }
        Settings override(Settings settings)
        {
            return new Settings(settings.type != null ? settings.type : type,
                                outbound.andThen(settings.outbound),
                                inbound.andThen(settings.inbound));
        }
    }

    private static final List<Settings> SETTINGS = ImmutableList.of(
        Settings.SMALL,
        Settings.LARGE,
        Settings.SMALL.outbound(outbound -> outbound.withCompression(true)),
        Settings.LARGE.outbound(outbound -> outbound.withCompression(true))
    );

    private void test(Settings extraSettings, SendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTest(s.override(extraSettings), test);
    }
    private void test(SendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTest(s, test);
    }
    private void doTest(Settings settings, SendTest test) throws Throwable
    {
        InboundConnections inbound = new InboundConnections(new InboundConnectionSettings());
        InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
        MessagingService.instance().removeInbound(endpoint);
        OutboundConnectionSettings outboundSettings = settings.outbound.apply(new OutboundConnectionSettings(endpoint))
                                              .withDefaults(settings.type, current_version);
        ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(outboundSettings.applicationReserveSendQueueEndpointCapacityInBytes), outboundSettings.applicationReserveSendQueueGlobalCapacityInBytes);
        OutboundConnection outbound = new OutboundConnection(settings.type, outboundSettings, reserveCapacityInBytes);
        try
        {
            inbound.open();
            test.accept(MessagingService.instance().getInbound(endpoint), outbound, endpoint);
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
            outbound.close(false);
        }
    }

    @Test
    public void testSendSmall() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int count = 10;
            CountDownLatch received = new CountDownLatch(count);
            Verb._TEST_1.unsafeSetHandler(() -> msg -> received.countDown());
            Message<?> message = Message.out(Verb._TEST_1, emptyMessage);
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            received.await(10L, TimeUnit.SECONDS);
            Assert.assertEquals(10, outbound.getSubmitted());
            Assert.assertEquals(0, outbound.getPending());
            Assert.assertEquals(10, outbound.getSent());
            Assert.assertEquals(10 * message.serializedSize(current_version), outbound.getSentBytes());
            Assert.assertEquals(0, outbound.droppedDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToError());
            Assert.assertEquals(0, outbound.droppedDueToError());
            Assert.assertEquals(0, outbound.droppedDueToTimeout());
            Assert.assertEquals(0, outbound.droppedBytesDueToTimeout());
            Verb._TEST_1.unsafeSetHandler(() -> null);
        });
    }

    @Test
    public void testSendLarge() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int count = 10;
            CountDownLatch received = new CountDownLatch(count);
            Verb._TEST_1.unsafeSetSerializer(() -> new IVersionedSerializer<EmptyMessage>()
            {
                public void serialize(EmptyMessage emptyMessage, DataOutputPlus out, int version) throws IOException
                {
                    for (int i = 0 ; i < LARGE_MESSAGE_THRESHOLD + 1 ; ++i)
                        out.writeByte(i);
                }
                public EmptyMessage deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.skipBytesFully(LARGE_MESSAGE_THRESHOLD + 1);
                    return emptyMessage;
                }
                public long serializedSize(EmptyMessage emptyMessage, int version)
                {
                    return LARGE_MESSAGE_THRESHOLD + 1;
                }
            });
            Verb._TEST_1.unsafeSetHandler(() -> msg -> received.countDown());
            Message<?> message = Message.out(Verb._TEST_1, emptyMessage);
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            received.await(10L, TimeUnit.SECONDS);
            Assert.assertEquals(10, outbound.getSubmitted());
            Assert.assertEquals(0, outbound.getPending());
            Assert.assertEquals(10, outbound.getSent());
            Assert.assertEquals(10 * message.serializedSize(current_version), outbound.getSentBytes());
            Assert.assertEquals(0, outbound.droppedDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToError());
            Assert.assertEquals(0, outbound.droppedDueToError());
            Assert.assertEquals(0, outbound.droppedDueToTimeout());
            Assert.assertEquals(0, outbound.droppedBytesDueToTimeout());
            Verb._TEST_1.unsafeSetHandler(() -> null);
        });
    }

    @Test
    public void testInsufficientSpace() throws Throwable
    {
        test(new Settings(null).outbound(settings -> settings
                                         .withApplicationReserveSendQueueCapacityInBytes(1 << 15, new ResourceLimits.Concurrent(1 << 16))
                                         .withApplicationSendQueueCapacityInBytes(1 << 16)),
             (inbound, outbound, endpoint) -> {

            CountDownLatch done = new CountDownLatch(1);
            Message<?> message = Message.out(Verb._TEST_1, emptyMessage);
            long id = MessagingService.instance().callbacks.addWithExpiration(new IAsyncCallbackWithFailure()
            {
                public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
                {
                    done.countDown();
                }

                public void response(Message msg)
                {
                    throw new IllegalStateException();
                }

                public boolean isLatencyForSnitch()
                {
                    return false;
                }
            }, message, endpoint, Long.MAX_VALUE);
            message = message.withId(id);
            AtomicInteger delivered = new AtomicInteger();
            Verb._TEST_1.unsafeSetSerializer(() -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    for (int i = 0 ; i <= 4 << 16 ; i += 8L)
                        out.writeLong(1L);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.skipBytesFully(4 << 16);
                    return null;
                }

                public long serializedSize(Object o, int version)
                {
                    return 4 << 16;
                }
            });
            Verb._TEST_1.unsafeSetHandler(() -> msg -> delivered.incrementAndGet());
            outbound.enqueue(message);
            done.await(10L, TimeUnit.SECONDS);
            Assert.assertEquals(0, delivered.get());
            Assert.assertEquals(1, outbound.getSubmitted());
            Assert.assertEquals(0, outbound.getPending());
            Assert.assertEquals(0, outbound.getSent());
            Assert.assertEquals(0, outbound.getSentBytes());
            Assert.assertEquals(1, outbound.droppedDueToOverload());
            Assert.assertEquals(message.serializedSize(current_version), outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToError());
            Assert.assertEquals(0, outbound.droppedDueToError());
            Assert.assertEquals(0, outbound.droppedDueToTimeout());
            Assert.assertEquals(0, outbound.droppedBytesDueToTimeout());
        });
    }

    @Test
    public void testSerializeError() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int count = 100;
            CountDownLatch done = new CountDownLatch(100);
            AtomicInteger serialized = new AtomicInteger();
            Message<?> message = Message.out(Verb._TEST_1, emptyMessage);
            Verb._TEST_1.unsafeSetSerializer(() -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    int i = serialized.incrementAndGet();
                    if (0 == (i & 15))
                    {
                        if (0 == (i & 16))
                            out.writeByte(i);
                        done.countDown();
                        throw new IOException();
                    }
                    if (1 != (i & 31))
                        out.writeByte(i);
                    else
                        done.countDown();
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.readByte();
                    return null;
                }

                public long serializedSize(Object o, int version)
                {
                    return 1;
                }
            });
            Verb._TEST_1.unsafeSetHandler(() -> msg -> done.countDown());
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            done.await(30L, TimeUnit.SECONDS);
            Assert.assertEquals(100, outbound.getSubmitted());
            Assert.assertEquals(0, outbound.getPending());
            Assert.assertEquals(90, outbound.getSent());
            Assert.assertEquals(90 * message.serializedSize(current_version), outbound.getSentBytes());
            Assert.assertEquals(0, outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedDueToOverload());
            Assert.assertEquals(10, outbound.droppedDueToError());
            Assert.assertEquals(10 * message.serializedSize(current_version), outbound.droppedBytesDueToError());
            Assert.assertEquals(0, outbound.droppedDueToTimeout());
            Assert.assertEquals(0, outbound.droppedBytesDueToTimeout());
            Assert.assertEquals(0, done.getCount());
        });
    }

    @Test
    public void testTimeout() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int count = 10;
            CountDownLatch enqueueDone = new CountDownLatch(1);
            CountDownLatch deliveryDone = new CountDownLatch(1);
            AtomicInteger delivered = new AtomicInteger();
            Verb._TEST_1.unsafeSetHandler(() -> msg -> delivered.incrementAndGet());
            Message<?> message = Message.builder(Verb._TEST_1, emptyMessage)
                                        .withExpirationTime(ApproximateTime.nanoTime() + TimeUnit.DAYS.toNanos(1L))
                                        .build();
            long sentSize = message.serializedSize(current_version);
            outbound.enqueue(message);
            long timeoutMillis = 10L;
            while (delivered.get() < 1);
            outbound.unsafeRunOnDelivery(() -> Uninterruptibles.awaitUninterruptibly(enqueueDone, 1L, TimeUnit.DAYS));
            message = Message.builder(Verb._TEST_1, emptyMessage)
                             .withExpirationTime(ApproximateTime.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis))
                             .build();
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            Uninterruptibles.sleepUninterruptibly(timeoutMillis * 2, TimeUnit.MILLISECONDS);
            enqueueDone.countDown();
            outbound.unsafeRunOnDelivery(deliveryDone::countDown);
            deliveryDone.await(1L, TimeUnit.MINUTES);
            Assert.assertEquals(1, delivered.get());
            Assert.assertEquals(11, outbound.getSubmitted());
            Assert.assertEquals(0, outbound.getPending());
            Assert.assertEquals(1, outbound.getSent());
            Assert.assertEquals(sentSize, outbound.getSentBytes());
            Assert.assertEquals(0, outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedDueToOverload());
            Assert.assertEquals(0, outbound.droppedDueToError());
            Assert.assertEquals(0, outbound.droppedBytesDueToError());
            Assert.assertEquals(10, outbound.droppedDueToTimeout());
            Assert.assertEquals(10 * message.serializedSize(current_version), outbound.droppedBytesDueToTimeout());
        });
    }

}
