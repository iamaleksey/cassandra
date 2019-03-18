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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ApproximateTime;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.OutboundConnection.Type.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnection.Type.SMALL_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

public class OutboundConnectionTest
{
    private final Map<Verb, Supplier<IVersionedSerializer<?>>> serializers = new HashMap<>();
    private final Map<Verb, Supplier<IVerbHandler<?>>> handlers = new HashMap<>();

    private void unsafeSetSerializer(Verb verb, Supplier<IVersionedSerializer<?>> supplier) throws Throwable
    {
        serializers.putIfAbsent(verb, verb.unsafeSetSerializer(supplier));
    }

    private void unsafeSetHandler(Verb verb, Supplier<IVerbHandler<?>> supplier) throws Throwable
    {
        handlers.putIfAbsent(verb, verb.unsafeSetHandler(supplier));
    }

    @After
    public void resetVerbs() throws Throwable
    {
        for (Map.Entry<Verb, Supplier<IVersionedSerializer<?>>> e : serializers.entrySet())
            e.getKey().unsafeSetSerializer(e.getValue());
        for (Map.Entry<Verb, Supplier<IVerbHandler<?>>> e : handlers.entrySet())
            e.getKey().unsafeSetHandler(e.getValue());
    }

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

    interface ManualSendTest
    {
        void accept(InboundConnections inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
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

    private void testManual(ManualSendTest test) throws Throwable
    {
        for (Settings s : SETTINGS)
            doTestManual(s, test);
    }

    private void doTest(Settings settings, SendTest test) throws Throwable
    {
        doTestManual(settings, (inbound, outbound, endpoint) -> {
            inbound.open();
            test.accept(MessagingService.instance().getInbound(endpoint), outbound, endpoint);
        });
    }

    private void doTestManual(Settings settings, ManualSendTest test) throws Throwable
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
            test.accept(inbound, outbound, endpoint);
        }
        finally
        {
            waitOrFail(inbound.close(), 10L, SECONDS);
            waitOrFail(outbound.close(false), 10L, SECONDS);
            resetVerbs();
            MessagingService.instance().messageHandlers.clear();
        }
    }

    @Test
    public void testSendSmall() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int count = 10;
            CountDownLatch received = new CountDownLatch(count);
            unsafeSetHandler(Verb._TEST_1, () -> msg -> received.countDown());
            Message<?> message = Message.out(Verb._TEST_1, noPayload);
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            received.await(10L, SECONDS);
            Assert.assertEquals(10, outbound.submitted());
            Assert.assertEquals(0, outbound.pending());
            Assert.assertEquals(10, outbound.sent());
            Assert.assertEquals(10 * message.serializedSize(current_version), outbound.sentBytes());
            Assert.assertEquals(0, outbound.droppedDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToError());
            Assert.assertEquals(0, outbound.droppedDueToError());
            Assert.assertEquals(0, outbound.droppedDueToTimeout());
            Assert.assertEquals(0, outbound.droppedBytesDueToTimeout());
        });
    }

    @Test
    public void testSendLarge() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int count = 10;
            CountDownLatch received = new CountDownLatch(count);
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<NoPayload>()
            {
                public void serialize(NoPayload noPayload, DataOutputPlus out, int version) throws IOException
                {
                    for (int i = 0 ; i < LARGE_MESSAGE_THRESHOLD + 1 ; ++i)
                        out.writeByte(i);
                }
                public NoPayload deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.skipBytesFully(LARGE_MESSAGE_THRESHOLD + 1);
                    return noPayload;
                }
                public long serializedSize(NoPayload noPayload, int version)
                {
                    return LARGE_MESSAGE_THRESHOLD + 1;
                }
            });
            unsafeSetHandler(Verb._TEST_1, () -> msg -> received.countDown());
            Message<?> message = Message.out(Verb._TEST_1, noPayload);
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            received.await(10L, SECONDS);
            Assert.assertEquals(10, outbound.submitted());
            Assert.assertEquals(0, outbound.pending());
            Assert.assertEquals(10, outbound.sent());
            Assert.assertEquals(10 * message.serializedSize(current_version), outbound.sentBytes());
            Assert.assertEquals(0, outbound.droppedDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedBytesDueToError());
            Assert.assertEquals(0, outbound.droppedDueToError());
            Assert.assertEquals(0, outbound.droppedDueToTimeout());
            Assert.assertEquals(0, outbound.droppedBytesDueToTimeout());
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
            Message<?> message = Message.out(Verb._TEST_1, noPayload);
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
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
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
            unsafeSetHandler(Verb._TEST_1, () -> msg -> delivered.incrementAndGet());
            outbound.enqueue(message);
            done.await(10L, SECONDS);
            Assert.assertEquals(0, delivered.get());
            Assert.assertEquals(1, outbound.submitted());
            Assert.assertEquals(0, outbound.pending());
            Assert.assertEquals(0, outbound.sent());
            Assert.assertEquals(0, outbound.sentBytes());
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
            Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                        .withExpiresAt(System.nanoTime() + SECONDS.toNanos(30L))
                                        .build();
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
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
            unsafeSetHandler(Verb._TEST_1, () -> msg -> done.countDown());
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            done.await(30L, SECONDS);
            Assert.assertEquals(100, outbound.submitted());
            Assert.assertEquals(0, outbound.pending());
            Assert.assertEquals(90, outbound.sent());
            Assert.assertEquals(90 * message.serializedSize(current_version), outbound.sentBytes());
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
            Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                        .withExpiresAt(ApproximateTime.nanoTime() + TimeUnit.DAYS.toNanos(1L))
                                        .build();
            long sentSize = message.serializedSize(current_version);
            outbound.enqueue(message);
            long timeoutMillis = 10L;
            while (delivered.get() < 1);
            outbound.unsafeRunOnDelivery(() -> Uninterruptibles.awaitUninterruptibly(enqueueDone, 1L, TimeUnit.DAYS));
            message = Message.builder(Verb._TEST_1, noPayload)
                             .withExpiresAt(ApproximateTime.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis))
                             .build();
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            Uninterruptibles.sleepUninterruptibly(timeoutMillis * 2, TimeUnit.MILLISECONDS);
            enqueueDone.countDown();
            outbound.unsafeRunOnDelivery(deliveryDone::countDown);
            deliveryDone.await(1L, MINUTES);
            Assert.assertEquals(1, delivered.get());
            Assert.assertEquals(11, outbound.submitted());
            Assert.assertEquals(0, outbound.pending());
            Assert.assertEquals(1, outbound.sent());
            Assert.assertEquals(sentSize, outbound.sentBytes());
            Assert.assertEquals(0, outbound.droppedBytesDueToOverload());
            Assert.assertEquals(0, outbound.droppedDueToOverload());
            Assert.assertEquals(0, outbound.droppedDueToError());
            Assert.assertEquals(0, outbound.droppedBytesDueToError());
            Assert.assertEquals(10, outbound.droppedDueToTimeout());
            Assert.assertEquals(10 * message.serializedSize(current_version), outbound.droppedBytesDueToTimeout());
        });
    }

    @Test
    public void testCloseIfEndpointDown() throws Throwable
    {
        testManual((inbound, outbound, endpoint) -> {
            Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                        .withExpiresAt(System.nanoTime() + SECONDS.toNanos(30L))
                                        .build();

            for (int i = 0 ; i < 1000 ; ++i)
                outbound.enqueue(message);

            waitOrFail(outbound.close(true), 10L, MINUTES);
        });
    }

    @Test
    public void testMessagePurging() throws Throwable
    {
        testManual((inbound, outbound, endpoint) -> {
            Runnable testWhileDisconnected = () -> {
                try
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Message<?> message = Message.out(Verb._TEST_1, noPayload);
                        outbound.enqueue(message);
                        Assert.assertFalse(outbound.isConnected());
                        Assert.assertEquals(outbound.queueSize(), 1);
                        CompletableFuture.runAsync(() -> {
                            while (outbound.queueSize() > 0 && !Thread.interrupted()) {}
                        }).get(10, SECONDS);
                        // Message should have been purged
                        Assert.assertEquals(outbound.queueSize(), 0);
                    }
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }
            };

            testWhileDisconnected.run();

            try
            {
                inbound.open();
                CountDownLatch latch = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> latch.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));
                Assert.assertEquals(outbound.queueSize(), 1);
                latch.await(10, SECONDS);
            }
            finally
            {
                waitOrFail(inbound.close(), 10, SECONDS);
                // Wait until disconnected
                CompletableFuture.runAsync(() -> {
                    while (outbound.isConnected() && !Thread.interrupted()) {}
                }).get(10, SECONDS);
            }

            testWhileDisconnected.run();
        });
    }

    @Test
    public void testMessageDeliveryOnReconnect() throws Throwable
    {
        testManual((inbound, outbound, endpoint) -> {
            try
            {
                inbound.open();
                CountDownLatch latch = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> latch.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));
                latch.await(10, SECONDS);
                Assert.assertEquals(latch.getCount(), 0);

                // Simulate disconnect
                waitOrFail(inbound.close(), 10, SECONDS);
                MessagingService.instance().removeInbound(endpoint);
                inbound = new InboundConnections(new InboundConnectionSettings());
                inbound.open();

                CountDownLatch latch2 = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> latch2.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));

                latch2.await(10, SECONDS);
                Assert.assertEquals(latch2.getCount(), 0);
            }
            finally
            {
                waitOrFail(inbound.close(), 10, SECONDS);
                waitOrFail(outbound.close(false), 10, SECONDS);
            }
        });
    }

    @Test
    public void testRecoverableCorruptedMessageDelivery() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            AtomicInteger counter = new AtomicInteger();
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    out.writeInt((Integer) o);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    if (counter.getAndIncrement() == 3)
                        throw new IOException();

                    return in.readInt();
                }

                public long serializedSize(Object o, int version)
                {
                    return Integer.BYTES;
                }
            });

            // Connect
            connect(outbound);

            CountDownLatch latch = new CountDownLatch(4);
            unsafeSetHandler(Verb._TEST_1, () -> message -> latch.countDown());
            for (int i = 0; i < 5; i++)
                outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));

            latch.await(10, SECONDS);
            Assert.assertEquals(latch.getCount(), 0);
            Assert.assertEquals(counter.get(), 6);
        });
    }

    @Test
    public void testUnrecoverableCorruptedMessageDelivery() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            AtomicInteger counter = new AtomicInteger();
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    out.writeInt((Integer) o);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    if (counter.getAndIncrement() == 3)
                        throw new RuntimeException();

                    return in.readInt();
                }

                public long serializedSize(Object o, int version)
                {
                    return Integer.BYTES;
                }
            });

            connect(outbound);
            for (int i = 0; i < 5; i++)
                outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));
            CompletableFuture.runAsync(() -> {
                while (outbound.isConnected() && !Thread.interrupted()) {}
            }).get(10, SECONDS);
            Assert.assertFalse(outbound.isConnected());
            Assert.assertEquals(inbound.errorCount(), 1);

            connect(outbound);
        });
    }

    @Test
    public void testCRCCorruption() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object o, DataOutputPlus out, int version) throws IOException
                {
                    out.writeInt((Integer) o);
                }

                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    return in.readInt();
                }

                public long serializedSize(Object o, int version)
                {
                    return Integer.BYTES;
                }
            });

            connect(outbound);

            outbound.unsafeGetChannel().pipeline().addFirst(new ChannelOutboundHandlerAdapter() {
                public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                    ByteBuf bb = (ByteBuf) msg;
                    bb.setByte(0, 0xAB);
                    ctx.write(msg, promise);
                }
            });
            outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));
            CompletableFuture.runAsync(() -> {
                while (outbound.isConnected() && !Thread.interrupted()) {}
            }).get(10, SECONDS);
            Assert.assertFalse(outbound.isConnected());
            // TODO: count corruptions

            connect(outbound);
        });
    }

    @Test
    public void testAcquireRelease() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            ExecutorService executor = Executors.newFixedThreadPool(100);
            int acquireStep = 123;
            AtomicLong acquisitions = new AtomicLong();
            AtomicLong releases = new AtomicLong();
            AtomicLong acquisitionFailures = new AtomicLong();
            for (int i = 0; i < 100; i++)
            {
                executor.submit(() -> {
                    for (int j = 0; j < 10000; j++)
                    {
                        if (outbound.unsafeAcquireCapacity(acquireStep))
                            acquisitions.incrementAndGet();
                        else
                            acquisitionFailures.incrementAndGet();
                    }

                });
            }

            for (int i = 0; i < 100; i++)
            {
                executor.submit(() -> {
                    for (int j = 0; j < 10000; j++)
                    {
                        outbound.unsafeReleaseCapacity(acquireStep);
                        releases.incrementAndGet();
                    }

                });
            }

            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            // We can release more than we acquire, which certainly should not happen in
            // real life, but since it's a test just for acquisition and release, it is fine
            Assert.assertEquals(-1 * acquisitionFailures.get() * acquireStep, outbound.pendingBytes());
        });
    }

    private void connect(OutboundConnection outbound) throws Throwable
    {
        CountDownLatch latch = new CountDownLatch(1);
        unsafeSetHandler(Verb._TEST_1, () -> message -> latch.countDown());
        outbound.enqueue(Message.out(Verb._TEST_1, 0xffffffff));
        latch.await(10, SECONDS);
        Assert.assertTrue(outbound.isConnected());
    }

    private static void waitOrFail(Future<?> future, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException
    {
        future.await(timeout, unit);
        Throwable cause = future.cause();
        if (!future.isSuccess())
        {
            if (cause != null)
                throw new ExecutionException(cause);
            throw new TimeoutException();
        }
    }

}
