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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.AcceptVersions;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.async.ConnectionUtils.check;
import static org.apache.cassandra.net.async.OutboundConnection.Type.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnection.Type.SMALL_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

public class ConnectionTest
{
    private final Map<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> serializers = new HashMap<>();
    private final Map<Verb, Supplier<? extends IVerbHandler<?>>> handlers = new HashMap<>();

    private void unsafeSetSerializer(Verb verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> supplier) throws Throwable
    {
        serializers.putIfAbsent(verb, verb.unsafeSetSerializer(supplier));
    }

    private void unsafeSetHandler(Verb verb, Supplier<? extends IVerbHandler<?>> supplier) throws Throwable
    {
        handlers.putIfAbsent(verb, verb.unsafeSetHandler(supplier));
    }

    @After
    public void resetVerbs() throws Throwable
    {
        for (Map.Entry<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> e : serializers.entrySet())
            e.getKey().unsafeSetSerializer(e.getValue());
        for (Map.Entry<Verb, Supplier<? extends IVerbHandler<?>>> e : handlers.entrySet())
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
        NettyFactory.instance.shutdownNow();
    }

    interface SendTest
    {
        void accept(InboundMessageHandlers inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
    }

    interface ManualSendTest
    {
        void accept(Settings settings, InboundSockets inbound, OutboundConnection outbound, InetAddressAndPort endpoint) throws Throwable;
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
            return new Settings(type, this.outbound.andThen(outbound), inbound);
        }
        Settings inbound(Function<InboundConnectionSettings, InboundConnectionSettings> inbound)
        {
            return new Settings(type, outbound, this.inbound.andThen(inbound));
        }
        Settings override(Settings settings)
        {
            return new Settings(settings.type != null ? settings.type : type,
                                outbound.andThen(settings.outbound),
                                inbound.andThen(settings.inbound));
        }
    }

    private static final EncryptionOptions.ServerEncryptionOptions encryptionOptions =
            new EncryptionOptions.ServerEncryptionOptions()
            .withEnabled(true)
            .withLegacySslStoragePort(true)
            .withInternodeEncryption(EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.all)
            .withKeyStore("test/conf/cassandra_ssl_test.keystore")
            .withKeyStorePassword("cassandra")
            .withTrustStore("test/conf/cassandra_ssl_test.truststore")
            .withTrustStorePassword("cassandra")
            .withRequireClientAuth(false)
            .withCipherSuites("TLS_RSA_WITH_AES_128_CBC_SHA");

    private static final AcceptVersions legacy = new AcceptVersions(VERSION_30, VERSION_30);

    private static final List<Function<Settings, Settings>> MODIFIERS = ImmutableList.of(
        settings -> settings.outbound(outbound -> outbound.withAcceptVersions(legacy))
                            .inbound(inbound -> inbound.withAcceptMessaging(legacy)),
        settings -> settings.outbound(outbound -> outbound.withEncryption(encryptionOptions))
                            .inbound(inbound -> inbound.withEncryption(encryptionOptions)),
        settings -> settings.outbound(outbound -> outbound.withCompression(true))
    );

    private static final List<Settings> SETTINGS = applyPowerSet(
        ImmutableList.of(Settings.SMALL, Settings.LARGE),
        MODIFIERS
    );

    private static List<Settings> applyPowerSet(List<Settings> settings, List<Function<Settings, Settings>> modifiers)
    {
        List<Settings> result = new ArrayList<>();
        for (Set<Function<Settings, Settings>> set : Sets.powerSet(new HashSet<>(modifiers)))
        {
            for (Settings s : settings)
            {
                for (Function<Settings, Settings> f : set)
                    s = f.apply(s);
                result.add(s);
            }
        }
        return result;
    }

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
        doTestManual(settings, (ignore, inbound, outbound, endpoint) -> {
            inbound.open().sync();
            test.accept(MessagingService.instance().getInbound(endpoint), outbound, endpoint);
        });
    }

    private void doTestManual(Settings settings, ManualSendTest test) throws Throwable
    {
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();
        InboundConnectionSettings inboundSettings = settings.inbound.apply(new InboundConnectionSettings())
                                                                    .withBindAddress(endpoint);
        InboundSockets inbound = new InboundSockets(Collections.singletonList(inboundSettings));
        OutboundConnectionSettings outboundTemplate = settings.outbound.apply(new OutboundConnectionSettings(endpoint))
                                                                       .withDefaultReserveLimits();
        ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(outboundTemplate.applicationReserveSendQueueEndpointCapacityInBytes), outboundTemplate.applicationReserveSendQueueGlobalCapacityInBytes);
        OutboundConnection outbound = new OutboundConnection(settings.type, outboundTemplate, reserveCapacityInBytes);
        try
        {
            test.accept(settings, inbound, outbound, endpoint);
        }
        finally
        {
            outbound.close(false);
            inbound.close().get(30L, SECONDS);
            outbound.close(false).get(30L, SECONDS);
            resetVerbs();
            MessagingService.instance().messageHandlers.clear();
        }
    }

    @Test
    public void testSendSmall() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 10;
            CountDownLatch received = new CountDownLatch(count);
            unsafeSetHandler(Verb._TEST_1, () -> msg -> received.countDown());
            Message<?> message = Message.out(Verb._TEST_1, noPayload);
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            received.await(10L, SECONDS);
            check(outbound).submitted(10)
                           .sent     (10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .overload ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
            check(inbound) .received (10, 10 * message.serializedSize(version))
                           .processed(10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
        });
    }

    @Test
    public void testSendLarge() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 10;
            CountDownLatch received = new CountDownLatch(count);
            unsafeSetSerializer(Verb._TEST_1, () -> new IVersionedSerializer<Object>()
            {
                public void serialize(Object noPayload, DataOutputPlus out, int version) throws IOException
                {
                    for (int i = 0 ; i < LARGE_MESSAGE_THRESHOLD + 1 ; ++i)
                        out.writeByte(i);
                }
                public Object deserialize(DataInputPlus in, int version) throws IOException
                {
                    in.skipBytesFully(LARGE_MESSAGE_THRESHOLD + 1);
                    return noPayload;
                }
                public long serializedSize(Object noPayload, int version)
                {
                    return LARGE_MESSAGE_THRESHOLD + 1;
                }
            });
            unsafeSetHandler(Verb._TEST_1, () -> msg -> received.countDown());
            Message<?> message = Message.builder(Verb._TEST_1, new Object())
                                        .withExpiresAt(System.nanoTime() + SECONDS.toNanos(30L))
                                        .build();
            for (int i = 0 ; i < count ; ++i)
                outbound.enqueue(message);
            received.await(10L, SECONDS);
            check(outbound).submitted(10)
                           .sent     (10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .overload ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
            check(inbound) .received (10, 10 * message.serializedSize(version))
                           .processed(10, 10 * message.serializedSize(version))
                           .pending  ( 0,  0)
                           .expired  ( 0,  0)
                           .error    ( 0,  0)
                           .check();
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
            Message<?> message = Message.out(Verb._TEST_1, new Object());
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
                 check(outbound).submitted( 1)
                                .sent     ( 0,  0)
                                .pending  ( 0,  0)
                                .overload ( 1,  message.serializedSize(current_version))
                                .expired  ( 0,  0)
                                .error    ( 0,  0)
                                .check();
                 check(inbound) .received ( 0,  0)
                                .processed( 0,  0)
                                .pending  ( 0,  0)
                                .expired  ( 0,  0)
                                .error    ( 0,  0)
                                .check();
        });
    }

    @Test
    public void testSerializeError() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 100;
            CountDownLatch done = new CountDownLatch(100);
            AtomicInteger serialized = new AtomicInteger();
            Message<?> message = Message.builder(Verb._TEST_1, new Object())
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
            done.await(60L, SECONDS);
            Assert.assertEquals(0, done.getCount());
            check(outbound).submitted(100)
                           .sent     ( 90, 90 * message.serializedSize(version))
                           .pending  (  0,  0)
                           .overload (  0,  0)
                           .expired  (  0,  0)
                           .error    ( 10, 10 * message.serializedSize(current_version))
                           .check();
            check(inbound) .received ( 90, 90 * message.serializedSize(version))
                           .processed( 90, 90 * message.serializedSize(version))
                           .pending  (  0,  0)
                           .expired  (  0,  0)
                           .error    (  0,  0)
                           .check();
        });
    }

    @Test
    public void testTimeout() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            int count = 10;
            CountDownLatch enqueueDone = new CountDownLatch(1);
            CountDownLatch deliveryDone = new CountDownLatch(1);
            AtomicInteger delivered = new AtomicInteger();
            Verb._TEST_1.unsafeSetHandler(() -> msg -> delivered.incrementAndGet());
            Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                        .withExpiresAt(ApproximateTime.nanoTime() + TimeUnit.DAYS.toNanos(1L))
                                        .build();
            long sentSize = message.serializedSize(version);
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
            deliveryDone.await(60L, SECONDS);
            Assert.assertEquals(1, delivered.get());
            check(outbound).submitted( 11)
                           .sent     (  1,  sentSize)
                           .pending  (  0,  0)
                           .overload (  0,  0)
                           .expired  ( 10, 10 * message.serializedSize(current_version))
                           .error    (  0,  0)
                           .check();
            check(inbound) .received (  1, sentSize)
                           .processed(  1, sentSize)
                           .pending  (  0,  0)
                           .expired  (  0,  0)
                           .error    (  0,  0)
                           .check();
        });
    }

    @Test
    public void testPre40() throws Throwable
    {
        MessagingService.instance().versions.set(FBUtilities.getBroadcastAddressAndPort(),
                                                 MessagingService.VERSION_30);

        try
        {
            test(new Settings(null).outbound(outbound -> outbound
                                                         .withDefaults(SMALL_MESSAGE, MessagingService.VERSION_30)
                                                         .withAcceptVersions(new MessagingService.AcceptVersions(MessagingService.VERSION_30, MessagingService.VERSION_30))
                                                         // TODO (alexp): This should be fixed and removed when frame decoding for compression and encryption is fixed
                                                         .withEncryption(null)
                                                         .withConnectTo(FBUtilities.getBroadcastAddressAndPort())
                                                         .withCompression(false))
                                   // TODO (alexp): This should be fixed and removed when frame decoding for compression and encryption is fixed
                                   .inbound(settings -> settings.withEncryption(null)
                                   ),
                 (inbound, outbound, endpoint) -> {
                     CountDownLatch latch = new CountDownLatch(1);
                     unsafeSetHandler(Verb._TEST_1,
                                      () -> (msg) -> latch.countDown());

                     Message<?> message = Message.out(Verb._TEST_1, noPayload);
                     outbound.enqueue(message);
                     latch.await(10, SECONDS);
                     Assert.assertEquals(0, latch.getCount());
                     Assert.assertTrue(outbound.isConnected());
                 });
        }
        finally
        {
            MessagingService.instance().versions.set(FBUtilities.getBroadcastAddressAndPort(),
                                                     current_version);
        }
    }

    @Test
    public void testCloseIfEndpointDown() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint) -> {
            Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                        .withExpiresAt(System.nanoTime() + SECONDS.toNanos(30L))
                                        .build();

            for (int i = 0 ; i < 1000 ; ++i)
                outbound.enqueue(message);

            outbound.close(true).get(10L, MINUTES);
        });
    }

    @Test
    public void testMessagePurging() throws Throwable
    {
        testManual((settings, inbound, outbound, endpoint) -> {
            Runnable testWhileDisconnected = () -> {
                try
                {
                    for (int i = 0; i < 5; i++)
                    {
                        Message<?> message = Message.builder(Verb._TEST_1, noPayload)
                                                    .withExpiresAt(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(50L))
                                                    .build();
                        outbound.enqueue(message);
                        Assert.assertFalse(outbound.isConnected());
                        Assert.assertEquals(1, outbound.queueSize());
                        CompletableFuture.runAsync(() -> {
                            while (outbound.queueSize() > 0 && !Thread.interrupted()) {}
                        }).get(10, SECONDS);
                        // Message should have been purged
                        Assert.assertEquals(0, outbound.queueSize());
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
                inbound.open().sync();
                CountDownLatch latch = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> latch.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));
                Assert.assertEquals(1, outbound.queueSize());
                latch.await(10, SECONDS);
                Assert.assertEquals(0, latch.getCount());
                Assert.assertEquals(0, outbound.queueSize());
            }
            finally
            {
                inbound.close().get(10, SECONDS);
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
        testManual((settings, inbound, outbound, endpoint) -> {
            try
            {
                inbound.open().sync();
                CountDownLatch latch = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> latch.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));
                latch.await(10, SECONDS);
                Assert.assertEquals(latch.getCount(), 0);

                // Simulate disconnect
                inbound.close().get(10, SECONDS);
                MessagingService.instance().removeInbound(endpoint);
                inbound = new InboundSockets(settings.inbound.apply(new InboundConnectionSettings()));
                inbound.open().sync();

                CountDownLatch latch2 = new CountDownLatch(1);
                unsafeSetHandler(Verb._TEST_1, () -> msg -> latch2.countDown());
                outbound.enqueue(Message.out(Verb._TEST_1, noPayload));

                latch2.await(10, SECONDS);
                Assert.assertEquals(latch2.getCount(), 0);
            }
            finally
            {
                inbound.close().get(10, SECONDS);
                outbound.close(false).get(10, SECONDS);
            }
        });
    }

    @Test
    public void testRecoverableCorruptedMessageDelivery() throws Throwable
    {
        test((inbound, outbound, endpoint) -> {
            int version = outbound.settings().acceptVersions.max;
            if (version < VERSION_40)
                return;

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
            int version = outbound.settings().acceptVersions.max;
            if (version < VERSION_40)
                return;

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
            int version = outbound.settings().acceptVersions.max;
            if (version < VERSION_40)
                return;

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

}
