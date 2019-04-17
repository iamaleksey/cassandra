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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.async.ConnectionUtils.*;
import static org.apache.cassandra.net.async.ConnectionUtils.check;
import static org.apache.cassandra.net.async.OutboundConnection.Type.LARGE_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnection.Type.SMALL_MESSAGE;
import static org.apache.cassandra.net.async.OutboundConnections.LARGE_MESSAGE_THRESHOLD;

public class ConnectionTest
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionTest.class);
    private static final SocketFactory factory = new SocketFactory();

    private final Map<Verb, Supplier<? extends IVersionedAsymmetricSerializer<?, ?>>> serializers = new HashMap<>();
    private final Map<Verb, Supplier<? extends IVerbHandler<?>>> handlers = new HashMap<>();

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
        factory.shutdownNow();
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

    static final AcceptVersions legacy = new AcceptVersions(VERSION_30, VERSION_30);

    static final List<Function<Settings, Settings>> MODIFIERS = ImmutableList.of(
        settings -> settings.outbound(outbound -> outbound.withAcceptVersions(legacy))
                            .inbound(inbound -> inbound.withAcceptMessaging(legacy)),
        settings -> settings.outbound(outbound -> outbound.withEncryption(encryptionOptions))
                            .inbound(inbound -> inbound.withEncryption(encryptionOptions)),
        settings -> settings.outbound(outbound -> outbound.withCompression(true))
    );

    static final List<Settings> SETTINGS = applyPowerSet(
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
}
