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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.MessageGenerator.UniformPayloadGenerator;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.lang.Math.min;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.utils.ApproximateTime.Measurement.ALMOST_SAME_TIME;
import static org.apache.cassandra.utils.ExecutorUtils.runWithThreadName;

public class ConnectionBurnTest
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionBurnTest.class);

    static
    {
        // stop updating ALMOST_SAME_TIME so that we get consistent message expiration times
        ApproximateTime.stop(ALMOST_SAME_TIME);
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCrossNodeTimeout(true);
    }

    static class NoGlobalInboundMetrics implements InboundMessageHandlers.GlobalMetricCallbacks
    {
        static final NoGlobalInboundMetrics instance = new NoGlobalInboundMetrics();
        public LatencyConsumer internodeLatencyRecorder(InetAddressAndPort to)
        {
            return (timeElapsed, timeUnit) -> {};
        }
        public void recordInternalLatency(Verb verb, long timeElapsed, TimeUnit timeUnit) {}
        public void recordDroppedMessage(Verb verb, long timeElapsed, TimeUnit timeUnit) {}
    }

    static class Inbound
    {
        final Map<InetAddressAndPort, Map<InetAddressAndPort, InboundMessageHandlers>> handlersByRecipientThenSender;
        final InboundSockets sockets;

        Inbound(List<InetAddressAndPort> endpoints, GlobalInboundSettings settings, Test test)
        {
            final InboundMessageHandlers.GlobalResourceLimits globalInboundLimits = new InboundMessageHandlers.GlobalResourceLimits(new ResourceLimits.Concurrent(settings.globalReserveLimit));
            Map<InetAddressAndPort, Map<InetAddressAndPort, InboundMessageHandlers>> handlersByRecipientThenSender = new HashMap<>();
            List<InboundConnectionSettings> bind = new ArrayList<>();
            for (InetAddressAndPort recipient : endpoints)
            {
                Map<InetAddressAndPort, InboundMessageHandlers> handlersBySender = new HashMap<>();
                for (InetAddressAndPort sender : endpoints)
                    handlersBySender.put(sender, new InboundMessageHandlers(recipient, sender, settings.queueCapacity, settings.endpointReserveLimit, globalInboundLimits, NoGlobalInboundMetrics.instance, test, test));

                handlersByRecipientThenSender.put(recipient, handlersBySender);
                bind.add(settings.template.withHandlers(handlersBySender::get).withBindAddress(recipient));
            }
            this.sockets = new InboundSockets(bind);
            this.handlersByRecipientThenSender = handlersByRecipientThenSender;
        }
    }


    private static class Test implements InboundMessageHandlers.HandlerProvider, InboundMessageHandlers.MessageConsumer
    {
        private final IVersionedSerializer<byte[]> serializer = new IVersionedSerializer<byte[]>()
        {
            public void serialize(byte[] payload, DataOutputPlus out, int version) throws IOException
            {
                long id = MessageGenerator.getId(payload);
                forId(id).serialize(id, payload, out, version);
            }

            public byte[] deserialize(DataInputPlus in, int version) throws IOException
            {
                MessageGenerator.Header header = MessageGenerator.readHeader(in, version);
                return forId(header.id).deserialize(header, in, version);
            }

            public long serializedSize(byte[] payload, int version)
            {
                return MessageGenerator.serializedSize(payload, version);
            }
        };

        static class Builder
        {
            long time;
            TimeUnit timeUnit;
            int endpoints;
            MessageGenerators generators;
            OutboundConnectionSettings outbound;
            GlobalInboundSettings inbound;
            public Builder time(long time, TimeUnit timeUnit) { this.time = time; this.timeUnit = timeUnit; return this; }
            public Builder endpoints(int endpoints) { this.endpoints = endpoints; return this; }
            public Builder inbound(GlobalInboundSettings inbound) { this.inbound = inbound; return this; }
            public Builder outbound(OutboundConnectionSettings outbound) { this.outbound = outbound; return this; }
            public Builder generators(MessageGenerators generators) { this.generators = generators; return this; }
            Test build() { return new Test(endpoints, generators, inbound, outbound, timeUnit.toNanos(time)); }
        }

        static Builder builder() { return new Builder(); }

        private static final int messageIdsPerConnection = 1 << 20;

        final long runForNanos;
        final int version;
        final List<InetAddressAndPort> endpoints;
        final Inbound inbound;
        final Connection[] connections;
        final long[] connectionMessageIds;
        final ExecutorService executor = Executors.newCachedThreadPool();
        final Map<Pair<InetAddressAndPort, InetAddressAndPort>, Connection> connectionLookup = new HashMap<>();

        private Test(int simulateEndpoints, MessageGenerators messageGenerators, GlobalInboundSettings inboundSettings, OutboundConnectionSettings outboundTemplate, long runForNanos)
        {
            this.endpoints = endpoints(simulateEndpoints);
            this.inbound = new Inbound(endpoints, inboundSettings, this);
            this.connections = new Connection[endpoints.size() * endpoints.size() * 3];
            this.connectionMessageIds = new long[connections.length];
            this.version = outboundTemplate.acceptVersions == null ? current_version : outboundTemplate.acceptVersions.max;
            this.runForNanos = runForNanos;

            int i = 0;
            long minId = 0, maxId = messageIdsPerConnection - 1;
            for (InetAddressAndPort recipient : endpoints)
            {
                for (InetAddressAndPort sender : endpoints)
                {
                    InboundMessageHandlers inboundHandlers = inbound.handlersByRecipientThenSender.get(recipient).get(sender);
                    OutboundConnectionSettings template = outboundTemplate.withDefaultReserveLimits();
                    ResourceLimits.Limit reserveEndpointCapacityInBytes = new ResourceLimits.Concurrent(template.applicationReserveSendQueueEndpointCapacityInBytes);
                    ResourceLimits.EndpointAndGlobal reserveCapacityInBytes = new ResourceLimits.EndpointAndGlobal(reserveEndpointCapacityInBytes, template.applicationReserveSendQueueGlobalCapacityInBytes);
                    for (ConnectionType type : ConnectionType.MESSAGING_TYPES)
                    {
                        Connection connection = new Connection(sender, recipient, type, inboundHandlers, template, reserveCapacityInBytes, messageGenerators.get(type), minId, maxId);
                        this.connections[i] = connection;
                        this.connectionMessageIds[i] = minId;
                        connectionLookup.put(Pair.create(sender, recipient), connection);
                        minId = maxId + 1;
                        maxId += messageIdsPerConnection;
                        ++i;
                    }
                }
            }
        }

        Connection forId(long messageId)
        {
            int i = Arrays.binarySearch(connectionMessageIds, messageId);
            if (i < 0) i = -2 -i;
            Connection connection = connections[i];
            assert connection.minId <= messageId && connection.maxId >= messageId;
            return connection;
        }

        public void run() throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, TimeoutException
        {
            Reporters reporters = new Reporters(endpoints, connections);
            try
            {
                long deadline = System.nanoTime() + runForNanos;
                Verb._TEST_2.unsafeSetHandler(() -> message -> {});
                Verb._TEST_2.unsafeSetSerializer(() -> serializer);
                inbound.sockets.open().get();

                CountDownLatch failed = new CountDownLatch(1);
                for (Connection connection : connections)
                    connection.startVerifier(failed::countDown, executor, deadline);

                for (int i = 0 ; i < 2 * connections.length ; ++i)
                {
                    executor.execute(() -> {
                        try
                        {
                            ThreadLocalRandom random = ThreadLocalRandom.current();
                            while (ApproximateTime.nanoTime() < deadline && !Thread.currentThread().isInterrupted())
                            {
                                Connection connection = connections[random.nextInt(connections.length)];
                                if (!connection.registerSender())
                                    continue;

                                try
                                {
                                    Thread.currentThread().setName("Generate-" + connection.linkId);
                                    int count = 0;
                                    switch (random.nextInt() & 3)
                                    {
                                        case 0: count = random.nextInt(100, 200); break;
                                        case 1: count = random.nextInt(200, 1000); break;
                                        case 2: count = random.nextInt(1000, 2000); break;
                                        case 3: count = random.nextInt(2000, 10000); break;
                                    }

                                    if (connection.outbound.type() == ConnectionType.LARGE_MESSAGES)
                                        count /= 2;

                                    while (connection.isSending()
                                           && count-- > 0
                                           && ApproximateTime.nanoTime() < deadline
                                           && !Thread.currentThread().isInterrupted())
                                        connection.sendOne();
                                }
                                finally
                                {
                                    connection.unregisterSender();
                                }
                            }
                        }
                        catch (Throwable t)
                        {
                            if (t instanceof InterruptedException)
                                return;
                            logger.error("Unexpected exception", t);
                            failed.countDown();
                        }
                    });
                }

                executor.execute(() -> {
                    Thread.currentThread().setName("Test-SetInFlight");
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    List<Connection> connections = new ArrayList<>(Arrays.asList(this.connections));
                    while (!Thread.currentThread().isInterrupted())
                    {
                        Collections.shuffle(connections);
                        int total = random.nextInt(1 << 20, 128 << 20);
                        for (int i = connections.size() - 1; i >= 1 ; --i)
                        {
                            int average = total / (i + 1);
                            int max = random.nextInt(1, min(2 * average, total - 2));
                            int min = random.nextInt(0, max);
                            connections.get(i).controller.setInFlightByteBounds(min, max);
                            total -= max;
                        }
                        connections.get(0).controller.setInFlightByteBounds(random.nextInt(0, total), total);
                        Uninterruptibles.sleepUninterruptibly(1L, TimeUnit.SECONDS);
                    }
                });

                // TODO: slowly modify the pattern of interrupts, from often to infrequent
                executor.execute(() -> {
                    Thread.currentThread().setName("Test-Reconnect");
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while (deadline > System.nanoTime())
                    {
                        try
                        {
                            Thread.sleep(random.nextInt(60000));
                        }
                        catch (InterruptedException e)
                        {
                            break;
                        }
                        Connection connection = connections[random.nextInt(connections.length)];
                        OutboundConnectionSettings template = connection.outboundTemplate;
                        template = ConnectionTest.SETTINGS.get(random.nextInt(ConnectionTest.SETTINGS.size()))
                                   .outbound.apply(template);
                        connection.reconnect(template);
                    }
                });

                executor.execute(() -> {
                    Thread.currentThread().setName("Test-Sync");
                    ThreadLocalRandom random = ThreadLocalRandom.current();
                    while (deadline > System.nanoTime())
                    {
                        try
                        {
                            Thread.sleep(random.nextInt(10000));
                            CountDownLatch latch = new CountDownLatch(1);
                            Connection connection = connections[random.nextInt(connections.length)];
                            connection.sync(latch::countDown);
                            latch.await();
                        }
                        catch (InterruptedException e)
                        {
                            break;
                        }
                    }
                });

                while (deadline > System.nanoTime() && failed.getCount() > 0)
                {
                    reporters.update();
                    reporters.print();
                    Uninterruptibles.awaitUninterruptibly(failed, 30L, TimeUnit.SECONDS);
                }

                executor.shutdownNow();
                ExecutorUtils.awaitTermination(1L, TimeUnit.MINUTES, executor);
            }
            finally
            {
                reporters.update();
                reporters.print();

                inbound.sockets.close().get();
                new FutureCombiner(Arrays.stream(connections)
                                         .map(c -> c.outbound.close(false))
                                         .collect(Collectors.toList()))
                .get();
            }
        }

        class WrappedInboundCallbacks implements InboundMessageCallbacks
        {
            final InboundMessageCallbacks wrapped;
            WrappedInboundCallbacks(InboundMessageCallbacks wrapped)
            {
                this.wrapped = wrapped;
            }

            public void onArrived(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onArrived(messageSize, header, timeElapsed, unit);
                wrapped.onArrived(messageSize, header, timeElapsed, unit);
            }

            public void onDispatched(int messageSize, Message.Header header)
            {
                forId(header.id).onDispatched(messageSize, header);
                wrapped.onDispatched(messageSize, header);
            }

            public void onExecuting(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onExecuting(messageSize, header, timeElapsed, unit);
                wrapped.onExecuting(messageSize, header, timeElapsed, unit);
            }

            public void onExecuted(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onExecuted(messageSize, header, timeElapsed, unit);
                wrapped.onExecuted(messageSize, header, timeElapsed, unit);
            }

            public void onProcessed(int messageSize, Message.Header header)
            {
                forId(header.id).onProcessed(messageSize, header);
                wrapped.onProcessed(messageSize, header);
            }

            public void onExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onExpired(messageSize, header, timeElapsed, unit);
                wrapped.onExpired(messageSize, header, timeElapsed, unit);
            }

            public void onArrivedExpired(int messageSize, Message.Header header, long timeElapsed, TimeUnit unit)
            {
                forId(header.id).onArrivedExpired(messageSize, header, timeElapsed, unit);
                wrapped.onArrivedExpired(messageSize, header, timeElapsed, unit);
            }

            public void onFailedDeserialize(int messageSize, Message.Header header, Throwable t)
            {
                forId(header.id).onFailedDeserialize(messageSize, header, t);
                wrapped.onFailedDeserialize(messageSize, header, t);
            }
        }

        public void fail(Message.Header header, Throwable failure)
        {
            // TODO: verify this
        }

        public void accept(Message<?> message)
        {
            forId(message.id()).process(message);
        }

        public InboundMessageCallbacks wrap(InboundMessageCallbacks wrap)
        {
            return new WrappedInboundCallbacks(wrap);
        }

        public InboundMessageHandler provide(
            FrameDecoder decoder,
            ConnectionType type,
            Channel channel,
            InetAddressAndPort self,
            InetAddressAndPort peer,
            int version,
            int largeMessageThreshold,
            int queueCapacity,
            ResourceLimits.Limit endpointReserveCapacity,
            ResourceLimits.Limit globalReserveCapacity,
            InboundMessageHandler.WaitQueue endpointWaitQueue,
            InboundMessageHandler.WaitQueue globalWaitQueue,
            InboundMessageHandler.OnHandlerClosed onClosed,
            InboundMessageCallbacks callbacks,
            Consumer<Message<?>> messageSink
            )
        {
            return new InboundMessageHandler(decoder, type, channel, self, peer, version, largeMessageThreshold, queueCapacity, endpointReserveCapacity, globalReserveCapacity, endpointWaitQueue, globalWaitQueue, onClosed, wrap(callbacks), messageSink)
            {
                final IntConsumer releaseCapacity = size -> super.releaseProcessedCapacity(size, null);
                protected void releaseProcessedCapacity(int bytes, Message.Header header)
                {
                    forId(header.id).controller.process(bytes, releaseCapacity);
                }
            };
        }

    }

    static List<InetAddressAndPort> endpoints(int count)
    {
        return IntStream.rangeClosed(1, count)
                        .mapToObj(ConnectionBurnTest::endpoint)
                        .collect(Collectors.toList());
    }

    private static InetAddressAndPort endpoint(int i)
    {
        try
        {
            return InetAddressAndPort.getByName("127.0.0." + i);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void test(GlobalInboundSettings inbound, OutboundConnectionSettings outbound) throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, TimeoutException
    {
        MessageGenerator small = new UniformPayloadGenerator(0, 1, (1 << 15));
        MessageGenerator large = new UniformPayloadGenerator(0, 1, (1 << 16) + (1 << 15));
        MessageGenerators generators = new MessageGenerators(small, large);
        outbound = outbound.withApplicationSendQueueCapacityInBytes(1 << 18)
                           .withApplicationReserveSendQueueCapacityInBytes(1 << 30, new ResourceLimits.Concurrent(Integer.MAX_VALUE));

        Test.builder()
            .generators(generators)
            .endpoints(4)
            .inbound(inbound)
            .outbound(outbound)
            .time(2L, TimeUnit.DAYS)
            .build().run();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, TimeoutException
    {
        GlobalInboundSettings inboundSettings = new GlobalInboundSettings()
                                                .withQueueCapacity(1 << 18)
                                                .withEndpointReserveLimit(1 << 20)
                                                .withGlobalReserveLimit(1 << 21)
                                                .withTemplate(new InboundConnectionSettings()
                                                              .withEncryption(ConnectionTest.encryptionOptions));

        test(inboundSettings, new OutboundConnectionSettings(null)
                              .withTcpUserTimeoutInMS(0));
        MessagingService.instance().socketFactory.shutdownNow();
    }

}
