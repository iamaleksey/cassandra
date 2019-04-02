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
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.junit.Assert;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.InboundMessageHandler.MessageProcessor;
import org.apache.cassandra.net.async.MessageGenerator.UniformPayloadGenerator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.net.MessagingService.current_version;

public class ConnectionBurnTest extends ConnectionTest
{
    static Map<InetAddressAndPort, OutboundConnections> getOutbound(List<InetAddressAndPort> endpoints, OutboundConnectionSettings template)
    {
        Map<InetAddressAndPort, OutboundConnections> result = new HashMap<>();
        for (InetAddressAndPort endpoint : endpoints)
            result.put(endpoint, OutboundConnections.unsafeCreate(template.toEndpoint(endpoint), null));
        return result;
    }

    private static final class MessageGenerators
    {
        final MessageGenerator small;
        final MessageGenerator large;

        private MessageGenerators(MessageGenerator small, MessageGenerator large)
        {
            this.small = small;
            this.large = large;
        }

        MessageGenerator get(OutboundConnection connection)
        {
            switch (connection.type())
            {
                case SMALL_MESSAGE:
                case URGENT:
                    return small;
                case LARGE_MESSAGE:
                    return large;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    private static final IVersionedSerializer<byte[]> serializer = new IVersionedSerializer<byte[]>()
    {
        public void serialize(byte[] t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(t.length);
            out.write(t);
        }

        public byte[] deserialize(DataInputPlus in, int version) throws IOException
        {
            int length = Ints.checkedCast(in.readUnsignedVInt());
            byte[] result = new byte[length];
            in.readFully(result);
            return result;
        }

        public long serializedSize(byte[] t, int version)
        {
            return t.length + VIntCoding.computeUnsignedVIntSize(t.length);
        }
    };

    private static class Test implements MessageProcessor, MessageCallbacks
    {

        private static final int messageIdsPerChannel = 1 << 20;
        private static final int messageIdsPerConnection = messageIdsPerChannel * 4;
        final int version;
        final List<InetAddressAndPort> endpoints;
        final Map<InetAddressAndPort, OutboundConnections> outbound;
        final Inbound inbound;
        final PerConnectionLookup testRunners;
        final ExecutorService executor = Executors.newCachedThreadPool();

        private Test(int simulateEndpoints, MessageGenerators messageGenerators, GlobalInboundSettings inboundSettings, OutboundConnectionSettings outboundTemplate)
        {
            this.endpoints = endpoints(simulateEndpoints);
            this.outbound = getOutbound(endpoints, outboundTemplate);
            this.inbound = new Inbound(endpoints, inboundSettings, this, this::wrap);
            this.testRunners = new PerConnectionLookup(outbound.values(), messageGenerators);
            this.version = outboundTemplate.acceptVersions.max;
        }

        public void run() throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException
        {
            try
            {
                Verb._TEST_2.unsafeSetSerializer(() -> serializer);
                inbound.sockets.open().get();
                List<Future<?>> results = Stream.of(testRunners.perConnections).flatMap(runner -> Stream.of(runner.perChannels))
                                                                     .map(runner -> executor.submit(() -> {
                          while (true)
                              runner.sendOne();
                      })).collect(Collectors.toList());
                FBUtilities.waitOnFutures(results);
            }
            finally
            {
                inbound.sockets.close().get();
                new FutureCombiner(outbound.values().stream().map(c -> c.close(false)).collect(Collectors.toList())).get();
            }
        }

        class PerConnection
        {
            class PerChannel implements MessageCallbacks, MessageProcessor
            {
                final Semaphore sendLimit;
                final OutboundConnection outbound;
                final MessageGenerator generator;
                final MessageGenerator corroborator;
                final long minId;
                final long maxId;

                private long nextSendId;
                private long nextReceiveId;
                private final ConcurrentSkipListMap<Long, Long> inFlight = new ConcurrentSkipListMap<>();
                private final WaitQueue waitQueue = new WaitQueue();

                PerChannel(Semaphore sendLimit, OutboundConnection outbound, MessageGenerator generator, long minId, long maxId)
                {
                    this.sendLimit = sendLimit;
                    this.outbound = outbound;
                    this.generator = generator.copy();
                    this.corroborator = generator.copy();
                    this.minId = minId;
                    this.maxId = maxId;
                    this.nextReceiveId = this.nextSendId = minId;
                }

                void sendOne()
                {
                    long id = nextSendId++;
                    if (nextSendId == maxId)
                        nextSendId = minId;
                    try
                    {
                        Message<?> msg = generator.generate(id).withId(id);
                        sendLimit.acquireUninterruptibly(msg.serializedSize(current_version));
                        inFlight.put(id, (long) msg.serializedSize(version));
                        outbound.enqueue(msg);
                    }
                    catch (ClosedChannelException e)
                    {
                        // TODO: make this a tested, not illegal, state
                        throw new IllegalStateException(e);
                    }
                }

                public void process(Message<?> message, int messageSize, MessageCallbacks callbacks)
                {
                    sendLimit.release(messageSize);
                    Message<?> canon;
                    synchronized (this)
                    {
                        canon = corroborator.generate(message.id);
                    }
                    Assert.assertArrayEquals((byte[])canon.payload, (byte[]) message.payload);
                    checkReceived(messageSize, message.id);
                    callbacks.onProcessed(messageSize);
                }

                public void onProcessed(int messageSize)
                {
                }

                public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
                {
                    onFailed(messageSize, id);
                }

                public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
                {
                    onFailed(messageSize, id);
                }

                public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
                {
                    System.out.println("Failed: " + id);
                    onFailed(messageSize, id);
                }

                private synchronized void onFailed(int messageSize, long id)
                {
                    sendLimit.release(messageSize);
                    Message<?> canon = corroborator.generate(id);
                    Assert.assertEquals(canon.serializedSize(current_version), messageSize);
                    checkReceived(messageSize, id);
                }

                private void checkReceived(int messageSize, long id)
                {
                    Assert.assertEquals((long)inFlight.remove(id), (long)messageSize);

                    Map.Entry<Long, Long> oldest;
                    synchronized (this)
                    {
                        if (id == nextReceiveId && nextReceiveId != maxId)
                            ++nextReceiveId;
                        else if (id == nextReceiveId)
                            nextReceiveId = minId;
                        else if (id - nextReceiveId < messageIdsPerChannel / 2)
                            nextReceiveId = id + 1;
                    }

                    waitQueue.signalAll();
                    oldest = inFlight.firstEntry();
                    if (oldest != null && nextReceiveId - oldest.getKey() > 16)
                    {
                        long waitUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100L);
                        while (true)
                        {
                            WaitQueue.Signal signal = waitQueue.register();
                            oldest = inFlight.firstEntry();
                            if (oldest == null || nextReceiveId - oldest.getKey() <= 16)
                            {
                                signal.cancel();
                                return;
                            }
                            Assert.assertTrue(signal.awaitUntilUninterruptibly(waitUntil));
                        }
                    }
                }
            }

            final PerChannel[] perChannels;
            final long minId;

            PerConnection(long minId, OutboundConnections connections, MessageGenerators generators)
            {
                this.perChannels = new PerChannel[3];
                this.minId = minId;
                long maxId = minId + messageIdsPerChannel;
                int i = 0;
                final Semaphore sendLimit = new Semaphore(1 << 22); // TODO: this should be dynamic, changing as the test progresses (sometimes small, sometimes large)
                for (OutboundConnection connection : ImmutableList.of(connections.urgent, connections.small, connections.large))
                {
                    perChannels[i++] = new PerChannel(sendLimit, connection, generators.get(connection), minId, maxId);
                    minId = maxId;
                    maxId = minId + messageIdsPerChannel;
                }
            }

            PerChannel get(long messageId)
            {
                return perChannels[(int)((messageId - minId) / messageIdsPerChannel)];
            }
        }

        class PerConnectionLookup
        {
            final PerConnection[] perConnections;

            PerConnectionLookup(Collection<OutboundConnections> outbound, MessageGenerators generators)
            {
                this.perConnections = new PerConnection[outbound.size()];
                long minId = 0;
                int i = 0;
                for (OutboundConnections connections : outbound)
                {
                    perConnections[i++] = new PerConnection(minId, connections, generators);
                    minId += messageIdsPerConnection;
                }
            }

            PerConnection get(long messageId)
            {
                return perConnections[(int) (messageId / messageIdsPerConnection)];
            }
        }

        public MessageCallbacks wrap(MessageCallbacks wrapped)
        {
            return new MessageCallbacks()
            {
                public void onProcessed(int messageSize)
                {
                    wrapped.onProcessed(messageSize);
                    Test.this.onProcessed(messageSize);
                }

                public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
                {
                    wrapped.onExpired(messageSize, id, verb, timeElapsed, unit);
                    Test.this.onExpired(messageSize, id, verb, timeElapsed, unit);
                }

                public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
                {
                    wrapped.onArrivedExpired(messageSize, id, verb, timeElapsed, unit);
                    Test.this.onArrivedExpired(messageSize, id, verb, timeElapsed, unit);
                }

                public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
                {
                    wrapped.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
                    Test.this.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
                }
            };
        }

        public void process(Message<?> message, int messageSize, MessageCallbacks callbacks)
        {
            testRunners.get(message.id).get(message.id).process(message, messageSize, callbacks);
        }

        public void onProcessed(int messageSize)
        {
        }

        public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
        {
            testRunners.get(id).get(id).onExpired(messageSize, id, verb, timeElapsed, unit);
        }

        public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
        {
            testRunners.get(id).get(id).onArrivedExpired(messageSize, id, verb, timeElapsed, unit);
        }

        public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
        {
            testRunners.get(id).get(id).onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
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

    public static void test(GlobalInboundSettings inbound, OutboundConnectionSettings outbound) throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException
    {
        MessageGenerator generator = new UniformPayloadGenerator(0, 1, (1 << 16) + (1 << 15));
        MessageGenerators generators = new MessageGenerators(generator, generator);
        outbound = outbound.withApplicationSendQueueCapacityInBytes(1 << 18)
                           .withApplicationReserveSendQueueCapacityInBytes(1 << 30, new ResourceLimits.Concurrent(Integer.MAX_VALUE));
        new Test(4, generators, inbound, outbound).run();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.daemonInitialization();
        GlobalInboundSettings inboundSettings = new GlobalInboundSettings()
                                                .withQueueCapacity(1 << 18)
                                                .withEndpointReserveLimit(1 << 20)
                                                .withGlobalReserveLimit(1 << 21)
                                                .withTemplate(new InboundConnectionSettings());
        test(inboundSettings, new OutboundConnectionSettings(null).withAcceptVersions(legacy));
    }

}
