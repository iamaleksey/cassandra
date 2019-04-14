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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.async.InboundMessageHandler.MessageProcessor;
import org.apache.cassandra.net.async.MessageGenerator.UniformPayloadGenerator;
import org.apache.cassandra.net.async.OutboundConnection.Type;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.net.MessagingService.current_version;

public class ConnectionBurnTest extends ConnectionTest
{
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

    static class Inbound
    {
        final Map<InetAddressAndPort, Map<InetAddressAndPort, InboundMessageHandlers>> handlersByRecipientThenSender;
        final InboundSockets sockets;

        Inbound(List<InetAddressAndPort> endpoints, GlobalInboundSettings settings, InboundMessageHandler.MessageProcessor process, Function<MessageCallbacks, MessageCallbacks> callbacks)
        {
            ResourceLimits.Limit globalLimit = new ResourceLimits.Concurrent(settings.globalReserveLimit);
            InboundMessageHandler.WaitQueue globalWaitQueue = new InboundMessageHandler.WaitQueue(globalLimit);
            Map<InetAddressAndPort, Map<InetAddressAndPort, InboundMessageHandlers>> handlersByRecipientThenSender = new HashMap<>();
            List<InboundConnectionSettings> bind = new ArrayList<>();
            for (InetAddressAndPort recipient : endpoints)
            {
                Map<InetAddressAndPort, InboundMessageHandlers> handlersBySender = new HashMap<>();
                for (InetAddressAndPort sender : endpoints)
                    handlersBySender.put(sender, new InboundMessageHandlers(sender, settings.queueCapacity, settings.endpointReserveLimit, globalLimit, globalWaitQueue, process, callbacks));

                handlersByRecipientThenSender.put(recipient, handlersBySender);
                bind.add(settings.template.withHandlers(handlersBySender::get).withBindAddress(recipient));
            }
            this.sockets = new InboundSockets(bind);
            this.handlersByRecipientThenSender = handlersByRecipientThenSender;
        }
    }


    private static class Test implements MessageProcessor
    {
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

        private Test(int simulateEndpoints, MessageGenerators messageGenerators, GlobalInboundSettings inboundSettings, OutboundConnectionSettings outboundTemplate, long runForNanos)
        {
            this.endpoints = endpoints(simulateEndpoints);
            this.inbound = new Inbound(endpoints, inboundSettings, this, this::wrap);
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
                    Semaphore sendLimit = new Semaphore(1 << 22);
                    InboundMessageHandlers inboundHandlers = inbound.handlersByRecipientThenSender.get(recipient).get(sender);
                    OutboundConnections connections = OutboundConnections.unsafeCreate(outboundTemplate.toEndpoint(recipient).withFrom(sender), null);
                    for (Type type : Type.MESSAGING)
                    {
                        this.connections[i] = new Connection(sender, recipient, inboundHandlers, connections.connectionFor(type), sendLimit, messageGenerators.get(type), minId, maxId, version);
                        this.connectionMessageIds[i] = minId;
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
            try
            {
                long deadline = System.nanoTime() + runForNanos;
                Verb._TEST_2.unsafeSetSerializer(() -> serializer);
                inbound.sockets.open().get();

                CountDownLatch done = new CountDownLatch(connections.length);
                CopyOnWriteArrayList<Throwable> fail = new CopyOnWriteArrayList<>();
                for (Connection connection : connections)
                {
                    executor.execute(() -> {
                        Thread.currentThread().setName("Test-" + connection.sender.address + "->" + connection.recipient.address + '_' + connection.outbound.type());
                        try
                        {
                            while (deadline > System.nanoTime() && !Thread.currentThread().isInterrupted())
                            {
                                for (int i = 0 ; i < 100 ; ++i)
                                    connection.sendOne();
                            }
                        }
                        catch (Throwable t)
                        {
                            fail.add(t);
                        }
                        finally
                        {
                            done.countDown();
                        }
                    });
                }

                Reporter reporter = new Reporter();
                while (deadline > System.nanoTime() && fail.isEmpty())
                {
                    reporter.update();
                    reporter.print();
                    Uninterruptibles.awaitUninterruptibly(done, 30L, TimeUnit.SECONDS);
                }

                executor.shutdownNow();

                if (!fail.isEmpty())
                {
                    AssertionError failure = new AssertionError("One or more tasks threw exceptions");
                    for (Throwable t : fail)
                        failure.addSuppressed(t);
                    throw failure;
                }

                ExecutorUtils.awaitTermination(1L, TimeUnit.MINUTES, executor);
            }
            finally
            {
                inbound.sockets.close().get();
                new FutureCombiner(Arrays.stream(connections)
                                         .map(c -> c.outbound.close(false))
                                         .collect(Collectors.toList()))
                .get();
            }
        }

        class Reporter
        {
            long[][] totalSentBytes = new long[endpoints.size()][endpoints.size() * 3];
            String[][] print = new String[2 + endpoints.size()][2 + endpoints.size() * 3];
            int[] width = new int[2 + endpoints.size() * 3];
            long[] columnTotals = new long[1 + endpoints.size() * 3];

            Reporter()
            {
                print[0][0] = "Recipient";
                print[print.length - 1][0] = "Total";
                print[0][width.length - 1] = "Total";
                for (int row = 0 ; row < endpoints.size() ; ++row)
                    print[1 + row][0] = Integer.toString(1 + row);
                for (int column = 0 ; column < endpoints.size() * 3 ; column += 3)
                {
                    String endpoint = Integer.toString(1 + column / 3);
                    print[0][1 + column] = endpoint + " Urgent";
                    print[0][2 + column] = endpoint + " Small";
                    print[0][3 + column] = endpoint + " Large";
                }
            }

            void update()
            {
                Arrays.fill(columnTotals, 0);
                int row = 0, connection = 0;
                for (InetAddressAndPort recipient : endpoints)
                {
                    int column = 0;
                    long rowTotal = 0;
                    for (InetAddressAndPort sender : endpoints)
                    {
                        for (Type type : Type.MESSAGING)
                        {
                            assert recipient.equals(connections[connection].recipient);
                            assert sender.equals(connections[connection].sender);
                            assert type == connections[connection].outbound.type();

                            long cur = connections[connection].outbound.sentBytes();
                            long prev = totalSentBytes[row][column];
                            totalSentBytes[row][column] = cur;
                            print[1 + row][1 + column] = prettyPrintMemory(cur - prev);
                            columnTotals[column] += cur - prev;
                            rowTotal += cur - prev;
                            ++column;
                            ++connection;
                        }
                    }
                    columnTotals[column] += rowTotal;
                    print[1 + row][1 + column] = prettyPrintMemory(rowTotal);
                    ++row;
                }
                for (int column = 0 ; column < columnTotals.length ; ++column)
                    print[print.length - 1][1 + column] = prettyPrintMemory(columnTotals[column]);
            }

            void print()
            {
                Arrays.fill(width, 0);
                for (int row = 0 ; row < print.length ; ++row)
                {
                    for (int column = 0 ; column < width.length ; ++column)
                    {
                        width[column] = Math.max(width[column], print[row][column].length());
                    }
                }

                for (int row = 0 ; row < print.length ; ++row)
                {
                    StringBuilder builder = new StringBuilder();
                    for (int column = 0 ; column < width.length ; ++column)
                    {
                        String s = print[row][column];
                        int pad = width[column] - s.length();
                        for (int i = 0 ; i < pad ; ++i)
                            builder.append(' ');
                        builder.append(s);
                        builder.append("  ");
                    }
                    System.out.println(builder.toString());
                }
            }
        }

        public MessageCallbacks wrap(MessageCallbacks wrapped)
        {
            return new MessageCallbacks()
            {
                public void onProcessed(int messageSize)
                {
                    wrapped.onProcessed(messageSize);
                }

                public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
                {
                    forId(id).onExpired(messageSize, id, verb, timeElapsed, unit);
                    wrapped.onExpired(messageSize, id, verb, timeElapsed, unit);
                }

                public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit)
                {
                    forId(id).onArrivedExpired(messageSize, id, verb, timeElapsed, unit);
                    wrapped.onArrivedExpired(messageSize, id, verb, timeElapsed, unit);
                }

                public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t)
                {
                    forId(id).onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
                    wrapped.onFailedDeserialize(messageSize, id, expiresAtNanos, callBackOnFailure, t);
                }
            };
        }

        public void process(Message<?> message, int messageSize, MessageCallbacks callbacks)
        {
            forId(message.id).process(message, messageSize, callbacks);
        }
    }

    public static String prettyPrintMemory(long size)
    {
        if (size >= 1000 * 1000 * 1000)
            return String.format("%.0fG", size / (double) (1 << 30));
        if (size >= 1000 * 1000)
            return String.format("%.0fM", size / (double) (1 << 20));
        return String.format("%.0fK", size / (double) (1 << 10));
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
        MessageGenerator generator = new UniformPayloadGenerator(0, 1, (1 << 16) + (1 << 15));
        MessageGenerators generators = new MessageGenerators(generator, generator);
        outbound = outbound.withApplicationSendQueueCapacityInBytes(1 << 18)
                           .withApplicationReserveSendQueueCapacityInBytes(1 << 30, new ResourceLimits.Concurrent(Integer.MAX_VALUE));

        Test.builder()
            .generators(generators)
            .endpoints(4)
            .inbound(inbound)
            .outbound(outbound)
            .time(1L, TimeUnit.HOURS)
            .build().run();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException, NoSuchFieldException, IllegalAccessException, TimeoutException
    {
        DatabaseDescriptor.daemonInitialization();
        GlobalInboundSettings inboundSettings = new GlobalInboundSettings()
                                                .withQueueCapacity(1 << 18)
                                                .withEndpointReserveLimit(1 << 20)
                                                .withGlobalReserveLimit(1 << 21)
                                                .withTemplate(new InboundConnectionSettings());
        test(inboundSettings, new OutboundConnectionSettings(null));
        MessagingService.instance().socketFactory.shutdownNow();
//        test(inboundSettings, new OutboundConnectionSettings(null).withAcceptVersions(legacy));
    }

}
