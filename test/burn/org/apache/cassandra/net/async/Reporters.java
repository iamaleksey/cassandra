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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.locator.InetAddressAndPort;

class Reporters
{
    final Collection<InetAddressAndPort> endpoints;
    final Connection[] connections;
    final List<Reporter> reporters;

    Reporters(Collection<InetAddressAndPort> endpoints, Connection[] connections)
    {
        this.endpoints = endpoints;
        this.connections = connections;
        this.reporters = ImmutableList.of(
            new OutboundReporter(true,  "Outbound Throughput",   OutboundConnection::sentBytes,          ConnectionBurnTest::prettyPrintMemory),
            new OutboundReporter(true,  "Outbound Expirations",  OutboundConnection::expiredCount,       Long::toString),
            new OutboundReporter(true,  "Outbound Errors",       OutboundConnection::errorCount,         Long::toString),
            new OutboundReporter(false, "Outbound Pending Bytes",OutboundConnection::pendingBytes,       ConnectionBurnTest::prettyPrintMemory),
            new InboundReporter (true,  "Inbound Throughput",    InboundMessageHandlers::processedBytes, ConnectionBurnTest::prettyPrintMemory),
            new InboundReporter (true,  "Inbound Expirations",   InboundMessageHandlers::expiredCount,   Long::toString),
            new InboundReporter (true,  "Inbound Errors",        InboundMessageHandlers::errorCount,     Long::toString),
            new InboundReporter (false, "Inbound Pending Bytes", InboundMessageHandlers::pendingBytes,   ConnectionBurnTest::prettyPrintMemory)
        );
    }

    void update()
    {
        for (Reporter reporter : reporters)
            reporter.update();
    }

    void print()
    {
        for (Reporter reporter : reporters)
        {
            reporter.print();
            System.out.println();
        }
    }

    interface Reporter
    {
        void update();
        void print();
    }

    class OutboundReporter implements Reporter
    {
        boolean accumulates;
        final String name;
        final ToLongFunction<OutboundConnection> get;
        final LongFunction<String> print;
        final long[][] previousValue;
        final long[] columnTotals = new long[1 + endpoints.size() * 3];
        final Table table;

        OutboundReporter(boolean accumulates, String name, ToLongFunction<OutboundConnection> get, LongFunction<String> print)
        {
            this.accumulates = accumulates;
            this.name = name;
            this.get = get;
            this.print = print;

            previousValue = accumulates ? new long[endpoints.size()][endpoints.size() * 3] : null;

            String[] rowNames = new String[endpoints.size() + 1];
            for (int row = 0 ; row < endpoints.size() ; ++row)
            {
                rowNames[row] = Integer.toString(1 + row);
            }
            rowNames[rowNames.length - 1] = "Total";

            String[] columnNames = new String[endpoints.size() * 3 + 1];
            for (int column = 0 ; column < endpoints.size() * 3 ; column += 3)
            {
                String endpoint = Integer.toString(1 + column / 3);
                columnNames[    column] = endpoint + " Urgent";
                columnNames[1 + column] = endpoint + " Small";
                columnNames[2 + column] = endpoint + " Large";
            }
            columnNames[columnNames.length - 1] = "Total";

            table = new Table(rowNames, columnNames, "Recipient");
        }

        public void update()
        {
            Arrays.fill(columnTotals, 0);
            int row = 0, connection = 0;
            for (InetAddressAndPort recipient : endpoints)
            {
                int column = 0;
                long rowTotal = 0;
                for (InetAddressAndPort sender : endpoints)
                {
                    for (OutboundConnection.Type type : OutboundConnection.Type.MESSAGING)
                    {
                        assert recipient.equals(connections[connection].recipient);
                        assert sender.equals(connections[connection].sender);
                        assert type == connections[connection].outbound.type();

                        long cur = get.applyAsLong(connections[connection].outbound);
                        long value;
                        if (accumulates)
                        {
                            long prev = previousValue[row][column];
                            previousValue[row][column] = cur;
                            value = cur - prev;
                        }
                        else
                        {
                            value = cur;
                        }
                        table.set(row, column, print.apply(value));
                        columnTotals[column] += value;
                        rowTotal += value;
                        ++column;
                        ++connection;
                    }
                }
                columnTotals[column] += rowTotal;
                table.set(row, column, print.apply(rowTotal));
                ++row;
            }
            for (int column = 0 ; column < columnTotals.length ; ++column)
                table.set(endpoints.size(), column, print.apply(columnTotals[column]));
        }

        public void print()
        {
            System.out.println("===" + name + "===");
            table.print();
        }
    }

    class InboundReporter implements Reporter
    {
        final boolean accumulates;
        final String name;
        final ToLongFunction<InboundMessageHandlers> get;
        final LongFunction<String> print;
        final long[][] previousValue;
        final long[] columnTotals = new long[1 + endpoints.size()];
        final Table table;

        InboundReporter(boolean accumulates, String name, ToLongFunction<InboundMessageHandlers> get, LongFunction<String> print)
        {
            this.accumulates = accumulates;
            this.name = name;
            this.get = get;
            this.print = print;

            previousValue = accumulates ? new long[endpoints.size()][endpoints.size()] : null;

            String[] rowNames = new String[endpoints.size() + 1];
            for (int row = 0 ; row < endpoints.size() ; ++row)
            {
                rowNames[row] = Integer.toString(1 + row);
            }
            rowNames[rowNames.length - 1] = "Total";

            String[] columnNames = new String[endpoints.size() + 1];
            for (int column = 0 ; column < endpoints.size() ; ++column)
            {
                String endpoint = Integer.toString(1 + column);
                columnNames[column] = "       " + endpoint;
            }
            columnNames[columnNames.length - 1] = "Total";

            table = new Table(rowNames, columnNames, "Recipient");
        }

        public void update()
        {
            Arrays.fill(columnTotals, 0);
            int row = 0, connection = 0;
            for (InetAddressAndPort recipient : endpoints)
            {
                int column = 0;
                long rowTotal = 0;
                for (InetAddressAndPort sender : endpoints)
                {
                    assert recipient.equals(connections[connection].recipient);
                    assert sender.equals(connections[connection].sender);

                    long cur = get.applyAsLong(connections[connection].inboundHandlers);
                    long value;
                    if (accumulates)
                    {
                        long prev = previousValue[row][column];
                        previousValue[row][column] = cur;
                        value = cur - prev;
                    }
                    else
                    {
                        value = cur;
                    }
                    table.set(row, column, print.apply(value));
                    columnTotals[column] += value;
                    rowTotal += value;
                    ++column;
                    connection += 3;
                }
                columnTotals[column] += rowTotal;
                table.set(row, column, print.apply(rowTotal));
                ++row;
            }
            for (int column = 0 ; column < columnTotals.length ; ++column)
                table.set(endpoints.size(), column, print.apply(columnTotals[column]));
        }

        public void print()
        {
            System.out.println("===" + name + "===");
            table.print();
        }
    }

    private static class Table
    {
        final String[][] print;
        final int[] width;
        public Table(String[] rowNames, String[] columnNames, String rowNameHeader)
        {
            print = new String[rowNames.length + 1][columnNames.length + 1];
            width = new int[columnNames.length + 1];
            print[0][0] = rowNameHeader;
            for (int i = 0 ; i < columnNames.length ; ++i)
                print[0][1 + i] = columnNames[i];
            for (int i = 0 ; i < rowNames.length ; ++i)
                print[1 + i][0] = rowNames[i];
        }

        void set(int row, int column, String value)
        {
            print[row + 1][column + 1] = value;
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

}

