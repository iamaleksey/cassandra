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
package org.apache.cassandra.db.virtual;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.InboundMessageHandlers;
import org.apache.cassandra.schema.TableMetadata;

final class InternodeInboundTable extends AbstractVirtualTable
{
    private static final String ADDRESS = "address";
    private static final String PORT = "port";

    private static final String RECEIVED_COUNT = "received_count";
    private static final String RECEIVED_BYTES = "received_bytes";
    private static final String PROCESSED_COUNT = "processed_count";
    private static final String PROCESSED_BYTES = "processed_bytes";
    private static final String PENDING_COUNT = "pending_count";
    private static final String PENDING_BYTES = "pending_bytes";
    private static final String EXPIRED_COUNT = "expired_count";
    private static final String EXPIRED_BYTES = "expired_bytes";
    private static final String ERROR_COUNT = "error_count";
    private static final String ERROR_BYTES = "error_bytes";

    InternodeInboundTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "internode_inbound")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(InetAddressType.instance, Int32Type.instance)))
                           .addPartitionKeyColumn(ADDRESS, InetAddressType.instance)
                           .addPartitionKeyColumn(PORT, Int32Type.instance)
                           .addRegularColumn(RECEIVED_COUNT, LongType.instance)
                           .addRegularColumn(RECEIVED_BYTES, LongType.instance)
                           .addRegularColumn(PROCESSED_COUNT, LongType.instance)
                           .addRegularColumn(PROCESSED_BYTES, LongType.instance)
                           .addRegularColumn(PENDING_COUNT, LongType.instance)
                           .addRegularColumn(PENDING_BYTES, LongType.instance)
                           .addRegularColumn(EXPIRED_COUNT, LongType.instance)
                           .addRegularColumn(EXPIRED_BYTES, LongType.instance)
                           .addRegularColumn(ERROR_COUNT, LongType.instance)
                           .addRegularColumn(ERROR_BYTES, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        ByteBuffer[] addressAndPortBytes = ((CompositeType) metadata().partitionKeyType).split(partitionKey.getKey());
        InetAddress address = InetAddressType.instance.compose(addressAndPortBytes[0]);
        int port = Int32Type.instance.compose(addressAndPortBytes[1]);
        InetAddressAndPort addressAndPort = InetAddressAndPort.getByAddressOverrideDefaults(address, port);

        SimpleDataSet result = new SimpleDataSet(metadata());
        InboundMessageHandlers handlers = MessagingService.instance().messageHandlers.get(addressAndPort);
        if (null != handlers)
            addRow(result, addressAndPort, handlers);
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        MessagingService.instance()
                        .messageHandlers
                        .forEach((addressAndPort, handlers) -> addRow(result, addressAndPort, handlers));
        return result;
    }

    private void addRow(SimpleDataSet dataSet, InetAddressAndPort addressAndPort, InboundMessageHandlers handlers)
    {
        dataSet.row(addressAndPort.address, addressAndPort.port)
               .column(RECEIVED_COUNT, handlers.receivedCount())
               .column(RECEIVED_BYTES, handlers.receivedBytes())
               .column(PROCESSED_COUNT, handlers.processedCount())
               .column(PROCESSED_BYTES, handlers.processedBytes())
               .column(PENDING_COUNT, handlers.pendingCount())
               .column(PENDING_BYTES, handlers.pendingBytes())
               .column(EXPIRED_COUNT, handlers.expiredCount())
               .column(EXPIRED_BYTES, handlers.expiredBytes())
               .column(ERROR_COUNT, handlers.errorCount())
               .column(ERROR_BYTES, handlers.errorBytes());
    }
}