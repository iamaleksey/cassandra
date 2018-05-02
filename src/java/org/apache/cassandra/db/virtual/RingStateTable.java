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

import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.SystemView2;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.gms.ApplicationState.NATIVE_ADDRESS_AND_PORT;
import static org.apache.cassandra.gms.ApplicationState.SCHEMA;
import static org.apache.cassandra.gms.ApplicationState.STATUS_WITH_PORT;

final class RingStateTable extends SystemView2
{
    private static final String RPC_READY = "rpc_ready";
    private static final String HOST_ID = "host_id";
    private static final String NET_VERSION = "net_version";
    private static final String NATIVE_ADDRESS = "native_address";
    private static final String RELEASE_VERSION = "release_version";
    private static final String RACK = "rack";
    private static final String DC = "dc";
    private static final String SCHEMA_VERSION = "schema_version";
    private static final String LOAD = "load";
    private static final String STATUS = "status";
    private static final String NAME = "name";
    private static final String PORT = "port";
    private static final String IP = "ip";

    RingStateTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "ring_state")
                           .comment("current ring state")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(IP, InetAddressType.instance)
                           .addPartitionKeyColumn(PORT, Int32Type.instance)
                           .addRegularColumn(NAME, UTF8Type.instance)
                           .addRegularColumn(STATUS, UTF8Type.instance)
                           .addRegularColumn(LOAD, UTF8Type.instance)
                           .addRegularColumn(SCHEMA_VERSION, UTF8Type.instance)
                           .addRegularColumn(DC, UTF8Type.instance)
                           .addRegularColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(RELEASE_VERSION, UTF8Type.instance)
                           .addRegularColumn(NATIVE_ADDRESS, UTF8Type.instance)
                           .addRegularColumn(NET_VERSION, UTF8Type.instance)
                           .addRegularColumn(HOST_ID, UTF8Type.instance)
                           .addRegularColumn(RPC_READY, UTF8Type.instance)
                           .build());
    }

    public void read(StatementRestrictions restrictions, QueryOptions options, ResultBuilder result)
    {
        Set<Entry<InetAddressAndPort, EndpointState>> currentRingState = Gossiper.instance.getEndpointStates();
        // We should pull the columns from the query state/options
        for(Entry<InetAddressAndPort, EndpointState> entry : currentRingState)
        {
            EndpointState endpoint = entry.getValue();
            result.row(entry.getKey().address, entry.getKey().port)
                  .column(NAME, entry.getKey().address.getHostName())
                  .column(STATUS, endpoint.getApplicationState(STATUS_WITH_PORT).value)
                  .column(LOAD, endpoint.getApplicationState(ApplicationState.LOAD).value)
                  .column(SCHEMA_VERSION, endpoint.getApplicationState(SCHEMA).value)
                  .column(DC, endpoint.getApplicationState(ApplicationState.DC).value)
                  .column(RACK, endpoint.getApplicationState(ApplicationState.RACK).value)
                  .column(RELEASE_VERSION, endpoint.getApplicationState(ApplicationState.RELEASE_VERSION).value)
                  .column(NATIVE_ADDRESS, endpoint.getApplicationState(NATIVE_ADDRESS_AND_PORT).value)
                  .column(NET_VERSION, endpoint.getApplicationState(ApplicationState.NET_VERSION).value)
                  .column(HOST_ID, endpoint.getApplicationState(ApplicationState.HOST_ID).value)
                  .column(RPC_READY, endpoint.getApplicationState(ApplicationState.RPC_READY).value)
                  .endRow();
        }
    }
}
