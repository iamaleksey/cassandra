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

package org.apache.cassandra.distributed.test.tracking;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

public class MutationTrackingWriteForwardingTest extends TestBaseImpl
{
    private static final int NODES = 3;
    private static final int RF = 1;

    private static int inst(int i)
    {
        return (i % NODES) + 1;
    }

    @Test
    public void testBasicWriteForwarding() throws Throwable
    {
        try (Cluster cluster = Cluster.build(NODES)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            String keyspaceName = "basic_write_forwarding_test";
            String tableName = "tbl";
            cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = " +
                                        "{'class': 'SimpleStrategy', 'replication_factor': " + RF + "} " +
                                        "AND replication_type='tracked';", keyspaceName));
            cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, primary key (k, c));", keyspaceName, tableName));

            for (int i = 0; i < 1000; i++)
            {
                int instance = inst(i);

                // Writes should be completed for the client, regardless of whether they are forwarded or not
                cluster.coordinator(instance).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?)", keyspaceName, tableName), ConsistencyLevel.ALL, i, i, i);

                // Writes should be ack'd in the journal too
                // But hypothetically these could lag behind client acks, so this check isn't totally safe
                // Would be nice to disable background reconciliation so we can test that writes are reconciling
                cluster.get(instance).runOnInstance(() -> {
                    Token token = DatabaseDescriptor.getPartitioner().getMinimumToken();
                    Range<Token> fullRange = new Range<>(token, token);
                    TableId tableId = Schema.instance.getTableMetadata(keyspaceName, tableName).id;
                    MutationSummary summary = MutationTrackingService.instance.summaryForRange(tableId, fullRange);

                    // Most reconciliation should be happening as part of the writes, allow a bit of wiggle-room
                    int maxUnreconciled = 5;
                    Assertions.assertThat(summary.unreconciledIds()).isLessThan(maxUnreconciled);
                });
            }
        }
    }
}
