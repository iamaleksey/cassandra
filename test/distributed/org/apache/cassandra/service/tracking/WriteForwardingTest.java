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

package org.apache.cassandra.service.tracking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.CassandraKeyspaceWriteHandler;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class WriteForwardingTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(WriteForwardingTest.class);

    private static final int NODES = 4;
    private static final int RF = 2;
    private static final int ROWS = 100;

    private static int instance(int i)
    {
        return (i % NODES) + 1;
    }

    @Test
    public void regular() throws Exception
    {
        String TABLE = "tbl_regular";

        Consumer<ICluster<IInvokableInstance>> action = cluster -> {
            cluster.schemaChange(format("CREATE TABLE %s.%s (pk int, ck int, v int, primary key (pk, ck));", KEYSPACE, TABLE));

            for (int i = 0; i < ROWS; i++)
            {
                int coordinatorIdx = instance(i);
                IInvokableInstance instance = cluster.get(coordinatorIdx);
                ICoordinator coordinator = instance.coordinator();
                int coordinatorNodeId = instance.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
                logger.debug("Test query: coordinated by host {} for key {}", coordinatorNodeId, i);
                coordinator.execute(format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)", KEYSPACE, TABLE), ConsistencyLevel.ALL, i, i, i);
                Object[][] rows = coordinator.execute(format("SELECT pk, ck, v FROM %s.%s WHERE pk = ? AND ck = ?", KEYSPACE, TABLE), ConsistencyLevel.ALL, i, i);
                Assertions.assertThat(rows).hasNumberOfRows(1);
                Assertions.assertThat(rows[0]).isEqualTo(new Object[]{i, i, i});
            }
        };
        test(action);
    }

    public void test(Consumer<ICluster<IInvokableInstance>> action) throws Exception
    {
        test(c -> {}, action);
    }

    public void test(Consumer<Cluster.Builder> builder, Consumer<ICluster<IInvokableInstance>> action) throws Exception
    {
        Cluster.Builder clusterBuilder = builder().withInstanceInitializer(BB::install)
                                                  .withConfig(c -> c.with(Feature.NATIVE_PROTOCOL))
                                                  .withDynamicPortAllocation(true)
                                                  .withNodes(NODES);
        builder.accept(clusterBuilder);
        try (ICluster<IInvokableInstance> cluster = clusterBuilder.start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + RF + "} AND replication_type='logged';"));
            action.accept(cluster);
            assertMutationIdsEquivalent(cluster);
        }
    }

    public static class BB
    {
        private static final Logger logger = LoggerFactory.getLogger(BB.class);

        public static void install(ClassLoader classLoader, Integer node)
        {
            new ByteBuddy().rebase(CassandraKeyspaceWriteHandler.class)
                           .method(named("beginWrite"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(MutationId.class)
                           .method(named("createNext").and(takesArguments(Token.class)))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(Hint.class)
                           .constructor(takesArguments(Mutation.class, Long.class, Integer.class))
                           .intercept(MethodDelegation.to(BB.class, "hintCtor"))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        // No need to key by instance, classloader isolation will create a copy for each instance
        private static final List<MutationId> trackedIds = new ArrayList<>();

        static void trackMutationId(MutationId id)
        {
            trackedIds.add(id);
            logger.info("Tracking new mutation ID {}", id);
        }

        @SuppressWarnings("unused")
        public static WriteContext beginWrite(Mutation mutation, boolean makeDurable, @SuperCall Callable<WriteContext> zuper)
        throws Exception
        {
            if (!mutation.getKeyspaceName().equals(KEYSPACE))
                return zuper.call();

            try
            {
                Assertions.assertThat(mutation.id().isNone()).isFalse();
                trackMutationId(mutation.id());

                // The issuer of a mutation ID must be a replica
                ClusterMetadata metadata = ClusterMetadata.current();
                int issuedBy = mutation.id().hostId();
                EndpointsForToken replicas = metadata.placements.get(Keyspace.open(KEYSPACE).getMetadata().params.replication).writes.forToken(mutation.key().getToken()).get();
                logger.info("Applying mutation issued by {}", issuedBy);
                Assertions.assertThat(replicas).anyMatch(replica -> metadata.directory.endpoint(new NodeId(issuedBy)).equals(replica.endpoint()));
            }
            catch (AssertionError e)
            {
                logger.error("CHECKPOINT: beginWrite mutation {}", mutation, e);
                throw e;
            }
            return zuper.call();
        }

        @SuppressWarnings("unused")
        public static MutationId createNext(Token token, @SuperCall Callable<MutationId> zuper)
        throws Exception
        {
            MutationId id = zuper.call();
            if (!id.isNone())
            {
                logger.info("Created MutationId {} for token {}", id, token);

                ClusterMetadata metadata = ClusterMetadata.current();
                EndpointsForToken replicas = metadata.placements.get(Keyspace.open(KEYSPACE).getMetadata().params.replication).writes.forToken(token).get();

                // Mutation IDs should always be issued by replicas
                try
                {
                    Assertions.assertThat(replicas).anyMatch(replica -> metadata.directory.endpoint(new NodeId(id.hostId())).equals(replica.endpoint()));
                }
                catch (Error e)
                {
                    logger.error("CHECKPOINT", e);
                    throw e;
                }
            }

            return id;
        }

        @SuppressWarnings("unused")
        public static Hint hintCtor(Mutation mutation, long creationTime, int gcgs, @SuperCall Callable<Hint> zuper)
        throws Exception
        {
            // Never create a hint for a logged keyspace
            Assertions.assertThat(mutation.getKeyspaceName()).isNotEqualTo(KEYSPACE);
            return zuper.call();
        }
    }

    private static Map<Integer, List<MutationId>> getIds(ICluster<IInvokableInstance> cluster)
    {
        Map<Integer, List<MutationId>> instanceMutationIds = new HashMap<>();

        cluster.forEach(instance -> {
            // Don't use instance ID from jvm-dtest, it doesn't match TCM myNodeId
            int instanceId = instance.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());

            List<byte[]> serialized = instance.callOnInstance(() -> {
                logger.info("Instance {} has tracked IDs: {}", instanceId, BB.trackedIds);
                List<byte[]> serializedIds = new ArrayList<>();

                for (MutationId id : BB.trackedIds)
                {
                    try (DataOutputBuffer out = new DataOutputBuffer())
                    {
                        MutationId.serializer.serialize(id, out, MessagingService.current_version);
                        serializedIds.add(out.toByteArray());
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                }

                return serializedIds;
            });

            List<MutationId> mutationIds = new ArrayList<>();
            for (byte[] bytes : serialized)
            {
                MutationId id;
                try
                {
                    id = MutationId.serializer.deserialize(new DataInputBuffer(bytes), MessagingService.current_version);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
                mutationIds.add(id);
            }
            instanceMutationIds.put(instanceId, mutationIds);
        });
        return instanceMutationIds;
    }

    private static void assertMutationIdsEquivalent(ICluster<IInvokableInstance> cluster)
    {
        Set<MutationId> allIds = new HashSet<>();
        Map<Integer, List<MutationId>> instanceMutationIds = getIds(cluster);
        Assertions.assertThat(instanceMutationIds).isNotEmpty();
        for (Map.Entry<Integer, List<MutationId>> entry : instanceMutationIds.entrySet())
        {
            int instance = entry.getKey();
            List<MutationId> idsForInstance = entry.getValue();
            Set<Integer> uniqueCoordinators = new HashSet<>();
            for (MutationId id : idsForInstance)
            {
                uniqueCoordinators.add(id.hostId());
            }
            // With NODES = 4, RF = 2, a replica contains mutations from 3 coordinators
            logger.info("Checking instance {}", instance);
            int maxCoordinators = Math.min((RF * 2) - 1, NODES);
            Assertions.assertThat(uniqueCoordinators).hasSizeLessThanOrEqualTo(maxCoordinators);
            Assertions.assertThat(uniqueCoordinators).contains(instance);

            allIds.addAll(idsForInstance);
        }

        Assertions.assertThat(allIds).hasSize(ROWS);
    }
}