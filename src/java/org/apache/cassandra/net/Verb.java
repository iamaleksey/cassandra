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
package org.apache.cassandra.net;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchRemoveVerbHandler;
import org.apache.cassandra.batchlog.BatchStoreVerbHandler;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.CounterMutationVerbHandler;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.MutationVerbHandler;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadCommandVerbHandler;
import org.apache.cassandra.db.ReadRepairVerbHandler;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.db.TruncateVerbHandler;
import org.apache.cassandra.db.Truncation;
import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.gms.EchoMessage;
import org.apache.cassandra.gms.GossipDigestAck;
import org.apache.cassandra.gms.GossipDigestAck2;
import org.apache.cassandra.gms.GossipDigestAck2VerbHandler;
import org.apache.cassandra.gms.GossipDigestAckVerbHandler;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.GossipDigestSynVerbHandler;
import org.apache.cassandra.gms.GossipShutdownVerbHandler;
import org.apache.cassandra.hints.HintMessage;
import org.apache.cassandra.hints.HintResponse;
import org.apache.cassandra.hints.HintVerbHandler;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.repair.RepairMessageVerbHandler;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.schema.SchemaPullVerbHandler;
import org.apache.cassandra.schema.SchemaPushVerbHandler;
import org.apache.cassandra.schema.SchemaVersionVerbHandler;
import org.apache.cassandra.service.EchoVerbHandler;
import org.apache.cassandra.service.SnapshotVerbHandler;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.CommitVerbHandler;
import org.apache.cassandra.service.paxos.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.ProposeVerbHandler;
import org.apache.cassandra.streaming.ReplicationDoneVerbHandler;
import org.apache.cassandra.utils.UUIDSerializer;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.*;
import static org.apache.cassandra.concurrent.Stage.INTERNAL_RESPONSE;
import static org.apache.cassandra.concurrent.Stage.MISC;
import static org.apache.cassandra.net.VerbTimeouts.*;
import static org.apache.cassandra.net.Verb.Priority.*;
import static org.apache.cassandra.schema.MigrationManager.MigrationsSerializer;

public enum Verb
{
    MUTATION_RSP         (60, P1,  writeTimeout,    REQUEST_RESPONSE,  () -> WriteResponse.serializer,       () -> ResponseVerbHandler.instance                                  ),
    MUTATION_REQ         (0,  P3,  writeTimeout,    MUTATION,          () -> Mutation.serializer,            () -> MutationVerbHandler.instance,         MUTATION_RSP            ),
    HINT_RSP             (61, P1,  writeTimeout,    REQUEST_RESPONSE,  () -> HintResponse.serializer,        () -> ResponseVerbHandler.instance                                  ),
    HINT_REQ             (1,  P4,  writeTimeout,    MUTATION,          () -> HintMessage.serializer,         () -> HintVerbHandler.instance,             HINT_RSP                ),
    READ_REPAIR_RSP      (62, P1,  writeTimeout,    REQUEST_RESPONSE,  () -> WriteResponse.serializer,       () -> ResponseVerbHandler.instance                                  ),
    READ_REPAIR_REQ      (2,  P1,  writeTimeout,    MUTATION,          () -> Mutation.serializer,            () -> ReadRepairVerbHandler.instance,       READ_REPAIR_RSP         ),
    BATCH_STORE_RSP      (65, P1,  writeTimeout,    REQUEST_RESPONSE,  () -> WriteResponse.serializer,       () -> ResponseVerbHandler.instance                                  ),
    BATCH_STORE_REQ      (5,  P3,  writeTimeout,    MUTATION,          () -> Batch.serializer,               () -> BatchStoreVerbHandler.instance,       BATCH_STORE_RSP         ),
    BATCH_REMOVE_RSP     (66, P1,  writeTimeout,    REQUEST_RESPONSE,  () -> WriteResponse.serializer,       () -> ResponseVerbHandler.instance                                  ),
    BATCH_REMOVE_REQ     (6,  P3,  writeTimeout,    MUTATION,          () -> UUIDSerializer.serializer,      () -> BatchRemoveVerbHandler.instance,      BATCH_REMOVE_RSP        ),

    PAXOS_PREPARE_RSP    (93, P2,  writeTimeout,    REQUEST_RESPONSE,  () -> Commit.serializer,              () -> ResponseVerbHandler.instance                                  ),
    PAXOS_PREPARE_REQ    (33, P2,  writeTimeout,    MUTATION,          () -> Commit.serializer,              () -> PrepareVerbHandler.instance,          PAXOS_PREPARE_RSP       ),
    PAXOS_PROPOSE_RSP    (94, P2,  writeTimeout,    REQUEST_RESPONSE,  () -> Commit.serializer,              () -> ResponseVerbHandler.instance                                  ),
    PAXOS_PROPOSE_REQ    (34, P2,  writeTimeout,    MUTATION,          () -> Commit.serializer,              () -> ProposeVerbHandler.instance,          PAXOS_PROPOSE_RSP       ),
    PAXOS_COMMIT_RSP     (95, P2,  writeTimeout,    REQUEST_RESPONSE,  () -> Commit.serializer,              () -> ResponseVerbHandler.instance                                  ),
    PAXOS_COMMIT_REQ     (35, P2,  writeTimeout,    MUTATION,          () -> Commit.serializer,              () -> CommitVerbHandler.instance,           PAXOS_COMMIT_RSP        ),

    TRUNCATE_RSP         (79, P0,  truncateTimeout, REQUEST_RESPONSE,  () -> TruncateResponse.serializer,    () -> ResponseVerbHandler.instance                                  ),
    TRUNCATE_REQ         (19, P0,  truncateTimeout, MUTATION,          () -> Truncation.serializer,          () -> TruncateVerbHandler.instance,         TRUNCATE_RSP            ),

    COUNTER_MUTATION_RSP (84, P1,  counterTimeout,  REQUEST_RESPONSE,  () -> WriteResponse.serializer,       () -> ResponseVerbHandler.instance                                  ),
    COUNTER_MUTATION_REQ (24, P2,  counterTimeout,  COUNTER_MUTATION,  () -> CounterMutation.serializer,     () -> CounterMutationVerbHandler.instance,  COUNTER_MUTATION_RSP    ),

    READ_RSP             (63, P2,  readTimeout,     REQUEST_RESPONSE,  () -> ReadResponse.serializer,        () -> ResponseVerbHandler.instance                                  ),
    READ_REQ             (3,  P3,  readTimeout,     READ,              () -> ReadCommand.serializer,         () -> ReadCommandVerbHandler.instance,      READ_RSP                ),
    RANGE_RSP            (69, P2,  rangeTimeout,    REQUEST_RESPONSE,  () -> ReadResponse.serializer,        () -> ResponseVerbHandler.instance                                  ),
    RANGE_REQ            (9,  P3,  rangeTimeout,    READ,              () -> ReadCommand.serializer,         () -> ReadCommandVerbHandler.instance,      RANGE_RSP               ),

    GOSSIP_DIGEST_SYN    (14, P0,  longTimeout,     GOSSIP,            () -> GossipDigestSyn.serializer,     () -> GossipDigestSynVerbHandler.instance                           ),
    GOSSIP_DIGEST_ACK    (15, P0,  longTimeout,     GOSSIP,            () -> GossipDigestAck.serializer,     () -> GossipDigestAckVerbHandler.instance                           ),
    GOSSIP_DIGEST_ACK2   (16, P0,  longTimeout,     GOSSIP,            () -> GossipDigestAck2.serializer,    () -> GossipDigestAck2VerbHandler.instance                          ),
    GOSSIP_SHUTDOWN      (29, P0,  rpcTimeout,      GOSSIP,            () -> EmptyMessage.serializer,        () -> GossipShutdownVerbHandler.instance                            ),

    ECHO_RSP             (91, P0,  rpcTimeout,      GOSSIP,            () -> EmptyMessage.serializer,        () -> ResponseVerbHandler.instance                                  ),
    ECHO_REQ             (31, P0,  rpcTimeout,      GOSSIP,            () -> EchoMessage.serializer,         () -> EchoVerbHandler.instance,             ECHO_RSP                ),
    PING_RSP             (97, P1,  pingTimeout,     GOSSIP,            () -> PongMessage.serializer,         () -> ResponseVerbHandler.instance                                  ),
    PING_REQ             (37, P1,  pingTimeout,     GOSSIP,            () -> PingMessage.serializer,         () -> PingVerbHandler.instance,             PING_RSP                ),

    SCHEMA_PUSH_RSP      (98, P0,  rpcTimeout,      MIGRATION,         () -> MigrationsSerializer.instance,  () -> ResponseVerbHandler.instance                                  ),
    SCHEMA_PUSH_REQ      (18, P0,  rpcTimeout,      MIGRATION,         () -> MigrationsSerializer.instance,  () -> SchemaPushVerbHandler.instance,       SCHEMA_PUSH_RSP         ),
    SCHEMA_PULL_RSP      (88, P0,  rpcTimeout,      MIGRATION,         () -> MigrationsSerializer.instance,  () -> ResponseVerbHandler.instance                                  ),
    SCHEMA_PULL_REQ      (28, P0,  rpcTimeout,      MIGRATION,         () -> EmptyMessage.serializer,        () -> SchemaPullVerbHandler.instance,       SCHEMA_PULL_RSP         ),
    SCHEMA_VERSION_RSP   (80, P0,  rpcTimeout,      MIGRATION,         () -> UUIDSerializer.serializer,      () -> ResponseVerbHandler.instance                                  ),
    SCHEMA_VERSION_REQ   (20, P0,  rpcTimeout,      MIGRATION,         () -> EmptyMessage.serializer,        () -> SchemaVersionVerbHandler.instance,    SCHEMA_VERSION_RSP      ),

    REPLICATION_DONE_RSP (82, P0,  rpcTimeout,      MISC,              () -> EmptyMessage.serializer,        () -> ResponseVerbHandler.instance                                  ),
    REPLICATION_DONE_REQ (22, P0,  rpcTimeout,      MISC,              () -> EmptyMessage.serializer,        () -> ReplicationDoneVerbHandler.instance,  REPLICATION_DONE_RSP    ),
    SNAPSHOT_RSP         (87, P0,  rpcTimeout,      MISC,              () -> EmptyMessage.serializer,        () -> ResponseVerbHandler.instance                                  ),
    SNAPSHOT_REQ         (27, P0,  rpcTimeout,      MISC,              () -> SnapshotCommand.serializer,     () -> SnapshotVerbHandler.instance,         SNAPSHOT_RSP            ),

    REPAIR_RSP           (92, P0,  rpcTimeout,      REQUEST_RESPONSE,  () -> EmptyMessage.serializer,        () -> ResponseVerbHandler.instance                                  ),
    REPAIR_REQ           (32, P0,  rpcTimeout,      ANTI_ENTROPY,      () -> RepairMessage.serializer,       () -> RepairMessageVerbHandler.instance,    REPAIR_RSP              ),

    // dummy verbs
    _TRACE               (30, P1,  rpcTimeout,      TRACING,           () -> EmptyMessage.serializer,        () -> null                                                          ),
    _SAMPLE              (42, P1,  rpcTimeout,      INTERNAL_RESPONSE, () -> null,                           () -> null                                                          ),
    _TEST_1              (10, P0,  writeTimeout,    INTERNAL_RESPONSE, () -> EmptyMessage.serializer,        () -> null                                                          ),
    _TEST_2              (11, P1,  rpcTimeout,      INTERNAL_RESPONSE, () -> EmptyMessage.serializer,        () -> null                                                          ),

    @Deprecated
    REQUEST_RSP          (4,  P1,  rpcTimeout,      REQUEST_RESPONSE,  () -> null,                           () -> ResponseVerbHandler.instance                                  ),
    @Deprecated
    INTERNAL_RSP         (23, P1,  rpcTimeout,      INTERNAL_RESPONSE, () -> null,                           () -> ResponseVerbHandler.instance                                  ),

    ;

    public enum Priority
    {
        P0,  // sends on the urgent connection (i.e. for Gossip, Echo)
        P1,  // small or empty responses
        P2,  // larger messages that can be dropped but who have a larger impact on system stability (e.g. READ_REPAIR, READ_RSP)
        P3,
        P4
    }

    public final int id;
    public final Priority priority;
    public final Stage stage;

    /**
     * Messages we receive from peers have a Verb that tells us what kind of message it is.
     * Most of the time, this is enough to determine how to deserialize the message payload.
     * The exception is the REQUEST_RSP verb, which just means "a respond to something you told me to do."
     * Traditionally, this was fine since each VerbHandler knew what type of payload it expected, and
     * handled the deserialization itself.  Now that we do that in ITC, to avoid the extra copy to an
     * intermediary byte[] (See CASSANDRA-3716), we need to wire that up to the CallbackInfo object
     * (see below).
     *
     * NOTE: we use a Supplier to avoid loading the dependent classes until necessary.
     */
    private final Supplier<IVersionedSerializer<?>> serializer;
    private final Supplier<IVerbHandler<?>> handler;

    public final Verb responseVerb;

    final LongUnaryOperator expiration;


    /**
     * Verbs it's okay to drop if the request has been queued longer than the request timeout.  These
     * all correspond to client requests or something triggered by them; we don't want to
     * drop internal messages like bootstrap or repair notifications.
     */
    Verb(int id, Priority priority, LongUnaryOperator expiration, Stage stage, Supplier<IVersionedSerializer<?>> serializer, Supplier<IVerbHandler<?>> handler)
    {
        this(id, priority, expiration, stage, serializer, handler, null);
    }

    Verb(int id, Priority priority, LongUnaryOperator expiration, Stage stage, Supplier<IVersionedSerializer<?>> serializer, Supplier<IVerbHandler<?>> handler, Verb responseVerb)
    {
        this.stage = stage;
        if (id < 0)
            throw new IllegalArgumentException("Verb id must be non-negative, got " + id + " for verb " + name());

        this.id = id;
        this.priority = priority;
        this.serializer = serializer;
        this.handler = handler;
        this.responseVerb = responseVerb;
        this.expiration = expiration;
    }

    public <T> IVersionedSerializer<T> serializer() { return (IVersionedSerializer<T>) serializer.get(); }
    public <T> IVerbHandler<T> handler() { return (IVerbHandler<T>) handler.get(); }

    public boolean isLegacyCallback()
    {
        return this == INTERNAL_RSP || this == REQUEST_RSP;
    }

    public long expirationTimeNanos(long nowNanos)
    {
        return expiration.applyAsLong(nowNanos);
    }

    // this is a little hacky, but reduces the number of parameters up top
    public boolean isResponse()
    {
        return handler.get() == ResponseVerbHandler.instance;
    }

    public Verb toPre40Verb()
    {
        if (!isResponse())
            return this;
        if (priority == P0)
            return INTERNAL_RSP;
        return REQUEST_RSP;
    }

    @VisibleForTesting
    public void unsafeSetHandler(Supplier<IVerbHandler<?>> handler) throws NoSuchFieldException, IllegalAccessException
    {
        Field field = Verb.class.getDeclaredField("handler");
        field.setAccessible(true);
        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(this, handler);
    }

    @VisibleForTesting
    public void unsafeSetSerializer(Supplier<IVersionedSerializer<?>> serializer) throws NoSuchFieldException, IllegalAccessException
    {
        Field field = Verb.class.getDeclaredField("serializer");
        field.setAccessible(true);
        Field modifiers = Field.class.getDeclaredField("modifiers");
        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(this, serializer);
    }

    private static final Verb[] idToVerbMap;

    static
    {
        Verb[] verbs = values();
        int max = -1;
        for (Verb v : verbs)
            max = Math.max(v.id, max);

        Verb[] idMap = new Verb[max + 1];
        for (Verb v : verbs)
        {
            if (idMap[v.id] != null)
                throw new IllegalArgumentException("cannot have two verbs that map to the same id: " + v + " and " + idMap[v.id]);
            idMap[v.id] = v;
        }

        idToVerbMap = idMap;
    }

    public static Verb fromId(int id)
    {
        Verb verb = id >= 0 && id < idToVerbMap.length ? idToVerbMap[id] : null;
        if (verb == null)
            throw new IllegalArgumentException("Unknown verb id " + id);
        return verb;
    }
}

class VerbTimeouts
{
    static LongUnaryOperator expiresAt(ToLongFunction<TimeUnit> timeout)
    {
        return nowNanos -> nowNanos + timeout.applyAsLong(NANOSECONDS);
    }
    static final LongUnaryOperator rpcTimeout = expiresAt(DatabaseDescriptor::getRpcTimeout);
    static final LongUnaryOperator writeTimeout = expiresAt(DatabaseDescriptor::getWriteRpcTimeout);
    static final LongUnaryOperator readTimeout = expiresAt(DatabaseDescriptor::getReadRpcTimeout);
    static final LongUnaryOperator rangeTimeout = expiresAt(DatabaseDescriptor::getRangeRpcTimeout);
    static final LongUnaryOperator counterTimeout = expiresAt(DatabaseDescriptor::getCounterWriteRpcTimeout);
    static final LongUnaryOperator truncateTimeout = expiresAt(DatabaseDescriptor::getTruncateRpcTimeout);
    static final LongUnaryOperator pingTimeout = expiresAt(DatabaseDescriptor::getPingTimeout);
    static final LongUnaryOperator longTimeout = expiresAt(units -> Math.max(DatabaseDescriptor.getRpcTimeout(units), units.convert(5L, TimeUnit.MINUTES)));
}