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

import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.net.async.OutboundMessageCallbacks;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.INTERNAL_RESPONSE;

public class RemoteCallbacks
{
    static class CallbackInfo
    {
        final long createdAtNanos;
        final long expiresAtNanos;

        final InetAddressAndPort peer;
        final IAsyncCallback callback;

        @Deprecated // for 3.0 compatibility purposes only
        public final Verb responseVerb;

        private CallbackInfo(Message message, InetAddressAndPort peer, IAsyncCallback callback)
        {
            this.createdAtNanos = message.createdAtNanos();
            this.expiresAtNanos = message.expiresAtNanos();
            this.peer = peer;
            this.callback = callback;
            this.responseVerb = message.verb().responseVerb;
        }

        public long timeout()
        {
            return expiresAtNanos - createdAtNanos;
        }

        boolean isReadyToDieAt(long atNano)
        {
            return atNano > expiresAtNanos;
        }

        boolean shouldHint()
        {
            return false;
        }

        boolean isFailureCallback()
        {
            return callback instanceof IAsyncCallbackWithFailure<?>;
        }

        public String toString()
        {
            return "CallbackInfo(" + "peer=" + peer + ", callback=" + callback + ", failureCallback=" + isFailureCallback() + ')';
        }
    }

    static class WriteCallbackInfo extends CallbackInfo
    {
        // either a Mutation, or a Paxos Commit (MessageOut)
        private final Object mutation;
        private final Replica replica;

        @VisibleForTesting
        WriteCallbackInfo(Message message, Replica replica, IAsyncCallbackWithFailure<?> callback, ConsistencyLevel consistencyLevel, boolean allowHints)
        {
            super(message, replica.endpoint(), callback);
            this.mutation = shouldHint(allowHints, message, consistencyLevel);
            //Local writes shouldn't go through messaging service (https://issues.apache.org/jira/browse/CASSANDRA-10477)
            //noinspection AssertWithSideEffects
            assert !peer.equals(FBUtilities.getBroadcastAddressAndPort());
            this.replica = replica;
        }

        public boolean shouldHint()
        {
            return mutation != null && StorageProxy.shouldHint(replica);
        }

        public Replica getReplica()
        {
            return replica;
        }

        public Mutation mutation()
        {
            return getMutation(mutation);
        }

        private static Mutation getMutation(Object object)
        {
            assert object instanceof Commit || object instanceof Mutation : object;
            return object instanceof Commit ? ((Commit) object).makeMutation()
                                            : (Mutation) object;
        }

        private static Object shouldHint(boolean allowHints, Message sentMessage, ConsistencyLevel consistencyLevel)
        {
            return allowHints
                   && sentMessage.verb() != Verb.COUNTER_MUTATION_REQ
                   && consistencyLevel != ConsistencyLevel.ANY
                   ? sentMessage.payload : null;
        }
    }

    /* This records all the results mapped by message Id */
    private final CallbackMap callbacks;
    private final OutboundMessageCallbacks expireDroppedMessages;

    RemoteCallbacks(MessagingService messagingService)
    {
        Consumer<CallbackInfo> onExpired = info ->
        {
            messagingService.latencySubscribers.maybeAdd(info.callback, info.peer, info.timeout(), NANOSECONDS);

            InternodeOutboundMetrics.totalExpiredCallbacks.mark();
            messagingService.markExpiredCallback(info.peer);

            if (info.callback.supportsBackPressure())
                messagingService.updateBackPressureOnReceive(info.peer, info.callback, true);

            if (info.isFailureCallback())
            {
                StageManager.getStage(INTERNAL_RESPONSE).submit(() ->
                {
                    ((IAsyncCallbackWithFailure)info.callback).onFailure(info.peer, RequestFailureReason.UNKNOWN);
                });
            }

            if (info.shouldHint())
            {
                WriteCallbackInfo writeCallbackInfo = ((WriteCallbackInfo) info);
                Mutation mutation = writeCallbackInfo.mutation();
                StorageProxy.submitHint(mutation, writeCallbackInfo.getReplica(), null);
            }
        };

        callbacks = new CallbackMap(DatabaseDescriptor.getMinRpcTimeout(NANOSECONDS) / 2, NANOSECONDS, onExpired);

        expireDroppedMessages = OutboundMessageCallbacks.invokeOnDrop((message, peer) -> callbacks.removeAndExpire(message.id(), peer));
    }

    public void addWithExpiration(IAsyncCallback cb, Message message, InetAddressAndPort to)
    {
        // mutations need to call the overload with a ConsistencyLevel
        assert message.verb() != Verb.MUTATION_REQ && message.verb() != Verb.COUNTER_MUTATION_REQ && message.verb() != Verb.PAXOS_COMMIT_REQ;
        CallbackInfo previous = callbacks.put(message.id(), to, new CallbackInfo(message, to, cb));
        assert previous == null : format("Callback already exists for id %d/%s! (%s)", message.id(), to, previous);
    }

    public void addWithExpiration(AbstractWriteResponseHandler<?> cb,
                                  Message<?> message,
                                  Replica to,
                                  ConsistencyLevel consistencyLevel,
                                  boolean allowHints)
    {
        assert message.verb() == Verb.MUTATION_REQ || message.verb() == Verb.COUNTER_MUTATION_REQ || message.verb() == Verb.PAXOS_COMMIT_REQ;
        CallbackInfo previous = callbacks.put(message.id(), to.endpoint(), new WriteCallbackInfo(message, to, cb, consistencyLevel, allowHints));
        assert previous == null : format("Callback already exists for id %d/%s! (%s)", message.id(), to.endpoint(), previous);
    }

    public CallbackInfo get(long id, InetAddressAndPort peer)
    {
        return callbacks.get(id, peer);
    }

    public CallbackInfo remove(long id, InetAddressAndPort peer)
    {
        return callbacks.remove(id, peer);
    }

    <T> IVersionedAsymmetricSerializer<?, T> responseSerializer(long id, InetAddressAndPort peer)
    {
        CallbackInfo info = get(id, peer);
        return info == null ? null : info.responseVerb.serializer();
    }

    void shutdownNow(boolean expireCallbacks)
    {
        callbacks.shutdownNow(expireCallbacks);
    }

    void shutdownGracefully()
    {
        callbacks.shutdownGracefully();
    }

    void awaitTerminationUntil(long deadlineNanos) throws TimeoutException, InterruptedException
    {
        callbacks.awaitTerminationUntil(deadlineNanos);
    }

    public OutboundMessageCallbacks expireDroppedMessagesCallbacks()
    {
        return expireDroppedMessages;
    }

    @VisibleForTesting
    public void unsafeClear()
    {
        callbacks.reset();
    }
}
