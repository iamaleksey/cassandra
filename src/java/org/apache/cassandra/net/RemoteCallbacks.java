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
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.net.async.OutboundMessageCallbacks;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.ExpiringMap;
import org.apache.cassandra.utils.FBUtilities;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.INTERNAL_RESPONSE;

public class RemoteCallbacks
{
    public static class CallbackInfo
    {
        protected final InetAddressAndPort target;
        protected final IAsyncCallback callback;
        @Deprecated // for 3.0 compatibility purposes only
        public final Verb verb;

        private CallbackInfo(InetAddressAndPort target, IAsyncCallback callback, Verb verb)
        {
            this.target = target;
            this.callback = callback;
            this.verb = verb;
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
            return "CallbackInfo(" + "target=" + target + ", callback=" + callback + ", failureCallback=" + isFailureCallback() + ')';
        }
    }

    static class WriteCallbackInfo extends CallbackInfo
    {
        // either a Mutation, or a Paxos Commit (MessageOut)
        private final Object mutation;
        private final Replica replica;

        @VisibleForTesting
        WriteCallbackInfo(Replica replica,
                                 IAsyncCallbackWithFailure<?> callback,
                                 Message message,
                                 ConsistencyLevel consistencyLevel,
                                 boolean allowHints,
                                 Verb verb)
        {
            super(replica.endpoint(), callback, verb);
            assert message != null;
            this.mutation = shouldHint(allowHints, message, consistencyLevel);
            //Local writes shouldn't go through messaging service (https://issues.apache.org/jira/browse/CASSANDRA-10477)
            assert (!target.equals(FBUtilities.getBroadcastAddressAndPort()));
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
    private final ExpiringMap<Long, CallbackInfo> callbacks;
    private final OutboundMessageCallbacks expireDroppedMessages;

    RemoteCallbacks(MessagingService messagingService)
    {
        BiConsumer<Long, ExpiringMap.CacheableObject<CallbackInfo>> timeoutReporter = (id, cachedObject) ->
        {
            final CallbackInfo expiredCallbackInfo = cachedObject.value;

            messagingService.latencySubscribers.maybeAdd(expiredCallbackInfo.callback, expiredCallbackInfo.target, cachedObject.timeout(), NANOSECONDS);

            InternodeOutboundMetrics.totalExpiredCallbacks.mark();
            messagingService.markExpiredCallback(expiredCallbackInfo.target);

            if (expiredCallbackInfo.callback.supportsBackPressure())
            {
                messagingService.updateBackPressureOnReceive(expiredCallbackInfo.target, expiredCallbackInfo.callback, true);
            }

            if (expiredCallbackInfo.isFailureCallback())
            {
                StageManager.getStage(INTERNAL_RESPONSE).submit(() ->
                                                                {
                                                                    ((IAsyncCallbackWithFailure)expiredCallbackInfo.callback).onFailure(expiredCallbackInfo.target, RequestFailureReason.UNKNOWN);
                                                                });
            }

            if (expiredCallbackInfo.shouldHint())
            {
                WriteCallbackInfo writeCallbackInfo = ((WriteCallbackInfo) expiredCallbackInfo);
                Mutation mutation = writeCallbackInfo.mutation();
                StorageProxy.submitHint(mutation, writeCallbackInfo.getReplica(), null);
            }
        };

        callbacks = new ExpiringMap<>(DatabaseDescriptor.getMinRpcTimeout(NANOSECONDS) / 2, NANOSECONDS, timeoutReporter);
        expireDroppedMessages = OutboundMessageCallbacks.invokeOnDrop(message -> callbacks.removeAndExpire(message.id()));
    }

    public long addWithExpiration(IAsyncCallback cb, Message message, InetAddressAndPort to)
    {
        assert message.verb() != Verb.MUTATION_REQ; // mutations need to call the overload with a ConsistencyLevel

        long messageId = Message.nextId();
        CallbackInfo previous = callbacks.put(messageId, new CallbackInfo(to, cb, message.verb()), message.createdAtNanos(), message.expiresAtNanos());
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    public long addWithExpiration(AbstractWriteResponseHandler<?> cb,
                                 Message<?> message,
                                 Replica to,
                                 ConsistencyLevel consistencyLevel,
                                 boolean allowHints)
    {
        assert message.verb() == Verb.MUTATION_REQ
               || message.verb() == Verb.COUNTER_MUTATION_REQ
               || message.verb() == Verb.PAXOS_COMMIT_REQ;

        long messageId = Message.nextId();
        CallbackInfo previous = callbacks.put(messageId,
                                              new WriteCallbackInfo(to,
                                                                    cb,
                                                                    message,
                                                                    consistencyLevel,
                                                                    allowHints,
                                                                    message.verb()),
                                              message.createdAtNanos(),
                                              message.expiresAtNanos());
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    public OutboundMessageCallbacks expireDroppedMessages()
    {
        return expireDroppedMessages;
    }

    public CallbackInfo get(long messageId)
    {
        return callbacks.get(messageId);
    }

    public CallbackInfo remove(long messageId)
    {
        return callbacks.remove(messageId);
    }

    /**
     * @return System.nanoTime() when callback was created.
     */
    public long getCreationTimeNanos(long messageId)
    {
        return callbacks.getCreationTimeNanos(messageId);
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

    @VisibleForTesting
    public void unsafeClear()
    {
        callbacks.reset();
    }

}
