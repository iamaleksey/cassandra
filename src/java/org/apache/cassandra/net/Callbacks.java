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
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ExpiringMap;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.concurrent.Stage.INTERNAL_RESPONSE;

public class Callbacks
{
    /* This records all the results mapped by message Id */
    private final ExpiringMap<Long, CallbackInfo> callbacks;

    public Callbacks(MessagingService messagingService)
    {
        BiConsumer<Long, ExpiringMap.CacheableObject<CallbackInfo>> timeoutReporter = (id, cachedObject) ->
        {
            final CallbackInfo expiredCallbackInfo = cachedObject.value;

            messagingService.latency.maybeAdd(expiredCallbackInfo.callback, expiredCallbackInfo.target, cachedObject.timeout(), NANOSECONDS);

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

        callbacks = new ExpiringMap<>(DatabaseDescriptor.getMinRpcTimeout(NANOSECONDS), NANOSECONDS, timeoutReporter);
    }

    public long addWithExpiration(IAsyncCallback cb, Message message, InetAddressAndPort to, long expiresAtNanos)
    {
        assert message.verb != Verb.MUTATION_REQ; // mutations need to call the overload with a ConsistencyLevel

        long messageId = Message.nextId();
        CallbackInfo previous = callbacks.putWithExpiration(messageId, new CallbackInfo(to, cb, message.verb), expiresAtNanos);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    public long addWithExpiration(AbstractWriteResponseHandler<?> cb,
                                 Message<?> message,
                                 Replica to,
                                 long expiresAtNanos,
                                 ConsistencyLevel consistencyLevel,
                                 boolean allowHints)
    {
        assert message.verb == Verb.MUTATION_REQ
               || message.verb == Verb.COUNTER_MUTATION_REQ
               || message.verb == Verb.PAXOS_COMMIT_REQ;

        long messageId = Message.nextId();
        CallbackInfo previous = callbacks.putWithExpiration(messageId,
                                              new WriteCallbackInfo(to,
                                                                    cb,
                                                                    message,
                                                                    consistencyLevel,
                                                                    allowHints,
                                                                    message.verb),
                                                            expiresAtNanos);
        assert previous == null : String.format("Callback already exists for id %d! (%s)", messageId, previous);
        return messageId;
    }

    public void removeAndExpire(long id)
    {
        callbacks.removeAndExpire(id);
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

    @VisibleForTesting
    public void unsafeClear()
    {
        callbacks.reset();
    }

    @VisibleForTesting
    public void unsafeSet(long messageId, CallbackInfo callback)
    {
        callbacks.put(messageId, callback);
    }

}
