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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.net.Message.Header;

/**
 * Encapsulates the callbacks that {@link InboundMessageHandler} invokes during the lifecycle of an inbound message
 * passing through it: from arrival to dispatch to execution.
 *
 * The flow will vary slightly between small and large messages. Small messages will be deserialized first and only
 * then dispatched to one of the {@link Stage} stages for execution, whereas a large message will be dispatched first,
 * and deserialized in-place on the relevant stage before being immediately processed.
 *
 * This difference will only show in case of deserialization failure. For large messages, it's possible for
 * {@link #onFailedDeserialize(int, Header, Throwable)} to be invoked after {@link #onExecuting(int, Header, long, TimeUnit)},
 * whereas for small messages it isn't.
 */
interface InboundMessageCallbacks
{
    /**
     * Invoked once resource permits for the message have been acquired. In case of small messages, the entire message
     * will have arrived. In case of large messages, it's possible that only the first frame of the large message has.
     */
    void onArrived(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked if a small message arrived too late to be processed, or if the first frame of a large message has an
     * expired header, or, alternatively, if a large message was still 'fresh' when its first frame arrived, but the
     * remaining frames have not arrived before expiry time.
     */
    void onArrivedExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked in several scenarios:
     *  1. A deserializer threw an exception while attempting to deserialize a small message
     *  2. A deserializer threw an exception while attempting to deserialize a large message
     *  3. A corrupt frame was encountered while assembling all the frames of a large message
     *  4. An {@link InboundMessageHandler} was closed, for whatever reason, when a large
     *     message frames were still being accumulated
     */
    void onFailedDeserialize(int messageSize, Header header, Throwable t);

    /**
     * Invoked just before a message-processing task is scheduled on the appropriate {@link Stage}
     * for the {@link Verb} of the message.
     */
    void onDispatched(int messageSize, Header header);

    /**
     * Invoked at the very beginning of execution of the message-processing task on the appropriate {@link Stage}.
     */
    void onExecuting(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked upon 'successful' processing of the message. Alternatively, {@link #onExpired(int, Header, long, TimeUnit)}
     * will be invoked if the message had expired while waiting to be processed in the queue of the {@link Stage}.
     */
    void onProcessed(int messageSize, Header header);

    /**
     * Invoked if the message had expired while waiting to be processed in the queue of the {@link Stage}. Otherwise,
     * {@link #onProcessed(int, Header)} will be invoked.
     */
    void onExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit);

    /**
     * Invoked at the very end of execution of the message-processing task, no matter the outcome of processing.
     */
    void onExecuted(int messageSize, Header header, long timeElapsed, TimeUnit unit);
}