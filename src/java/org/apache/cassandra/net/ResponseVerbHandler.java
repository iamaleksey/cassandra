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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ApproximateTime;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class ResponseVerbHandler implements IVerbHandler
{
    public static final ResponseVerbHandler instance = new ResponseVerbHandler();

    private static final Logger logger = LoggerFactory.getLogger( ResponseVerbHandler.class );

    public void doVerb(Message message)
    {

        long latencyNanos = ApproximateTime.nanoTime() - MessagingService.instance().callbacks.getCreationTimeNanos(message.id);
        CallbackInfo callbackInfo = MessagingService.instance().callbacks.remove(message.id);
        if (callbackInfo == null)
        {
            String msg = "Callback already removed for {} (from {})";
            logger.trace(msg, message.id, message.from);
            Tracing.trace(msg, message.id, message.from);
            return;
        }

        Tracing.trace("Processing response from {}", message.from);
        IAsyncCallback cb = callbackInfo.callback;
        if (message.isFailureResponse())
        {
            ((IAsyncCallbackWithFailure) cb).onFailure(message.from, (RequestFailureReason) message.payload);
        }
        else
        {
            MessagingService.instance().latency.maybeAdd(cb, message.from, latencyNanos, NANOSECONDS);
            cb.response(message);
        }

        if (callbackInfo.callback.supportsBackPressure())
        {
            MessagingService.instance().updateBackPressureOnReceive(message.from, cb, false);
        }
    }
}
