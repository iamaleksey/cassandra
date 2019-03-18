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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.net.async.InboundCallbacks.OnMessageExpired;
import org.apache.cassandra.net.async.InboundCallbacks.OnMessageProcessed;
import org.apache.cassandra.utils.ApproximateTime;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.ParameterType.*;
import static org.apache.cassandra.net.MessageFlag.IS_FAILURE_RESPONSE;

public class ProcessMessageTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(ProcessMessageTask.class);

    private final Message message;
    private final int messageSize;
    private final long enqueueTime;

    private final OnMessageExpired onExpired;
    private final OnMessageProcessed onProcessed;

    public ProcessMessageTask(Message message)
    {
        this(message, 0, OnMessageProcessed.NOOP, OnMessageExpired.NOOP);
    }

    ProcessMessageTask(Message message, int messageSize, OnMessageProcessed onProcessed, OnMessageExpired onExpired)
    {
        assert message != null;
        this.message = message;
        this.messageSize = messageSize;
        this.enqueueTime = ApproximateTime.nanoTime();
        this.onProcessed = onProcessed;
        this.onExpired = onExpired;
    }

    public void run()
    {
        long nowNanos = ApproximateTime.nanoTime();

        MessagingService.instance().metrics.addQueueWaitTime(message.verb, nowNanos - enqueueTime, NANOSECONDS);

        if (nowNanos > message.expiresAtNanos)
        {
            onExpired.call(message.verb, messageSize, nowNanos - message.createdAtNanos, NANOSECONDS);
            return;
        }

        try
        {
            message.process();
        }
        catch (IOException ioe)
        {
            handleFailure(ioe);
            throw new RuntimeException(ioe);
        }
        catch (TombstoneOverwhelmingException | IndexNotAvailableException e)
        {
            handleFailure(e);
            logger.error(e.getMessage());
        }
        catch (Throwable t)
        {
            handleFailure(t);
            throw t;
        }
        finally
        {
            onProcessed.call(messageSize);
        }
    }

    private void handleFailure(Throwable t)
    {
        if (message.callBackOnFailure())
        {
            RequestFailureReason reason = RequestFailureReason.forException(t);
            Message response = Message.respondWithFlagAndParameter(message, noPayload, IS_FAILURE_RESPONSE, FAILURE_REASON, reason);
            MessagingService.instance().sendResponse(response, message.from);
        }
    }
}
