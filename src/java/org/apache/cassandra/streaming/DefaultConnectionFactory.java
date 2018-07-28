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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.async.NettyFactory;
import org.apache.cassandra.net.async.OutboundConnectionInitiator;
import org.apache.cassandra.net.async.OutboundConnectionSettings;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.net.async.OutboundConnection.Type.*;

public class DefaultConnectionFactory implements StreamConnectionFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultConnectionFactory.class);

    @VisibleForTesting
    public static long MAX_WAIT_TIME_NANOS = TimeUnit.SECONDS.toNanos(30);
    @VisibleForTesting
    public static int MAX_CONNECT_ATTEMPTS = 3;

    @Override
    public Channel createConnection(OutboundConnectionSettings template, int messagingVersion) throws IOException
    {
        EventLoop eventLoop = NettyFactory.instance.outboundStreamingGroup().next();

        // TODO: why do we have our own connection logic here?  WHy not reuse that in OC?
        Bootstrap bootstrap = OutboundConnectionInitiator.createBootstrap(eventLoop, STREAM, template, messagingVersion);

        int connectionAttemptCount = 0;
        long now = System.nanoTime();
        final long end = now + MAX_WAIT_TIME_NANOS;
        final Channel channel;
        while (true)
        {
            ChannelFuture channelFuture = bootstrap.connect();
            channelFuture.awaitUninterruptibly(end - now, TimeUnit.MILLISECONDS);
            if (channelFuture.isSuccess())
            {
                channel = channelFuture.channel();
                break;
            }

            connectionAttemptCount++;
            now = System.nanoTime();
            if (connectionAttemptCount == MAX_CONNECT_ATTEMPTS || end - now <= 0)
                throw new IOException("failed to connect to " + template.endpoint + " for streaming data", channelFuture.cause());

            long waitms = DatabaseDescriptor.getRpcTimeout(MILLISECONDS) * (long)Math.pow(2, connectionAttemptCount);
            logger.warn("Failed attempt {} to connect to {}. Retrying in {} ms.", connectionAttemptCount, template.endpoint, waitms);
            Uninterruptibles.sleepUninterruptibly(waitms, TimeUnit.MILLISECONDS);
        }

        return channel;
    }
}
