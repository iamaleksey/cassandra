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

package org.apache.cassandra.net.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;

import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.FailedFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.HandshakeProtocol.Initiate;
import org.apache.cassandra.net.async.HandshakeProtocol.Mode;
import org.apache.cassandra.net.async.HandshakeProtocol.Accept;
import org.apache.cassandra.net.async.OutboundConnection.Type;
import org.apache.cassandra.net.async.OutboundConnectionInitiator.Result.MessagingSuccess;
import org.apache.cassandra.net.async.OutboundConnectionInitiator.Result.StreamingSuccess;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.async.HandshakeProtocol.*;
import static org.apache.cassandra.net.async.NettyFactory.*;
import static org.apache.cassandra.net.async.NettyFactory.newSslHandler;
import static org.apache.cassandra.net.async.OutboundConnection.Type.STREAM;
import static org.apache.cassandra.net.async.OutboundConnectionInitiator.Result.incompatible;
import static org.apache.cassandra.net.async.OutboundConnectionInitiator.Result.messagingSuccess;
import static org.apache.cassandra.net.async.OutboundConnectionInitiator.Result.retry;
import static org.apache.cassandra.net.async.OutboundConnectionInitiator.Result.streamingSuccess;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

/**
 * A {@link ChannelHandler} to execute the send-side of the internode handshake protocol.
 * As soon as the handler is added to the channel via {@link ChannelInboundHandler#channelActive(ChannelHandlerContext)}
 * (which is only invoked if the underlying TCP connection was properly established), the {@link Initiate}
 * handshake is sent. See {@link HandshakeProtocol} for full details.
 * <p>
 * Upon completion of the handshake (on success or fail), the {@link #resultPromise} is completed.
 * See {@link Result} for details about the different result states.
 * <p>
 * This class extends {@link ByteToMessageDecoder}, which is a {@link ChannelInboundHandler}, because this handler
 * waits for the peer's handshake response (the {@link Accept} of the internode messaging handshake protocol).
 */
public class OutboundConnectionInitiator<SuccessType extends OutboundConnectionInitiator.Result.Success>
{
    private static final Logger logger = LoggerFactory.getLogger(OutboundConnectionInitiator.class);

    private final Type type;
    private final OutboundConnectionSettings settings;
    private final int requestMessagingVersion; // for pre40 nodes only
    private final Promise<Result<SuccessType>> resultPromise;

    private OutboundConnectionInitiator(Type type, OutboundConnectionSettings settings,
                                        int requestMessagingVersion, Promise<Result<SuccessType>> resultPromise)
    {
        this.type = type;
        this.requestMessagingVersion = requestMessagingVersion;
        this.settings = settings;
        this.resultPromise = resultPromise;
    }

    /**
     * Initiate a connection with the requested messaging version.
     * if the other node supports a newer version, or doesn't support this version, we will fail to connect
     * and try again with the version they reported
     */
    public static Future<Result<StreamingSuccess>> initiateStreaming(EventLoop eventLoop, OutboundConnectionSettings settings, int requestMessagingVersion)
    {
        return new OutboundConnectionInitiator<StreamingSuccess>(STREAM, settings, requestMessagingVersion, new AsyncPromise<>(eventLoop))
               .initiate(eventLoop);
    }

    /**
     * Initiate a connection with the requested messaging version.
     * if the other node supports a newer version, or doesn't support this version, we will fail to connect
     * and try again with the version they reported
     */
    public static Future<Result<MessagingSuccess>> initiateMessaging(EventLoop eventLoop, Type type, OutboundConnectionSettings settings, int requestMessagingVersion)
    {
        return new OutboundConnectionInitiator<MessagingSuccess>(type, settings, requestMessagingVersion, new AsyncPromise<>(eventLoop))
               .initiate(eventLoop);
    }

    private Future<Result<SuccessType>> initiate(EventLoop eventLoop)
    {
        if (logger.isTraceEnabled())
            logger.trace("creating outbound bootstrap to {}, requestVersion: {}", settings, requestMessagingVersion);

        if (!settings.authenticate())
        {
            // interrupt other connections, so they must attempt to re-authenticate
            MessagingService.instance().interruptOutbound(settings.endpoint);
            return new FailedFuture<>(eventLoop, new IOException("authentication failed to " + settings.endpoint));
        }

        // this is a bit ugly, but is the easiest way to ensure that if we timeout we can propagate a suitable error message
        // and still guarantee that, if on timing out we raced with success, the successfully created channel is handled
        AtomicBoolean timedout = new AtomicBoolean();
        Future<Void> bootstrap = createBootstrap(eventLoop)
                                 .connect()
                                 .addListener(future -> {
                                     if (!future.isSuccess())
                                     {
                                         if (future.isCancelled() && !timedout.get())
                                             resultPromise.cancel(true);
                                         else if (future.isCancelled())
                                             resultPromise.tryFailure(new IOException("Timeout handshaking with " + settings.connectTo));
                                         else
                                             resultPromise.tryFailure(future.cause());
                                     }
                                 });

        ScheduledFuture<?> timeout = eventLoop.schedule(() -> {
            timedout.set(true);
            bootstrap.cancel(false);
        }, TIMEOUT_MILLIS, MILLISECONDS);
        bootstrap.addListener(future -> timeout.cancel(true));

        // Note that the bootstrap future's listeners may be invoked outside of the eventLoop,
        // as Epoll failures on connection and disconnect may be run on the GlobalEventExecutor
        // Since this FutureResult's listeners are all given to our resultPromise, they are guaranteed to be invoked by the eventLoop.
        return new FutureResult<>(resultPromise, bootstrap);
    }

    /**
     * Create the {@link Bootstrap} for connecting to a remote peer. This method does <b>not</b> attempt to connect to the peer,
     * and thus does not block.
     */
    private Bootstrap createBootstrap(EventLoop eventLoop)
    {
        Bootstrap bootstrap = instance.newBootstrap(eventLoop, settings.tcpUserTimeoutInMS)
                              .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.tcpConnectTimeoutInMS)
                              .option(ChannelOption.SO_KEEPALIVE, true)
                              .option(ChannelOption.SO_REUSEADDR, true)
                              .option(ChannelOption.TCP_NODELAY, settings.tcpNoDelay)
                              .option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, NoSizeEstimator.instance)
                              .handler(new Initializer());

        if (settings.socketSendBufferSizeInBytes > 0)
            bootstrap.option(ChannelOption.SO_SNDBUF, settings.socketSendBufferSizeInBytes);

        InetAddressAndPort remoteAddress = settings.connectTo;
        bootstrap.remoteAddress(new InetSocketAddress(remoteAddress.address, remoteAddress.port));
        return bootstrap;
    }

    private class Initializer extends ChannelInitializer<SocketChannel>
    {
        public void initChannel(SocketChannel channel) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (settings.withEncryption())
            {
                // check if we should actually encrypt this connection
                SslContext sslContext = SSLFactory.getOrCreateSslContext(settings.encryption, true, SSLFactory.SocketType.CLIENT);
                // for some reason channel.remoteAddress() will return null
                InetAddressAndPort address = settings.endpoint;
                InetSocketAddress peer = settings.encryption.require_endpoint_verification ? new InetSocketAddress(address.address, address.port) : null;
                SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
                logger.trace("creating outbound netty SslContext: context={}, engine={}", sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
                pipeline.addFirst("ssl", sslHandler);
            }

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            pipeline.addLast("handshake", new Handler());
        }

    }

    private class Handler extends ByteToMessageDecoder
    {
        /**
         * {@inheritDoc}
         *
         * Invoked when the channel is made active, and sends out the {@link Initiate}.
         * In the case of streaming, we do not require a full bi-directional handshake; the initial message,
         * containing the streaming protocol version, is all that is required.
         */
        @Override
        public void channelActive(final ChannelHandlerContext ctx)
        {
            Mode mode = type == STREAM ? Mode.STREAM : Mode.REGULAR;
            Initiate msg = new Initiate(requestMessagingVersion, settings.acceptVersions, mode, settings.withCompression(), settings.withCrc(), getBroadcastAddressAndPort());
            logger.trace("starting handshake with peer {}, msg = {}", settings.connectTo, msg);
            AsyncChannelPromise.writeAndFlush(ctx, msg.encode(),
                  future -> { if (!future.isSuccess()) exceptionCaught(ctx, future.cause()); });

            if (type == STREAM && requestMessagingVersion < VERSION_40)
                ctx.pipeline().remove(this);

            ctx.fireChannelActive();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelInactive(ctx);
            resultPromise.tryFailure(new ClosedChannelException());
        }

        /**
         * {@inheritDoc}
         *
         * Invoked when we get the response back from the peer, which should contain the second message of the internode messaging handshake.
         * <p>
         * If the peer's protocol version does not equal what we were expecting, immediately close the channel (and socket);
         * do *not* send out the third message of the internode messaging handshake.
         * We will reconnect on the appropriate protocol version.
         */
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        {
            try
            {
                Accept msg = Accept.maybeDecode(in, requestMessagingVersion);
                if (msg == null)
                    return;

                int useMessagingVersion = msg.useMessagingVersion;
                int peerMessagingVersion = msg.maxMessagingVersion;
                logger.trace("received second handshake message from peer {}, msg = {}", settings.connectTo, msg);

                FrameEncoder frameEncoder = null;
                Result<SuccessType> result;
                if (useMessagingVersion > 0)
                {
                    if (useMessagingVersion < settings.acceptVersions.min || useMessagingVersion > settings.acceptVersions.max)
                    {
                        result = incompatible(useMessagingVersion, peerMessagingVersion);
                    }
                    else
                    {
                        // This is a bit ugly
                        if (type != STREAM)
                        {
                            if (settings.withCompression)
                                frameEncoder = FrameEncoderLZ4.fastInstance;
                            else if (settings.withCrc)
                                frameEncoder = FrameEncoderCrc.instance;
                            else
                                frameEncoder = FrameEncoderUnprotected.instance;

                            result = (Result<SuccessType>) messagingSuccess(ctx.channel(), useMessagingVersion, frameEncoder.allocator());
                        }
                        else
                        {
                            result = (Result<SuccessType>) streamingSuccess(ctx.channel(), useMessagingVersion);
                        }
                    }
                }
                else
                {
                    assert type != STREAM;
                    // pre40 handshake responses only (can be a post40 node)
                    if (peerMessagingVersion == requestMessagingVersion
                        || peerMessagingVersion > settings.acceptVersions.max) // this clause is for impersonating 3.0 node in testing only
                    {
                        if (settings.withCompression)
                            frameEncoder = FrameEncoderLegacyLZ4.instance;
                        else
                            frameEncoder = FrameEncoderLegacy.instance;

                        result = (Result<SuccessType>) messagingSuccess(ctx.channel(), requestMessagingVersion, frameEncoder.allocator());
                    }
                    else if (peerMessagingVersion < settings.acceptVersions.min)
                        result = incompatible(-1, peerMessagingVersion);
                    else
                        result = retry(peerMessagingVersion);

                    if (result.isSuccess())
                    {
                        ConfirmOutboundPre40 message = new ConfirmOutboundPre40(settings.acceptVersions.max, getBroadcastAddressAndPort());
                        AsyncChannelPromise.writeAndFlush(ctx, message.encode());
                    }
                }

                ChannelPipeline pipeline = ctx.pipeline();
                if (result.isSuccess())
                {
                    if (type != STREAM)
                    {
                        assert frameEncoder != null;
                        frameEncoder.addLastTo(pipeline);
                    }
                    pipeline.remove(this);
                }
                else
                {
                    pipeline.close();
                }

                resultPromise.trySuccess(result);
            }
            catch (Throwable t)
            {
                exceptionCaught(ctx, t);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        {
            JVMStabilityInspector.inspectThrowable(cause);
            resultPromise.tryFailure(cause);
            if (isConnectionResetException(cause))
                logger.info("Failed to connect to peer {}", settings.endpoint, cause);
            else
                logger.error("Failed to handshake with peer {}", settings.endpoint, cause);
            ctx.close();
        }
    }

    /**
     * The result of the handshake. Handshake has 3 possible outcomes:
     *  1) it can be successful, in which case the channel and version to used is returned in this result.
     *  2) we may decide to disconnect to reconnect with another protocol version (namely, the version is passed in this result).
     *  3) we can have a negotiation failure for an unknown reason. (#sadtrombone)
     */
    public static class Result<SuccessType extends Result.Success>
    {
        /**
         * Describes the result of receiving the response back from the peer (Message 2 of the handshake)
         * and implies an action that should be taken.
         */
        enum Outcome
        {
            SUCCESS, RETRY, INCOMPATIBLE
        }

        public static class Success<SuccessType extends Success> extends Result<SuccessType>
        {
            public final Channel channel;
            public final int messagingVersion;
            Success(Channel channel, int messagingVersion)
            {
                super(Outcome.SUCCESS);
                this.channel = channel;
                this.messagingVersion = messagingVersion;
            }
        }

        public static class StreamingSuccess extends Success<StreamingSuccess>
        {
            StreamingSuccess(Channel channel, int messagingVersion)
            {
                super(channel, messagingVersion);
            }
        }

        public static class MessagingSuccess extends Success<MessagingSuccess>
        {
            public final FrameEncoder.PayloadAllocator allocator;
            MessagingSuccess(Channel channel, int messagingVersion, FrameEncoder.PayloadAllocator allocator)
            {
                super(channel, messagingVersion);
                this.allocator = allocator;
            }
        }

        static class Retry<SuccessType extends Success> extends Result<SuccessType>
        {
            final int withMessagingVersion;
            Retry(int withMessagingVersion)
            {
                super(Outcome.RETRY);
                this.withMessagingVersion = withMessagingVersion;
            }
        }

        static class Incompatible<SuccessType extends Success> extends Result<SuccessType>
        {
            final int closestSupportedVersion;
            final int maxMessagingVersion;
            Incompatible(int closestSupportedVersion, int maxMessagingVersion)
            {
                super(Outcome.INCOMPATIBLE);
                this.closestSupportedVersion = closestSupportedVersion;
                this.maxMessagingVersion = maxMessagingVersion;
            }
        }

        final Outcome outcome;

        private Result(Outcome outcome)
        {
            this.outcome = outcome;
        }

        boolean isSuccess() { return outcome == Outcome.SUCCESS; }
        public SuccessType success() { return (SuccessType) this; }
        static MessagingSuccess messagingSuccess(Channel channel, int messagingVersion, FrameEncoder.PayloadAllocator allocator) { return new MessagingSuccess(channel, messagingVersion, allocator); }
        static StreamingSuccess streamingSuccess(Channel channel, int messagingVersion) { return new StreamingSuccess(channel, messagingVersion); }

        public Retry retry() { return (Retry) this; }
        static <SuccessType extends Success> Result<SuccessType> retry(int withMessagingVersion) { return new Retry<>(withMessagingVersion); }

        public Incompatible incompatible() { return (Incompatible) this; }
        static <SuccessType extends Success> Result<SuccessType> incompatible(int closestSupportedVersion, int maxMessagingVersion) { return new Incompatible(closestSupportedVersion, maxMessagingVersion); }
    }

}
