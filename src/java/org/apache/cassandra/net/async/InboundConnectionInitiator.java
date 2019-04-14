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
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.Future;

import javax.net.ssl.SSLSession;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.async.StreamingInboundHandler;

import static java.lang.Math.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.net.MessagingService.*;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.MessagingService.minimum_version;
import static org.apache.cassandra.net.async.SocketFactory.WIRETRACE;
import static org.apache.cassandra.net.async.SocketFactory.newSslHandler;

public class InboundConnectionInitiator
{
    private static final Logger logger = LoggerFactory.getLogger(InboundConnectionInitiator.class);

    private static class Initializer extends ChannelInitializer<SocketChannel>
    {
        private final InboundConnectionSettings settings;
        private final ChannelGroup channelGroup;

        public Initializer(InboundConnectionSettings settings, ChannelGroup channelGroup)
        {
            this.settings = settings;
            this.channelGroup = channelGroup;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            channelGroup.add(channel);

            channel.config().setOption(ChannelOption.ALLOCATOR, BufferPoolAllocator.instance);
            channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
            channel.config().setOption(ChannelOption.SO_REUSEADDR, true);
            channel.config().setOption(ChannelOption.TCP_NODELAY, true); // we only send handshake messages; no point ever delaying

            ChannelPipeline pipeline = channel.pipeline();

            // order of handlers: ssl -> logger -> handshakeHandler
            if (settings.encryption.enabled)
            {
                if (settings.encryption.optional)
                {
                    pipeline.addFirst("ssl", new OptionalSslHandler(settings.encryption));
                }
                else
                {
                    SslContext sslContext = SSLFactory.getOrCreateSslContext(settings.encryption, true, SSLFactory.SocketType.SERVER);
                    InetSocketAddress peer = settings.encryption.require_endpoint_verification ? channel.remoteAddress() : null;
                    SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
                    logger.trace("creating inbound netty SslContext: context={}, engine={}", sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
                    pipeline.addFirst("ssl", sslHandler);
                }
            }

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            channel.pipeline().addLast("handshake", new Handler(settings));
        }
    }

    /**
     * Create a {@link Channel} that listens on the {@code localAddr}. This method will block while trying to bind to the address,
     * but it does not make a remote call.
     */
    private static ChannelFuture bind(Initializer initializer) throws ConfigurationException
    {
        logger.info("Listening on {}", initializer.settings);

        ServerBootstrap bootstrap = initializer.settings.socketFactory
                                    .newServerBootstrap()
                                    .option(ChannelOption.SO_BACKLOG, 1 << 9)
                                    .childHandler(initializer);

        int socketReceiveBufferSizeInBytes = initializer.settings.socketReceiveBufferSizeInBytes;
        if (socketReceiveBufferSizeInBytes > 0)
            bootstrap.childOption(ChannelOption.SO_RCVBUF, socketReceiveBufferSizeInBytes);

        InetAddressAndPort bind = initializer.settings.bindAddress;
        ChannelFuture channelFuture = bootstrap.bind(new InetSocketAddress(bind.address, bind.port));

        if (!channelFuture.awaitUninterruptibly().isSuccess())
        {
            if (channelFuture.channel().isOpen())
                channelFuture.channel().close();

            Throwable failedChannelCause = channelFuture.cause();

            String causeString = "";
            if (failedChannelCause != null && failedChannelCause.getMessage() != null)
                causeString = failedChannelCause.getMessage();

            if (causeString.contains("in use"))
            {
                throw new ConfigurationException(bind + " is in use by another process.  Change listen_address:storage_port " +
                                                 "in cassandra.yaml to values that do not conflict with other services");
            }
            // looking at the jdk source, solaris/windows bind failue messages both use the phrase "cannot assign requested address".
            // windows message uses "Cannot" (with a capital 'C'), and solaris (a/k/a *nux) doe not. hence we search for "annot" <sigh>
            else if (causeString.contains("annot assign requested address"))
            {
                throw new ConfigurationException("Unable to bind to address " + bind
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            }
            else
            {
                throw new ConfigurationException("failed to bind to: " + bind, failedChannelCause);
            }
        }

        return channelFuture;
    }

    public static ChannelFuture bind(InboundConnectionSettings settings, ChannelGroup channelGroup)
    {
        return bind(new Initializer(settings, channelGroup));
    }

    /**
     * 'Server-side' component that negotiates the internode handshake when establishing a new connection.
     * This handler will be the first in the netty channel for each incoming connection (secure socket (TLS) notwithstanding),
     * and once the handshake is successful, it will configure the proper handlers ({@link InboundMessageHandler}
     * or {@link StreamingInboundHandler}) and remove itself from the working pipeline.
     */
    static class Handler extends ByteToMessageDecoder
    {
        private final InboundConnectionSettings settings;

        private HandshakeProtocol.Initiate initiate;
        private HandshakeProtocol.ConfirmOutboundPre40 confirmOutboundPre40;

        /**
         * A future the essentially places a timeout on how long we'll wait for the peer
         * to complete the next step of the handshake.
         */
        private Future<?> handshakeTimeout;

        Handler(InboundConnectionSettings settings)
        {
            this.settings = settings;
        }

        /**
         * On registration, immediately schedule a timeout to kill this connection if it does not handshake promptly,
         * and authenticate the remote address.
         */
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception
        {
            handshakeTimeout = ctx.executor().schedule(() -> {
                logger.error("Timeout handshaking with " + ctx.channel().remoteAddress());
                failHandshake(ctx);
            }, HandshakeProtocol.TIMEOUT_MILLIS, MILLISECONDS);

            logSsl(ctx);
            authenticate(ctx.channel().remoteAddress());
        }

        private void authenticate(SocketAddress socketAddress) throws IOException
        {
            if (socketAddress.getClass().getSimpleName().equals("EmbeddedSocketAddress"))
                return;

            if (!(socketAddress instanceof InetSocketAddress))
                throw new IOException(String.format("Unexpected SocketAddress type: %s, %s", socketAddress.getClass(), socketAddress));

            InetSocketAddress addr = (InetSocketAddress)socketAddress;
            if (!settings.authenticate(addr.getAddress(), addr.getPort()))
                throw new IOException("Authentication failure for inbound connection from peer " + addr);
        }

        private void logSsl(ChannelHandlerContext ctx)
        {
            SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
            if (sslHandler != null)
            {
                SSLSession session = sslHandler.engine().getSession();
                logger.info("connection from peer {}, protocol = {}, cipher suite = {}",
                            ctx.channel().remoteAddress(), session.getProtocol(), session.getCipherSuite());
            }
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
        {
            if (initiate == null) initiate(ctx, in);
            else if (initiate.acceptVersions == null && confirmOutboundPre40 == null) confirmPre40(ctx, in);
            else throw new IllegalStateException("Should no longer be on pipeline");
        }

        void initiate(ChannelHandlerContext ctx, ByteBuf in) throws IOException
        {
            initiate = HandshakeProtocol.Initiate.maybeDecode(in);
            if (initiate == null)
                return;

            logger.trace("Received handshake initiation message from peer {}, message = {}", ctx.channel().remoteAddress(), initiate);
            if (initiate.acceptVersions != null)
            {
                logger.trace("Connection version {} (min {}) from {}", initiate.acceptVersions.max, initiate.acceptVersions.min, initiate.from);

                final AcceptVersions accept;
                switch (initiate.mode)
                {
                    case REGULAR: accept = settings.acceptMessaging; break;
                    case STREAM: accept = settings.acceptStreaming; break;
                    default: throw new IllegalStateException();
                }

                int useMessagingVersion = max(accept.min, min(accept.max, initiate.acceptVersions.max));
                ByteBuf flush = new HandshakeProtocol.Accept(useMessagingVersion, accept.max).encode(ctx.alloc());

                AsyncChannelPromise.writeAndFlush(ctx, flush, (ChannelFutureListener) future -> {
                    if (!future.isSuccess())
                        exceptionCaught(future.channel(), future.cause());
                });

                if (initiate.acceptVersions.min > accept.max)
                {
                    logger.info("peer {} only supports messaging versions higher ({}) than this node supports ({})", ctx.channel().remoteAddress(), initiate.acceptVersions.min, current_version);
                    failHandshake(ctx);
                }
                else if (initiate.acceptVersions.max < accept.min)
                {
                    logger.info("peer {} only supports messaging versions lower ({}) than this node supports ({})", ctx.channel().remoteAddress(), initiate.acceptVersions.max, minimum_version);
                    failHandshake(ctx);
                }
                else
                {
                    switch (initiate.mode)
                    {
                        case STREAM:
                            setupStreamingPipeline(initiate.from, ctx);
                            break;
                        case REGULAR:
                            setupMessagingPipeline(initiate.from, useMessagingVersion, initiate.acceptVersions.max, ctx.pipeline());
                    }
                }
            }
            else
            {
                int version = initiate.requestMessagingVersion;
                assert version < VERSION_40 && version >= settings.acceptMessaging.min;
                logger.trace("Connection version {} from {}", version, ctx.channel().remoteAddress());

                switch(initiate.mode)
                {
                    case STREAM:
                    {
                        // streaming connections are per-session and have a fixed version.  we can't do anything with a wrong-version stream connection, so drop it.
                        if (version != settings.acceptStreaming.max)
                        {
                            logger.warn("Received stream using protocol version {} (my version {}). Terminating connection", version, settings.acceptStreaming.max);
                            failHandshake(ctx);
                        }
                        setupStreamingPipeline(initiate.from, ctx);
                        break;
                    }
                    case REGULAR:
                    {
                        // if this version is < the MS version the other node is trying
                        // to connect with, the other node will disconnect
                        ByteBuf response = HandshakeProtocol.Accept.respondPre40(settings.acceptMessaging.max, ctx.alloc());
                        AsyncChannelPromise.writeAndFlush(ctx, response,
                              (ChannelFutureListener) future -> {
                                   if (!future.isSuccess())
                                       exceptionCaught(future.channel(), future.cause());
                        });

                        if (version < VERSION_30)
                            throw new IOException(String.format("Unable to read obsolete message version %s from %s; The earliest version supported is 3.0.0", version, ctx.channel().remoteAddress()));

                        // we don't setup the messaging pipeline here, as the legacy messaging handshake requires one more message to finish
                    }
                }
            }

        }

        /**
         * Handles the third (and last) message in the internode messaging handshake protocol for pre40 nodes.
         * Grabs the protocol version and IP addr the peer wants to use.
         */
        @VisibleForTesting
        void confirmPre40(ChannelHandlerContext ctx, ByteBuf in)
        {
            confirmOutboundPre40 = HandshakeProtocol.ConfirmOutboundPre40.maybeDecode(in);
            if (confirmOutboundPre40 == null)
                return;

            logger.trace("Received third handshake message from peer {}, message = {}", ctx.channel().remoteAddress(), confirmOutboundPre40);
            setupMessagingPipeline(confirmOutboundPre40.from, initiate.requestMessagingVersion, confirmOutboundPre40.maxMessagingVersion, ctx.pipeline());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        {
            exceptionCaught(ctx.channel(), cause);
        }

        private void exceptionCaught(Channel channel, Throwable cause)
        {
            logger.error("Failed to properly handshake with peer {}. Closing the channel.", channel.remoteAddress(), cause);
            failHandshake(channel);
        }

        private void failHandshake(ChannelHandlerContext ctx)
        {
            failHandshake(ctx.channel());
        }

        private void failHandshake(Channel channel)
        {
            channel.close();
            if (handshakeTimeout != null)
                handshakeTimeout.cancel(true);
        }

        private void setupStreamingPipeline(InetAddressAndPort from, ChannelHandlerContext ctx)
        {
            handshakeTimeout.cancel(true);

            ChannelPipeline pipeline = ctx.pipeline();
            Channel channel = ctx.channel();

            // TODO: cleanup pre40 vs post40
            if (from == null)
            {
                InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
                from = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            }
            pipeline.replace(this, "streamInbound", new StreamingInboundHandler(from, current_version, null));

            // pass a custom recv ByteBuf allocator to the channel. the default recv ByteBuf size is 1k, but in streaming we're
            // dealing with large bulk blocks of data, let's default to larger sizes
            // TODO we should be allocating fixed size large buffers here, and accumulating within them
            //      at no point on a large messages or streaming connection does it make sense to do otherwise
            //      on a small connection it might offer benefit to allocate tiny buffers, but it still seems unlikely
            //      since the buffer lifetime is limited; we just run the risk of needing to allocate two or more to read a message.
            ctx.channel().config().setRecvByteBufAllocator(new AdaptiveRecvByteBufAllocator(1 << 10, 1 << 13, 1 << 16));
        }

        @VisibleForTesting
        void setupMessagingPipeline(InetAddressAndPort from, int useMessagingVersion, int maxMessagingVersion, ChannelPipeline pipeline)
        {
            handshakeTimeout.cancel(true);
            
            // record the "true" endpoint, i.e. the one the peer is identified with, as opposed to the socket it connected over
            instance().versions.set(from, maxMessagingVersion);

            FrameDecoder frameDecoder;
            if (initiate.withCompression && useMessagingVersion >= VERSION_40)
                frameDecoder = FrameDecoderLZ4.fast();
            else if (initiate.withCompression)
                frameDecoder = new FrameDecoderLegacyLZ4(useMessagingVersion);
            else if (initiate.withCrc)
                frameDecoder = FrameDecoderCrc.create();
            else if (useMessagingVersion >= VERSION_40)
                frameDecoder = new FrameDecoderUnprotected();
            else
                frameDecoder = new FrameDecoderLegacy(useMessagingVersion);

            frameDecoder.addLastTo(pipeline);

            logger.info("connection established from {}, version = {}, compress = {}, encryption = {}", from, useMessagingVersion, initiate.withCompression,
                        SocketFactory.encryptionLogStatement(settings.encryption));

            InboundMessageHandler handler =
                settings.handlers.apply(from).createHandler(frameDecoder,
                                                            settings.socketFactory.synchronousWorkExecutor,
                                                            pipeline.channel(),
                                                            useMessagingVersion);
            pipeline.addLast("deserialize", handler);

            pipeline.remove(this);
        }
    }

    private static class OptionalSslHandler extends ByteToMessageDecoder
    {
        private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

        OptionalSslHandler(EncryptionOptions.ServerEncryptionOptions encryptionOptions)
        {
            this.encryptionOptions = encryptionOptions;
        }

        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
        {
            if (in.readableBytes() < 5)
            {
                // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
                // once more bytes a ready.
                return;
            }

            if (SslHandler.isEncrypted(in))
            {
                // Connection uses SSL/TLS, replace the detection handler with a SslHandler and so use encryption.
                SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions, true, SSLFactory.SocketType.SERVER);
                Channel channel = ctx.channel();
                InetSocketAddress peer = encryptionOptions.require_endpoint_verification ? (InetSocketAddress) channel.remoteAddress() : null;
                SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
                ctx.pipeline().replace(this, "ssl", sslHandler);
            }
            else
            {
                // Connection use no TLS/SSL encryption, just remove the detection handler and continue without
                // SslHandler in the pipeline.
                ctx.pipeline().remove(this);
            }
        }
    }
}
