package org.apache.cassandra.net.async;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;

import static io.netty.channel.unix.Errors.ERRNO_ECONNRESET_NEGATIVE;
import static io.netty.channel.unix.Errors.ERROR_ECONNREFUSED_NEGATIVE;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A factory for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class SocketFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SocketFactory.class);

    private static final int EVENT_THREADS = Integer.getInteger(Config.PROPERTY_PREFIX + "internode-event-threads", FBUtilities.getAvailableProcessors());

    public enum Provider { EPOLL, NIO }
    public static final Provider DEFAULT_PROVIDER = NativeTransportService.useEpoll() ? Provider.EPOLL : Provider.NIO;

    /** a useful addition for debugging; simply set to true to get more data in your logs */
    static final boolean WIRETRACE = false;
    static
    {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup defaultGroup;
    // we need a separate EventLoopGroup for outbound streaming because sendFile is blocking
    private final EventLoopGroup outboundStreamingGroup;
    public final ExecutorService synchronousWorkExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("MessagingService-SynchronousWork"));

    public SocketFactory() { this(DEFAULT_PROVIDER); }
    public SocketFactory(Provider provider)
    {
        this.acceptGroup = getEventLoopGroup(provider, 1, "Messaging-AcceptLoop");
        this.defaultGroup = getEventLoopGroup(provider, EVENT_THREADS, NamedThreadFactory.globalPrefix() + "Messaging-EventLoop");
        this.outboundStreamingGroup = getEventLoopGroup(provider, EVENT_THREADS, "Streaming-EventLoop");
        assert    provider == providerOf(acceptGroup)
               && provider == providerOf(defaultGroup)
               && provider == providerOf(outboundStreamingGroup);
    }

    private static EventLoopGroup getEventLoopGroup(Provider provider, int threadCount, String threadNamePrefix)
    {
        switch (provider)
        {
            case EPOLL:
                logger.debug("using netty epoll event loop for pool prefix {}", threadNamePrefix);
                return new EpollEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
            case NIO:
                logger.debug("using netty nio event loop for pool prefix {}", threadNamePrefix);
                return new NioEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
            default:
                throw new IllegalStateException();
        }
    }

    static Provider providerOf(EventLoopGroup eventLoopGroup)
    {
        while (eventLoopGroup instanceof SingleThreadEventLoop)
            eventLoopGroup = ((SingleThreadEventLoop) eventLoopGroup).parent();

        if (eventLoopGroup instanceof EpollEventLoopGroup)
            return Provider.EPOLL;
        if (eventLoopGroup instanceof NioEventLoopGroup)
            return Provider.NIO;
        throw new IllegalStateException();
    }

    static Bootstrap newBootstrap(EventLoop eventLoop, int tcpUserTimeoutInMS)
    {
        if (eventLoop == null)
            throw new IllegalArgumentException("must provide eventLoop");

        Bootstrap bootstrap = new Bootstrap()
                              .group(eventLoop)
                              .option(ChannelOption.ALLOCATOR, BufferPoolAllocator.instance)
                              .option(ChannelOption.SO_KEEPALIVE, true);

        switch (providerOf(eventLoop))
        {
            case EPOLL:
                bootstrap.channel(EpollSocketChannel.class);
                bootstrap.option(EpollChannelOption.TCP_USER_TIMEOUT, tcpUserTimeoutInMS);
                break;
            case NIO:
                bootstrap.channel(NioSocketChannel.class);
        }
        return bootstrap;
    }

    ServerBootstrap newServerBootstrap()
    {
        return newServerBootstrap(acceptGroup, defaultGroup);
    }

    static ServerBootstrap newServerBootstrap(EventLoopGroup acceptGroup, EventLoopGroup defaultGroup)
    {
        ServerBootstrap bootstrap = new ServerBootstrap()
               .group(acceptGroup, defaultGroup)
               .option(ChannelOption.ALLOCATOR, BufferPoolAllocator.instance)
               .option(ChannelOption.SO_REUSEADDR, true);

        switch (providerOf(defaultGroup))
        {
            case EPOLL:
               bootstrap.channel(EpollServerSocketChannel.class);
               break;
            case NIO:
               bootstrap.channel(NioServerSocketChannel.class);
        }

        return bootstrap;
    }

    /**
     * Creates a new {@link SslHandler} from provided SslContext.
     * @param peer enables endpoint verification for remote address when not null
     */
    static SslHandler newSslHandler(Channel channel, SslContext sslContext, @Nullable InetSocketAddress peer)
    {
        if (peer == null)
        {
            return sslContext.newHandler(channel.alloc());
        }
        else
        {
            logger.debug("Creating SSL handler for {}:{}", peer.getHostString(), peer.getPort());
            SslHandler sslHandler = sslContext.newHandler(channel.alloc(), peer.getHostString(), peer.getPort());
            SSLEngine engine = sslHandler.engine();
            SSLParameters sslParameters = engine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(sslParameters);
            return sslHandler;
        }
    }

    static String encryptionLogStatement(EncryptionOptions options)
    {
        if (options == null)
            return "disabled";

        String encryptionType = OpenSsl.isAvailable() ? "openssl" : "jdk";
        return "enabled (" + encryptionType + ')';
    }

    public EventLoopGroup defaultGroup()
    {
        return defaultGroup;
    }

    public EventLoopGroup outboundStreamingGroup()
    {
        return outboundStreamingGroup;
    }

    public void shutdownNow()
    {
        acceptGroup.shutdownGracefully(0, 2, SECONDS);
        defaultGroup.shutdownGracefully(0, 2, SECONDS);
        outboundStreamingGroup.shutdownGracefully(0, 2, SECONDS);
        synchronousWorkExecutor.shutdownNow();
    }

    public void awaitTerminationUntil(long deadlineNanos) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> groups = ImmutableList.of(acceptGroup, defaultGroup, outboundStreamingGroup, synchronousWorkExecutor);
        ExecutorUtils.awaitTerminationUntil(deadlineNanos, groups);
    }

    public static boolean isConnectionResetException(Throwable t)
    {
        if (t instanceof ClosedChannelException)
            return true;
        if (t instanceof ConnectException)
            return true;
        if (t instanceof Errors.NativeIoException)
        {
            int errorCode = ((Errors.NativeIoException) t).expectedErr();
            return    errorCode == ERRNO_ECONNRESET_NEGATIVE
                   || errorCode != ERROR_ECONNREFUSED_NEGATIVE;
        }
        return false;
    }

}
