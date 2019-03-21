package org.apache.cassandra.net.async;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.CRC32;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.Errors;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.collect.Iterables.*;
import static io.netty.channel.unix.Errors.ERRNO_ECONNRESET_NEGATIVE;
import static io.netty.channel.unix.Errors.ERROR_ECONNREFUSED_NEGATIVE;
import static java.util.Collections.*;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_40;

/**
 * A factory for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class NettyFactory
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    private static final int EVENT_THREADS = Integer.getInteger(Config.PROPERTY_PREFIX + "internode-event-threads", FBUtilities.getAvailableProcessors());

    /** a useful addition for debugging; simply set to true to get more data in your logs */
    static final boolean WIRETRACE = false;
    static
    {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    public static final boolean USE_EPOLL = NativeTransportService.useEpoll();

    /**
     * A factory instance that all normal, runtime code should use. Separate instances should only be used for testing.
     */
    public static final NettyFactory instance = new NettyFactory(USE_EPOLL);

    private final boolean useEpoll;

    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup defaultGroup;
    // we need a separate EventLoopGroup for outbound streaming because sendFile is blocking
    private final EventLoopGroup outboundStreamingGroup;
    final ExecutorService synchronousWorkExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("MessagingService-SynchronousWork"));

    /**
     * Constructor that allows modifying the {@link NettyFactory#useEpoll} for testing purposes. Otherwise, use the
     * default {@link #instance}.
     */
    private NettyFactory(boolean useEpoll)
    {
        this.useEpoll = useEpoll;
        this.acceptGroup = getEventLoopGroup(useEpoll, 1, "Messaging-AcceptLoop");
        this.defaultGroup = getEventLoopGroup(useEpoll, EVENT_THREADS, NamedThreadFactory.globalPrefix() + "Messaging-EventLoop");
        this.outboundStreamingGroup = getEventLoopGroup(useEpoll, EVENT_THREADS, "Streaming-EventLoop");
    }

    private static EventLoopGroup getEventLoopGroup(boolean useEpoll, int threadCount, String threadNamePrefix)
    {
        if (useEpoll)
        {
            logger.debug("using netty epoll event loop for pool prefix {}", threadNamePrefix);
            return new EpollEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
        }

        logger.debug("using netty nio event loop for pool prefix {}", threadNamePrefix);
        return new NioEventLoopGroup(threadCount, new DefaultThreadFactory(threadNamePrefix, true));
    }

    Bootstrap newBootstrap(EventLoop eventLoop, int tcpUserTimeoutInMS)
    {
        if (eventLoop == null)
            throw new IllegalArgumentException("must provide eventLoop");

        Class<? extends Channel> transport = useEpoll ? EpollSocketChannel.class
                                                      : NioSocketChannel.class;

        Bootstrap bootstrap = new Bootstrap()
                              .group(eventLoop)
                              .channel(transport)
                              .option(ChannelOption.ALLOCATOR, BufferPoolAllocator.instance)
                              .option(ChannelOption.SO_KEEPALIVE, true);
        if (useEpoll)
            bootstrap.option(EpollChannelOption.TCP_USER_TIMEOUT, tcpUserTimeoutInMS);

        return bootstrap;
    }

    ServerBootstrap newServerBootstrap()
    {
        Class<? extends ServerChannel> transport = useEpoll ? EpollServerSocketChannel.class
                                                            : NioServerSocketChannel.class;

        return new ServerBootstrap()
               .group(acceptGroup, defaultGroup)
               .channel(transport)
               .option(ChannelOption.ALLOCATOR, BufferPoolAllocator.instance)
               .option(ChannelOption.SO_REUSEADDR, true);
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
        synchronousWorkExecutor.shutdown();
    }

    public void awaitTerminationUntil(long deadlineNanos) throws InterruptedException, TimeoutException
    {
        List<ExecutorService> groups = ImmutableList.of(acceptGroup, defaultGroup, outboundStreamingGroup, synchronousWorkExecutor);
        for (ExecutorService executor : concat(groups, singleton(synchronousWorkExecutor)))
        {
            long wait = deadlineNanos - System.nanoTime();
            if (wait <= 0 || !executor.awaitTermination(wait, NANOSECONDS))
                throw new TimeoutException();
        }
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
