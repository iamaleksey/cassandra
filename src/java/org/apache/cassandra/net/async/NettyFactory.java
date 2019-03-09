package org.apache.cassandra.net.async;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
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
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.FastThreadLocal;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MessagingService.VERSION_40;

/**
 * A factory for building Netty {@link Channel}s. Channels here are setup with a pipeline to participate
 * in the internode protocol handshake, either the inbound or outbound side as per the method invoked.
 */
public final class NettyFactory
{
    private static final Logger logger = LoggerFactory.getLogger(NettyFactory.class);

    private static final int EVENT_THREADS = Integer.getInteger(Config.PROPERTY_PREFIX + "internode-event-threads", FBUtilities.getAvailableProcessors());

    private static final int LEGACY_COMPRESSION_BLOCK_SIZE = 1 << 16;
    private static final int LEGACY_LZ4_HASH_SEED = 0x9747b28c;

    /** a useful addition for debugging; simply set to true to get more data in your logs */
    static final boolean WIRETRACE = false;
    static
    {
        if (WIRETRACE)
            InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    public static final boolean USE_EPOLL = NativeTransportService.useEpoll();

    private static final FastThreadLocal<CRC32> crc32 = new FastThreadLocal<CRC32>()
    {
        @Override
        protected CRC32 initialValue()
        {
            return new CRC32();
        }
    };

    static CRC32 crc32()
    {
        CRC32 crc = crc32.get();
        crc.reset();
        return crc;
    }

    static final int CRC24_INIT = 0x875060;
    // Polynomial chosen from https://users.ece.cmu.edu/~koopman/crc/index.html
    // Provides hamming distance of 8 for messages up to length 105 bits;
    // we only support 8-64 bits at present, with an expected range of 40-48.
    static final int CRC24_POLY = 0x1974F0B;

    /**
     * NOTE: the order of bytes must reach the wire in the same order the CRC is computed, with the CRC
     * immediately following in a trailer.  Since we read in least significant byte order, if you
     * write to a buffer using putInt or putLong, the byte order will be reversed and
     * you will lose the guarantee of protection from burst corruptions of 24 bits in length.
     *
     * Make sure either to write byte-by-byte to the wire, or to use Integer/Long.reverseBytes if you
     * write to a BIG_ENDIAN buffer.
     *
     * See http://users.ece.cmu.edu/~koopman/pubs/ray06_crcalgorithms.pdf
     *
     * Complain to the ethernet spec writers, for having inverse bit to byte significance order.
     *
     * Note we use the most naive algorithm here.  We support at most 8 bytes, and typically supply
     * 5 or fewer, so any efficiency of a table approach is swallowed by the time to hit L3, even
     * for a tiny (4bit) table.
     *
     * @param bytes an up to 8-byte register containing bytes to compute the CRC over
     *              the bytes AND bits will be read least-significant to most significant.
     * @param len   the number of bytes, greater than 0 and fewer than 9, to be read from bytes
     * @return      the least-significant bit AND byte order crc24 using the CRC24_POLY polynomial
     */
    public static int crc24(long bytes, int len)
    {
        int crc = CRC24_INIT;
        while (len-- > 0)
        {
            crc ^= (bytes & 0xff) << 16;
            bytes >>= 8;

            for (int i = 0; i < 8; i++)
            {
                crc <<= 1;
                if ((crc & 0x1000000) != 0)
                    crc ^= CRC24_POLY;
            }
        }
        return crc;
    }

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
               .option(ChannelOption.SO_KEEPALIVE, true)
               .option(ChannelOption.SO_REUSEADDR, true)
               .option(ChannelOption.TCP_NODELAY, true); // we only send handshake messages; no point ever delaying
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

    static int computeCrc32(ByteBuf buffer, int startReaderIndex, int endReaderIndex)
    {
        CRC32 crc = crc32();
        crc.update(buffer.internalNioBuffer(startReaderIndex, endReaderIndex - startReaderIndex));
        return (int) crc.getValue();
    }

    static int computeCrc32(ByteBuffer buffer, int start, int end)
    {
        CRC32 crc = crc32();
        updateCrc32(crc, buffer, start, end);
        return (int) crc.getValue();
    }

    static void updateCrc32(CRC32 crc, ByteBuffer buffer, int start, int end)
    {
        int savePosition = buffer.position();
        int saveLimit = buffer.limit();
        buffer.limit(end);
        buffer.position(start);
        crc.update(buffer);
        buffer.limit(saveLimit);
        buffer.position(savePosition);
    }

    static ChannelOutboundHandlerAdapter getLZ4Encoder(int messagingVersion)
    {
        if (messagingVersion < VERSION_40)
            return new Lz4FrameEncoder(LZ4Factory.fastestInstance(), false, LEGACY_COMPRESSION_BLOCK_SIZE, XXHashFactory.fastestInstance().newStreamingHash32(LEGACY_LZ4_HASH_SEED).asChecksum());
        return LZ4Encoder.fastInstance;
    }

    static ChannelInboundHandlerAdapter getLZ4Decoder(int messagingVersion)
    {
        if (messagingVersion < VERSION_40)
            return new Lz4FrameDecoder(LZ4Factory.fastestInstance(), XXHashFactory.fastestInstance().newStreamingHash32(LEGACY_LZ4_HASH_SEED).asChecksum());
        return LZ4Decoder.fast();
    }

    public EventLoopGroup defaultGroup()
    {
        return defaultGroup;
    }

    public EventLoopGroup outboundStreamingGroup()
    {
        return outboundStreamingGroup;
    }

    public void close() throws InterruptedException
    {
        EventLoopGroup[] groups = new EventLoopGroup[] { acceptGroup, defaultGroup };
        for (EventLoopGroup group : groups)
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        for (EventLoopGroup group : groups)
            group.awaitTermination(60, TimeUnit.SECONDS);
    }

}
