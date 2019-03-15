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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.AcceptVersions;
import org.apache.cassandra.utils.memory.BufferPool;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.getBits;
import static org.apache.cassandra.net.Message.validateLegacyProtocolMagic;
import static org.apache.cassandra.net.async.Crc.*;
import static org.apache.cassandra.net.async.Crc.computeCrc32;

/**
 * Messages for the handshake phase of the internode protocol.
 * <p>
 * The handshake's main purpose is to establish a protocol version that both side can talk, as well as exchanging a few connection
 * options/parameters. The handshake is composed of 3 messages, the first being sent by the initiator of the connection. The other
 * side then answer with the 2nd message. At that point, if a version mismatch is detected by the connection initiator,
 * it will simply disconnect and reconnect with a more appropriate version. But if the version is acceptable, the connection
 * initiator sends the third message of the protocol, after which it considers the connection ready.
 * <p>
 * See below for a more precise description of each of those 3 messages.
 * <p>
 * Note that this handshake protocol doesn't fully apply to streaming. For streaming, only the first message is sent,
 * after which the streaming protocol takes over (not documented here)
 */
public class HandshakeProtocol
{
    static final long TIMEOUT_MILLIS = 3 * DatabaseDescriptor.getRpcTimeout(MILLISECONDS);

    public enum Mode
    {
        STREAM,
        REGULAR // GOSSIP, SMALL or LARGE if pre40; GOSSIP or SMALL if post40
    }

    /**
     * The initial message sent when a node creates a new connection to a remote peer. This message contains:
     *   1) the {@link Message#PROTOCOL_MAGIC} number (4 bytes).
     *   2) the connection flags (4 bytes), which encodes:
     *      - the version the initiator thinks should be used for the connection (in practice, either the initiator
     *        version if it's the first time we connect to that remote since startup, or the last version known for that
     *        peer otherwise).
     *      - the "mode" of the connection: whether it is for streaming or for messaging.
     *      - whether compression should be used or not (if it is, compression is enabled _after_ the last message of the
     *        handshake has been sent).
     * <p>
     * More precisely, connection flags:
     * <pre>
     * {@code
     *                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |U U C M       |                |                               |
     * |N N M O       |     VERSION    |             unused            |
     * |U U P D       |                |                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * }
     * </pre>
     * UNU - unused bits lowest two bits; from a historical note: used to be "serializer type," which was always Binary
     * CMP - compression enabled bit
     * MOD - connection mode. If the bit is on, the connection is for streaming; if the bit is off, it is for inter-node messaging.
     * VERSION - {@link org.apache.cassandra.net.MessagingService#current_version}
     */
    public static class Initiate
    {
        /** Contains the PROTOCOL_MAGIC (int) and the flags (int). */
        private static final int MIN_LENGTH = 8;
        private static final int MAX_LENGTH = 12 + CompactEndpointSerializationHelper.MAXIMUM_SIZE;

        @Deprecated // this is ignored by post40 nodes, i.e. if maxMessagingVersion is set
        final int requestMessagingVersion;
        // the messagingVersion bounds the sender will accept to initiate a connection;
        // if the remote peer supports any, the newest supported version will be selected; otherwise the nearest supported version
        final AcceptVersions acceptVersions;
        final Mode mode;
        final boolean withCrc;
        final boolean withCompression;
        final InetAddressAndPort from;

        Initiate(int requestMessagingVersion, AcceptVersions acceptVersions, Mode mode, boolean withCompression, boolean withCrc, InetAddressAndPort from)
        {
            this.requestMessagingVersion = requestMessagingVersion;
            this.acceptVersions = acceptVersions;
            this.mode = mode;
            this.withCompression = withCompression;
            this.withCrc = withCrc;
            this.from = from;
        }

        @VisibleForTesting
        int encodeFlags()
        {
            int flags = 0;
            if (withCompression)
                flags |= 1 << 2;
            if (mode == Mode.STREAM)
                flags |= 1 << 3;
            if (withCrc)
                flags |= 1 << 4;

            flags |= (requestMessagingVersion << 8);
            if (acceptVersions.max < VERSION_40)
                return flags; // for testing, permit serializing as though we are pre40
            flags |= (acceptVersions.min << 16);
            flags |= (acceptVersions.max << 24);
            return flags;
        }

        public ByteBuf encode()
        {
            ByteBuffer buffer = BufferPool.get(MAX_LENGTH, BufferType.OFF_HEAP);
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(buffer))
            {
                out.writeInt(Message.PROTOCOL_MAGIC);
                out.writeInt(encodeFlags());
                if (acceptVersions.max >= VERSION_40)
                {
                    CompactEndpointSerializationHelper.instance.serialize(from, out, requestMessagingVersion);
                    out.writeInt(computeCrc32(buffer, 0, buffer.position()));
                }
                buffer.flip();
                return BufferPoolAllocator.wrapUnshared(buffer);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        static Initiate maybeDecode(ByteBuf buf) throws IOException
        {
            int start = buf.readerIndex();
            if (buf.readableBytes() < MIN_LENGTH)
                return null;

            try (ByteBufDataInputPlus in = new ByteBufDataInputPlus(buf))
            {
                validateLegacyProtocolMagic(in.readInt());
                int flags = in.readInt();

                int requestedMessagingVersion = getBits(flags, 8, 8);
                int minMessagingVersion = getBits(flags, 16, 8);
                int maxMessagingVersion = getBits(flags, 24, 8);
                boolean isStream = getBits(flags, 3, 1) == 1;
                boolean withCompression = getBits(flags, 2, 1) == 1;
                boolean withCrc = getBits(flags, 4, 1) == 1;
                Mode mode = isStream ? Mode.STREAM : Mode.REGULAR;

                InetAddressAndPort from = null;

                if (maxMessagingVersion >= MessagingService.VERSION_40)
                {
                    from = CompactEndpointSerializationHelper.instance.deserialize(in, requestedMessagingVersion);

                    int computed = computeCrc32(buf, start, buf.readerIndex());
                    int read = in.readInt();
                    if (read != computed)
                        throw new InvalidCrc(read, computed);
                }

                return new Initiate(requestedMessagingVersion,
                                    minMessagingVersion == 0 && maxMessagingVersion == 0
                                        ? null : new AcceptVersions(minMessagingVersion, maxMessagingVersion),
                                    mode, withCompression, withCrc, from);

            }
            catch (EOFException e)
            {
                buf.readerIndex(start);
                return null;
            }
        }

        @VisibleForTesting
        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Initiate))
                return false;

            Initiate that = (Initiate)other;
            return    this.mode == that.mode
                   && this.withCompression == that.withCompression
                   && this.withCrc == that.withCrc
                   && Objects.equals(this.requestMessagingVersion, that.requestMessagingVersion)
                   && Objects.equals(this.acceptVersions, that.acceptVersions);
        }

        @Override
        public String toString()
        {
            return String.format("First(request: %d, min: %d, max: %d, mode: %s, compress: %b)",
                                 requestMessagingVersion, acceptVersions.min, acceptVersions.max, mode, withCompression);
        }
    }


    /**
     * The second message of the handshake, sent by the node receiving the {@link Initiate} back to the
     * connection initiator.
     *
     * This message contains the messaging version of the peer sending this message
     * and the negotiated messaging version if one could be accepted by both peers,
     * or if not the closest version that this peer could support to the ones requested.
     */
    static class AcceptInbound
    {
        /** The messaging version sent by the receiving peer (int). */
        private static final int MAX_LENGTH = 12;

        final int useMessagingVersion;
        final int maxMessagingVersion;

        AcceptInbound(int useMessagingVersion, int maxMessagingVersion)
        {
            this.useMessagingVersion = useMessagingVersion;
            this.maxMessagingVersion = maxMessagingVersion;
        }

        public ByteBuf encode(ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.directBuffer(MAX_LENGTH);
            buffer.clear();
            buffer.writeInt(maxMessagingVersion);
            buffer.writeInt(useMessagingVersion);
            buffer.writeInt(computeCrc32(buffer, 0, 8));
            return buffer;
        }

        /**
         * Respond to pre40 nodes only with our current messagingVersion
         */
        public static ByteBuf respondPre40(int messagingVersion, ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.directBuffer(4);
            buffer.clear();
            buffer.writeInt(messagingVersion);
            return buffer;
        }

        static AcceptInbound maybeDecode(ByteBuf in) throws InvalidCrc
        {
            int readerIndex = in.readerIndex();
            if (in.readableBytes() < 4)
                return null;
            int maxMessagingVersion = in.readInt();
            int useMessagingVersion = 0;
            if (maxMessagingVersion >= VERSION_40)
            {
                if (in.readableBytes() < 8)
                {
                    in.readerIndex(readerIndex);
                    return null;
                }
                useMessagingVersion = in.readInt();
                // verify crc
                int computed = computeCrc32(in, readerIndex, readerIndex + 8);
                int read = in.readInt();
                if (read != computed)
                    throw new InvalidCrc(read, computed);
            }
            return new AcceptInbound(useMessagingVersion, maxMessagingVersion);
        }

        @VisibleForTesting
        @Override
        public boolean equals(Object other)
        {
            return other instanceof AcceptInbound
                   && this.useMessagingVersion == ((AcceptInbound) other).useMessagingVersion
                   && this.maxMessagingVersion == ((AcceptInbound) other).maxMessagingVersion;
        }

        @Override
        public String toString()
        {
            return String.format("AcceptInbound(use: %d, max: %d)", useMessagingVersion, maxMessagingVersion);
        }
    }

    /**
     * The third message of the handshake, sent by the connection initiator on reception of {@link AcceptInbound}.
     * This message contains:
     *   1) The connection initiator's {@link org.apache.cassandra.net.MessagingService#current_version} (4 bytes).
     *      This indicates the max messaging version supported by this node.
     *   2) The connection initiator's broadcast address as encoded by {@link org.apache.cassandra.net.CompactEndpointSerializationHelper}.
     *      This can be either 7 bytes for an IPv4 address, or 19 bytes for an IPv6 one, post40.
     *      This can be either 5 bytes for an IPv4 address, or 17 bytes for an IPv6 one, pre40.
     * <p>
     * This message concludes the handshake protocol. After that, the connection will used either for streaming, or to
     * send messages. If the connection is to be compressed, compression is enabled only after this message is sent/received.
     */
    static class ConfirmOutboundPre40
    {
        private static final int MAX_LENGTH = 4 + CompactEndpointSerializationHelper.MAXIMUM_SIZE;

        final int maxMessagingVersion;
        final InetAddressAndPort from;

        ConfirmOutboundPre40(int maxMessagingVersion, InetAddressAndPort from)
        {
            this.maxMessagingVersion = maxMessagingVersion;
            this.from = from;
        }

        public ByteBuf encode()
        {
            ByteBuffer buffer = BufferPool.get(MAX_LENGTH, BufferType.OFF_HEAP);
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(buffer))
            {
                out.writeInt(maxMessagingVersion);
                CompactEndpointSerializationHelper.instance.serialize(from, out, maxMessagingVersion);
                buffer.flip();
                return BufferPoolAllocator.wrapUnshared(buffer);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @SuppressWarnings("resource")
        static ConfirmOutboundPre40 maybeDecode(ByteBuf in)
        {
            in.markReaderIndex();
            DataInputPlus input = new ByteBufDataInputPlus(in);
            try
            {
                int version = input.readInt();
                InetAddressAndPort address = CompactEndpointSerializationHelper.instance.deserialize(input, version);
                return new ConfirmOutboundPre40(version, address);
            }
            catch (EOFException e)
            {
                // makes the assumption we didn't have enough bytes to deserialize an IPv6 address,
                // as we only check the MIN_LENGTH of the buf.
                in.resetReaderIndex();
                return null;
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @VisibleForTesting
        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof ConfirmOutboundPre40))
                return false;

            ConfirmOutboundPre40 that = (ConfirmOutboundPre40) other;
            return this.maxMessagingVersion == that.maxMessagingVersion
                   && Objects.equals(this.from, that.from);
        }

        @Override
        public String toString()
        {
            return String.format("Third(maxMessagingVersion: %d; address: %s)", maxMessagingVersion, from);
        }
    }
}
