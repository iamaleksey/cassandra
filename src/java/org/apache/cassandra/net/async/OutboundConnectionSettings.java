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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.netty.channel.WriteBufferWaterMark;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.AcceptVersions;
import org.apache.cassandra.utils.CoalescingStrategies;
import org.apache.cassandra.utils.CoalescingStrategies.CoalescingStrategy;

import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.async.OutboundConnection.Type.STREAM;
import static org.apache.cassandra.net.async.SocketFactory.encryptionLogStatement;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

/**
 * A collection of data points to be passed around for outbound connections.
 */
public class OutboundConnectionSettings
{
    private static final String INTRADC_TCP_NODELAY_PROPERTY = Config.PROPERTY_PREFIX + "otc_intradc_tcp_nodelay";
    /**
     * Enabled/disable TCP_NODELAY for intradc connections. Defaults to enabled.
     */
    private static final boolean INTRADC_TCP_NODELAY = Boolean.parseBoolean(System.getProperty(INTRADC_TCP_NODELAY_PROPERTY, "true"));

    public final IInternodeAuthenticator authenticator;
    // TODO make sure each caller is using the correct one of each of these
    public final InetAddressAndPort endpoint;
    public final InetAddressAndPort connectTo; // may be represented by a different IP address on this node's local network
    public final EncryptionOptions encryption;
    public final Boolean withCompression;
    public final Boolean withCrc;
    public final CoalescingStrategy coalescingStrategy;
    public final Integer socketSendBufferSizeInBytes;
    // TODO: document these, and perhaps derive defaults from system memory settings?
    public final Integer applicationSendQueueCapacityInBytes;
    public final Integer applicationReserveSendQueueEndpointCapacityInBytes;
    public final ResourceLimits.Limit applicationReserveSendQueueGlobalCapacityInBytes;
    public final Boolean tcpNoDelay;
    public final int flushLowWaterMark, flushHighWaterMark;
    public final Integer tcpConnectTimeoutInMS;
    public final Integer tcpUserTimeoutInMS;
    public final AcceptVersions acceptVersions;
    public final InetAddressAndPort from;
    public final SocketFactory socketFactory;

    public OutboundConnectionSettings(InetAddressAndPort endpoint)
    {
        this(endpoint, null);
    }

    public OutboundConnectionSettings(InetAddressAndPort endpoint, InetAddressAndPort preferred)
    {
        this(null, endpoint, preferred, null, null, null, null, null, null, null, null, null, 1 << 15, 1 << 16, null, null, null, null, null);
    }

    private OutboundConnectionSettings(IInternodeAuthenticator authenticator,
                                       InetAddressAndPort endpoint,
                                       InetAddressAndPort connectTo,
                                       EncryptionOptions encryption,
                                       Boolean withCompression,
                                       Boolean withCrc, CoalescingStrategy coalescingStrategy,
                                       Integer socketSendBufferSizeInBytes,
                                       Integer applicationSendQueueCapacityInBytes,
                                       Integer applicationReserveSendQueueEndpointCapacityInBytes,
                                       ResourceLimits.Limit applicationReserveSendQueueGlobalCapacityInBytes,
                                       Boolean tcpNoDelay,
                                       int flushLowWaterMark,
                                       int flushHighWaterMark,
                                       Integer tcpConnectTimeoutInMS,
                                       Integer tcpUserTimeoutInMS,
                                       AcceptVersions acceptVersions,
                                       InetAddressAndPort from,
                                       SocketFactory socketFactory)
    {
        this.authenticator = authenticator;
        this.endpoint = endpoint;
        this.connectTo = connectTo;
        this.encryption = encryption;
        this.withCompression = withCompression;
        this.withCrc = withCrc;
        this.coalescingStrategy = coalescingStrategy;
        this.socketSendBufferSizeInBytes = socketSendBufferSizeInBytes;
        this.applicationSendQueueCapacityInBytes = applicationSendQueueCapacityInBytes;
        this.applicationReserveSendQueueEndpointCapacityInBytes = applicationReserveSendQueueEndpointCapacityInBytes;
        this.applicationReserveSendQueueGlobalCapacityInBytes = applicationReserveSendQueueGlobalCapacityInBytes;
        this.tcpNoDelay = tcpNoDelay;
        this.flushLowWaterMark = flushLowWaterMark;
        this.flushHighWaterMark = flushHighWaterMark;
        this.tcpConnectTimeoutInMS = tcpConnectTimeoutInMS;
        this.tcpUserTimeoutInMS = tcpUserTimeoutInMS;
        this.acceptVersions = acceptVersions;
        this.from = from;
        this.socketFactory = socketFactory;
    }

    public boolean authenticate()
    {
        return authenticator.authenticate(endpoint.address, endpoint.port);
    }

    public boolean withEncryption()
    {
        return encryption != null;
    }

    public boolean withCompression()
    {
        return withCompression != null && withCompression;
    }

    public boolean withCrc()
    {
        return withCrc != null && withCrc;
    }

    public String toString()
    {
        return String.format("peer: (%s, %s), compression: %s, encryption: %s, coalesce: %s",
                             endpoint, connectTo, withCompression, encryptionLogStatement(encryption),
                             coalescingStrategy != null ? coalescingStrategy : CoalescingStrategies.Strategy.DISABLED);
    }

    public OutboundConnectionSettings withAuthenticator(IInternodeAuthenticator authenticator)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings toEndpoint(InetAddressAndPort endpoint)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withConnectTo(InetAddressAndPort connectTo)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withEncryption(ServerEncryptionOptions encryptionOptions)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryptionOptions, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withCompression(boolean compress)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, compress,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withCrc(boolean crc)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              crc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withCoalescingStrategy(CoalescingStrategy coalescingStrategy)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withSocketSendBufferSizeInBytes(int socketSendBufferSizeInBytes)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withApplicationSendQueueCapacityInBytes(int applicationSendQueueCapacityInBytes)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withApplicationReserveSendQueueCapacityInBytes(Integer applicationReserveSendQueueEndpointCapacityInBytes, ResourceLimits.Limit applicationReserveSendQueueGlobalCapacityInBytes)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withTcpNoDelay(boolean tcpNoDelay)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    @SuppressWarnings("unused")
    public OutboundConnectionSettings withNettyBufferBounds(WriteBufferWaterMark nettyBufferBounds)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withTcpConnectTimeoutInMS(int tcpConnectTimeoutInMS)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withTcpUserTimeoutInMS(int tcpUserTimeoutInMS)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withAcceptVersions(AcceptVersions acceptVersions)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withFrom(InetAddressAndPort from)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withSocketFactory(SocketFactory socketFactory)
    {
        return new OutboundConnectionSettings(authenticator, endpoint, connectTo, encryption, withCompression,
                                              withCrc, coalescingStrategy, socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark, tcpConnectTimeoutInMS,
                                              tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    public OutboundConnectionSettings withDefaultReserveLimits()
    {
        Integer applicationReserveSendQueueEndpointCapacityInBytes = this.applicationReserveSendQueueEndpointCapacityInBytes;
        ResourceLimits.Limit applicationReserveSendQueueGlobalCapacityInBytes = this.applicationReserveSendQueueGlobalCapacityInBytes;

        if (applicationReserveSendQueueEndpointCapacityInBytes == null)
            applicationReserveSendQueueEndpointCapacityInBytes = DatabaseDescriptor.getInternodeApplicationReserveReceiveQueueEndpointCapacityInBytes();
        if (applicationReserveSendQueueGlobalCapacityInBytes == null)
            applicationReserveSendQueueGlobalCapacityInBytes = MessagingService.instance().reserveSendQueueGlobalLimitInBytes;

        return withApplicationReserveSendQueueCapacityInBytes(applicationReserveSendQueueEndpointCapacityInBytes, applicationReserveSendQueueGlobalCapacityInBytes);
    }

    public OutboundConnectionSettings withDefaults(OutboundConnection.Type type)
    {
        return withDefaults(type, MessagingService.instance().versions.get(endpoint));
    }

    // note that connectTo is updated even if specified, in the case of pre40 messaging and using encryption (to update port)
    public OutboundConnectionSettings withDefaults(OutboundConnection.Type type, int messagingVersion)
    {
        IInternodeAuthenticator authenticator = this.authenticator;
        InetAddressAndPort endpoint = this.endpoint;
        InetAddressAndPort connectTo = this.connectTo;
        InetAddressAndPort from = this.from;
        EncryptionOptions encryptionOptions = this.encryption;
        Boolean crc = this.withCrc;
        Boolean compress = this.withCompression;
        CoalescingStrategy coalescingStrategy = this.coalescingStrategy;
        Integer socketSendBufferSizeInBytes = this.socketSendBufferSizeInBytes;
        Integer applicationSendQueueCapacityInBytes = this.applicationSendQueueCapacityInBytes;
        Integer applicationReserveSendQueueEndpointCapacityInBytes = this.applicationReserveSendQueueEndpointCapacityInBytes;
        ResourceLimits.Limit applicationReserveSendQueueGlobalCapacityInBytes = this.applicationReserveSendQueueGlobalCapacityInBytes;
        Boolean tcpNoDelay = this.tcpNoDelay;
        Integer flushLowWaterMark = this.flushLowWaterMark;
        Integer flushHighWaterMark = this.flushHighWaterMark;
        Integer tcpConnectTimeoutInMS = this.tcpConnectTimeoutInMS;
        Integer tcpUserTimeoutInMS = this.tcpUserTimeoutInMS;
        AcceptVersions acceptVersions = this.acceptVersions;
        SocketFactory socketFactory = this.socketFactory;

        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        InetAddressAndPort self = getBroadcastAddressAndPort();

        if (endpoint == null)
            throw new IllegalArgumentException();

        if (encryptionOptions == null)
            encryptionOptions = defaultEncryptionOptions(endpoint);

        // fill in defaults
        if (connectTo == null)
            connectTo = SystemKeyspace.getPreferredIP(endpoint);
        connectTo = maybeWithSecurePort(connectTo, messagingVersion, withEncryption());

        if (from == null)
            from = getBroadcastAddressAndPort();

        // by default we do not compress streaming at the pipeline level,
        // as the streams will (typically) handle compression themselves
        if (compress == null)
            compress = type != STREAM && shouldCompressConnection(snitch, self, endpoint);

        if (crc == null)
            crc = type != STREAM && !compress;

        if (crc && messagingVersion < VERSION_40)
            crc = false; // cannot use Crc framing with pre-40

        if (tcpNoDelay == null)
            tcpNoDelay = isInLocalDC(snitch, self, endpoint)
                         ? INTRADC_TCP_NODELAY : DatabaseDescriptor.getInterDCTcpNoDelay();

        if (coalescingStrategy == null)
            coalescingStrategy = defaultCoalescingStrategy(endpoint, type);

        if (authenticator == null)
            authenticator = DatabaseDescriptor.getInternodeAuthenticator();

        if (socketSendBufferSizeInBytes == null)
            socketSendBufferSizeInBytes = DatabaseDescriptor.getInternodeSocketSendBufferSizeInBytes();

        if (applicationSendQueueCapacityInBytes == null)
            applicationSendQueueCapacityInBytes = DatabaseDescriptor.getInternodeApplicationSendQueueCapacityInBytes();

        if (applicationReserveSendQueueGlobalCapacityInBytes == null)
            applicationReserveSendQueueGlobalCapacityInBytes = MessagingService.instance().reserveSendQueueGlobalLimitInBytes;

        if (applicationReserveSendQueueEndpointCapacityInBytes == null)
            applicationReserveSendQueueEndpointCapacityInBytes = DatabaseDescriptor.getInternodeApplicationReserveReceiveQueueEndpointCapacityInBytes();

        if (tcpConnectTimeoutInMS == null)
            tcpConnectTimeoutInMS = DatabaseDescriptor.getInternodeTcpConnectTimeoutInMS();

        if (tcpUserTimeoutInMS == null)
            tcpUserTimeoutInMS = DatabaseDescriptor.getInternodeTcpUserTimeoutInMS();

        if (acceptVersions == null)
            acceptVersions = type == STREAM ? MessagingService.accept_streaming : MessagingService.accept_messaging;

        if (socketFactory == null)
            socketFactory = MessagingService.instance().socketFactory;

        Preconditions.checkArgument(socketSendBufferSizeInBytes == 0 || socketSendBufferSizeInBytes >= 1 << 10, "illegal socket send buffer size: " + socketSendBufferSizeInBytes);
        Preconditions.checkArgument(applicationSendQueueCapacityInBytes >= 1 << 10, "illegal application send queue capacity: " + applicationSendQueueCapacityInBytes);
        Preconditions.checkArgument(tcpUserTimeoutInMS >= 0, "tcp user timeout must be non negative: " + tcpUserTimeoutInMS);
        Preconditions.checkArgument(tcpConnectTimeoutInMS > 0, "tcp connect timeout must be positive: " + tcpConnectTimeoutInMS);

        return new OutboundConnectionSettings(authenticator, endpoint, connectTo,
                                              encryptionOptions, compress, crc, coalescingStrategy,
                                              socketSendBufferSizeInBytes, applicationSendQueueCapacityInBytes,
                                              applicationReserveSendQueueEndpointCapacityInBytes,
                                              applicationReserveSendQueueGlobalCapacityInBytes,
                                              tcpNoDelay, flushLowWaterMark, flushHighWaterMark,
                                              tcpConnectTimeoutInMS, tcpUserTimeoutInMS, acceptVersions, from, socketFactory);
    }

    private static boolean isInLocalDC(IEndpointSnitch snitch, InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        String remoteDC = snitch.getDatacenter(remoteHost);
        String localDC = snitch.getDatacenter(localHost);
        return remoteDC != null && remoteDC.equals(localDC);
    }

    @VisibleForTesting
    static EncryptionOptions defaultEncryptionOptions(InetAddressAndPort endpoint)
    {
        ServerEncryptionOptions options = DatabaseDescriptor.getInternodeMessagingEncyptionOptions();
        return options.shouldEncrypt(endpoint) ? options : null;
    }

    @VisibleForTesting
    static boolean shouldCompressConnection(IEndpointSnitch snitch, InetAddressAndPort localHost, InetAddressAndPort remoteHost)
    {
        return (DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.all)
               || ((DatabaseDescriptor.internodeCompression() == Config.InternodeCompression.dc) && !isInLocalDC(snitch, localHost, remoteHost));
    }

    private static CoalescingStrategy defaultCoalescingStrategy(InetAddressAndPort remote, OutboundConnection.Type type)
    {
        // potentially harmful to coalesce gossip
        if (type == OutboundConnection.Type.URGENT)
            return null;
        // no point coalescing large messages
        if (type == OutboundConnection.Type.LARGE_MESSAGE)
            return null;

        String strategyName = DatabaseDescriptor.getOtcCoalescingStrategy();
        String displayName = remote.toString();
        return CoalescingStrategies.newCoalescingStrategy(strategyName,
                                                          DatabaseDescriptor.getOtcCoalescingWindow(),
                                                          OutboundConnection.logger,
                                                          displayName);

    }

    private static InetAddressAndPort maybeWithSecurePort(InetAddressAndPort address, int messagingVersion, boolean isEncrypted)
    {
        if (!isEncrypted || messagingVersion >= VERSION_40)
            return address;

        // if we don't know the version of the peer, assume it is 4.0 (or higher) as the only time is would be lower
        // (as in a 3.x version) is during a cluster upgrade (from 3.x to 4.0). In that case the outbound connection will
        // unfortunately fail - however the peer should connect to this node (at some point), and once we learn it's version, it'll be
        // in versions map. thus, when we attempt to reconnect to that node, we'll have the version and we can get the correct port.
        // we will be able to remove this logic at 5.0.
        // Also as of 4.0 we will propagate the "regular" port (which will support both SSL and non-SSL) via gossip so
        // for SSL and version 4.0 always connect to the gossiped port because if SSL is enabled it should ALWAYS
        // listen for SSL on the "regular" port.
        return address.withPort(DatabaseDescriptor.getSSLStoragePort());
    }

}
