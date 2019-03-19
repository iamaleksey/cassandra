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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.tracing.Tracing.TraceType;
import org.apache.cassandra.utils.ApproximateTime;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.net.MessagingService.ONE_BYTE;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.instance;

/*
 * * @param <T> The type of the message payload.
 */
public class Message<T>
{
    // this is a valid ID for pre40 nodes
    private static final long NO_ID = 0L;

    private static final AtomicInteger nextId = new AtomicInteger(0);

    /**
     * The amount of prefix data, in bytes, before the serialized message.
     */
    private static final int PRE_40_MESSAGE_PREFIX_SIZE = 12;

    /**
     * we preface every message with this number so the recipient can validate the sender is sane
     */
    public static final int PROTOCOL_MAGIC = 0xCA552DFA;

    public static final Serializer serializer = new Serializer();

    public final InetAddressAndPort from;

    public final long id;
    public final long createdAtNanos;
    public final long expiresAtNanos;
    public final Verb verb;
    public final T payload;

    private final int flags;
    private final Map<ParameterType, Object> parameters;

    private Message(InetAddressAndPort from,
                    T payload,
                    int flags,
                    Map<ParameterType, Object> parameters,
                    Verb verb,
                    long createdAtNanos,
                    long expiresAtNanos,
                    long id)
    {
        this.from = from;
        this.payload = payload;
        this.flags = flags;
        this.parameters = parameters;
        this.verb = verb;
        this.createdAtNanos = createdAtNanos;
        this.expiresAtNanos = expiresAtNanos;
        this.id = id;
    }

    public static class Builder<T>
    {
        private Verb verb;
        private InetAddressAndPort from;
        private T payload;
        private int flags = noFlags();
        private final Map<ParameterType, Object> parameters = new EnumMap<>(ParameterType.class);
        private long createdAtNanos;
        private long expiresAtNanos;
        private long id;

        private Builder()
        {
        }

        public Builder<T> from(InetAddressAndPort from)
        {
            this.from = from;
            return this;
        }

        public Builder<T> withPayload(T payload)
        {
            this.payload = payload;
            return this;
        }

        public Builder<T> withFlag(MessageFlag flag)
        {
            flags = addFlag(flags, flag);
            return this;
        }

        public Builder<T> withParameter(ParameterType type, Object value)
        {
            parameters.put(type, value);
            return this;
        }

        public Builder<T> withParameters(Map<ParameterType, Object> parameters)
        {
            this.parameters.putAll(parameters);
            return this;
        }

        public Builder<T> ofVerb(Verb verb)
        {
            this.verb = verb;
            if (expiresAtNanos == 0 && verb != null && createdAtNanos != 0)
                expiresAtNanos = verb.expiresAtNanos(createdAtNanos);
            if (!this.verb.isResponse() && from == null) // default to sending from self if we're a request verb
                from = FBUtilities.getBroadcastAddressAndPort();
            return this;
        }

        public Builder<T> withCreatedAt(long createdAtNanos)
        {
            this.createdAtNanos = createdAtNanos;
            if (expiresAtNanos == 0 && verb != null)
                expiresAtNanos = verb.expiresAtNanos(createdAtNanos);
            return this;
        }

        public Builder<T> withExpiresAt(long expiresAtNanos)
        {
            this.expiresAtNanos = expiresAtNanos;
            return this;
        }

        public Builder<T> withId(long id)
        {
            this.id = id;
            return this;
        }

        public Message<T> build()
        {
            if (verb == null)
                throw new IllegalArgumentException();
            if (from == null)
                throw new IllegalArgumentException();
            if (payload == null)
                throw new IllegalArgumentException();

            return buildUnsafe();
        }

        public Message<T> buildUnsafe()
        {
            return new Message<>(from, payload, flags, parameters, verb, createdAtNanos, expiresAtNanos, id);
        }
    }

    public static <T> Message<T> out(Verb verb, T payload)
    {
        return outWithParameter(verb, payload, null, null);
    }

    public static <T> Message<T> outWithParameter(Verb verb, T payload, ParameterType parameterType, Object parameterValue)
    {
        assert !verb.isResponse();
        return outWithParameter(0, verb, 0, payload, parameterType, parameterValue);
    }

    static <T> Message<T> outWithParameter(long id, Verb verb, long expiresAtNanos, T payload, ParameterType parameterType, Object parameterValue)
    {
        if (payload == null)
            throw new IllegalArgumentException();

        InetAddressAndPort from = FBUtilities.getBroadcastAddressAndPort();
        long createdAtNanos = ApproximateTime.nanoTime();
        if (expiresAtNanos == 0)
            expiresAtNanos = verb.expiresAtNanos(createdAtNanos);

        return new Message<>(from, payload, noFlags(), buildParameters(parameterType, parameterValue), verb, ApproximateTime.nanoTime(), expiresAtNanos, id);
    }

    public static <T> Message<T> internalResponse(Verb verb, T payload)
    {
        assert verb.isResponse();
        return outWithParameter(0, verb, 0, payload, null, null);
    }

    public <T> Message<T> responseWith(T payload)
    {
        return outWithParameter(id, verb.responseVerb, expiresAtNanos, payload, null, null);
    }

    public Message<NoPayload> emptyResponse()
    {
        return responseWith(NoPayload.noPayload);
    }

    public Message<RequestFailureReason> failureResponse(RequestFailureReason reason)
    {
        return outWithParameter(id, Verb.FAILURE_RSP, expiresAtNanos, reason, null, null);
    }

    public static <T> Builder<T> builder(Message<T> message)
    {
        return new Builder<T>().from(message.from)
                               .withPayload(message.payload)
                               .ofVerb(message.verb)
                               .withId(message.id)
                               .withExpiresAt(message.expiresAtNanos)
                               .withCreatedAt(message.createdAtNanos)
                               .withParameters(message.parameters);
    }

    public static <T> Builder<T> builder(Verb verb, T payload)
    {
        return new Builder<T>().ofVerb(verb)
                               .withCreatedAt(ApproximateTime.nanoTime())
                               .withPayload(payload);
    }

    private static Map<ParameterType, Object> buildParameters(ParameterType type, Object value)
    {
        Map<ParameterType, Object> parameters = Collections.emptyMap();
        if (Tracing.isTracing())
            parameters = Tracing.instance.addTraceHeaders(new EnumMap<>(ParameterType.class));

        if (type != null)
        {
            if (parameters.isEmpty())
                parameters = Collections.singletonMap(type, value);
            else
                parameters.put(type, value);
        }

        return parameters;
    }

    private static Map<ParameterType, Object> addParameter(Map<ParameterType, Object> parameters, ParameterType type, Object value)
    {
        if (type == null)
            return parameters;

        if (parameters.isEmpty())
            return Collections.singletonMap(type, value);

        Map<ParameterType, Object> result = new EnumMap<>(parameters);
        result.put(type, value);
        return result;
    }

    public Message<T> withFlag(MessageFlag flag)
    {
        return new Message<>(from, payload, addFlag(flags, flag), parameters, verb, createdAtNanos, expiresAtNanos, id);
    }

    public Message<T> withParameter(ParameterType type, Object value)
    {
        return new Message<>(from, payload, flags, addParameter(parameters, type, value), verb, createdAtNanos, expiresAtNanos, id);
    }

    static long nextId()
    {
        long id;
        do
        {
            id = nextId.incrementAndGet();
        } while (id == NO_ID);
        return id;
    }

    public Message<T> withId(long id)
    {
        return new Message<>(from, payload, flags, parameters, verb, createdAtNanos, expiresAtNanos, id);
    }

    public Message<T> withIdAndFlag(long id, MessageFlag flag)
    {
        return new Message<>(from, payload, addFlag(flags, flag), parameters, verb, createdAtNanos, expiresAtNanos, id);
    }

    /**
     * Since how long (in milliseconds) the message has lived.
     */
    public long elapsedSinceCreated(TimeUnit units)
    {
        return units.convert(ApproximateTime.nanoTime() - createdAtNanos, NANOSECONDS);
    }

    public long creationTimeMillis()
    {
        return ApproximateTime.toCurrentTimeMillis(createdAtNanos);
    }

    /**
     * Whether the message has crossed the node boundary, that is whether it originated from another node.
     *
     */
    public boolean isCrossNode()
    {
        return !from.equals(FBUtilities.getBroadcastAddressAndPort());
    }

    boolean isFailureResponse()
    {
        return verb == Verb.FAILURE_RSP;
    }

    /*
     * Flags
     */

    boolean callBackOnFailure()
    {
        return containsFlag(flags, MessageFlag.CALL_BACK_ON_FAILURE);
    }

    public boolean trackRepairedData()
    {
        return containsFlag(flags, MessageFlag.TRACK_REPAIRED_DATA);
    }

    private static int noFlags()
    {
        return 0;
    }

    private static boolean containsFlag(int flags, MessageFlag flag)
    {
        return (flags & (1 << flag.ordinal())) != 0;
    }

    private static int addFlag(int flags, MessageFlag flag)
    {
        return flags | (1 << flag.ordinal());
    }

    private static void forEachFlag(int flags, Consumer<MessageFlag> consumer)
    {
        for (MessageFlag flag : MessageFlag.ALL_VALUES)
            if (containsFlag(flags, flag))
                consumer.accept(flag);
    }

    /*
     * Parameters
     */

    @Nullable
    public ForwardToContainer forwardTo()
    {
        return (ForwardToContainer) parameters.get(ParameterType.FORWARD_TO);
    }

    @Nullable
    public InetAddressAndPort forwardedFrom()
    {
        return (InetAddressAndPort) parameters.get(ParameterType.FORWARDED_FROM);
    }

    @Nullable
    public UUID traceSession()
    {
        return (UUID) parameters.get(ParameterType.TRACE_SESSION);
    }

    @Nullable
    public TraceType traceType()
    {
        return (TraceType) parameters.getOrDefault(ParameterType.TRACE_TYPE, TraceType.QUERY);
    }

    public long getSlowQueryTimeout(TimeUnit units)
    {
        return DatabaseDescriptor.getSlowQueryTimeout(units);
    }

    @SuppressWarnings("unchecked")
    public void process() throws IOException
    {
        verb.handler().doVerb((Message) this);
    }

    public String toString()
    {
        return "(from:" + from + ", type:" + verb.stage + " verb:" + verb + ')';
    }

    public static final class InvalidLegacyProtocolMagic extends IOException
    {
        public final int read;
        private InvalidLegacyProtocolMagic(int read)
        {
            super(String.format("Read %d, Expected %d", read, PROTOCOL_MAGIC));
            this.read = read;
        }
    }

    public static void validateLegacyProtocolMagic(int magic) throws InvalidLegacyProtocolMagic
    {
        if (magic != PROTOCOL_MAGIC)
            throw new InvalidLegacyProtocolMagic(magic);
    }

    /**
     * Each message contains a header with several fixed fields, an optional key-value parameters section, and then
     * the message payload itself. Below is a visualization of the layout.
     *
     *  The parameters are prefixed by the count of key-value pairs; this value is encoded as unsigned vint.
     *  An individual parameter has an unsvint id (more specifically, a {@link ParameterType}), and a byte array value.
     *  The parameter value is prefixed with it's length, encoded as an unsigned vint, followed by by the value's bytes.
     *
     * Legacy Notes (see {@link Serializer#serialize(Message, DataOutputPlus, int)} for complete details):
     * - pre 4.0, the IP address was sent along in the header, before the verb. The IP address may be either IPv4 (4 bytes) or IPv6 (16 bytes)
     * - pre-4.0, the verb was encoded as a 4-byte integer; in 4.0 and up it is an unsigned vint
     * - pre-4.0, the payloadSize was encoded as a 4-byte integer; in 4.0 and up it is an unsigned vint
     * - pre-4.0, the count of a parameter values was encoded as a 4-byte integer; in 4.0 and up it is an unsigned vint
     * - pre-4.0, parameter names were encoded as strings; in 4.0 they are encoded as enum id vints
     * - pre-4.0, expiry time wasn't encoded at all; in 4.0 it's an unsigned vint
     * - pre-4.0, message id was an int; in 4.0 and up it's an unsigned vint
     * - pre-4.0, messages included PROTOCOL MAGIC BYTES; post-4.0, we rely on frame CRCs instead
     * - pre-4.0, messages would serialize boolean params as dummy ONE_BYTEs; post-4.0 we have a dedicated 'flags' vint
     *
     * <pre>
     * {@code
     *            1 1 1 1 1 2 2 2 2 2 3
     *  0 2 4 6 8 0 2 4 6 8 0 2 4 6 8 0
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Message ID (vint)             |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Creation timestamp (int)      |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Expiry (vint)                 |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Verb (vint)                   |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Flags (vint)                  |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Param count (vint)            |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                               /
     * /           Params              /
     * /                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * | Payload size (vint)           |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |                               /
     * /           Payload             /
     * /                               |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * }
     * </pre>
     */
    public static final class Serializer
    {
        private static final int CREATION_TIME_SIZE = 4;

        private Serializer()
        {
        }

        public <T> void serialize(Message<T> message, DataOutputPlus out, int version) throws IOException
        {
            if (version >= VERSION_40)
                serializePost40(message, out, version);
            else
                serializePre40(message, out, version);
        }

        public <T> Message<T> deserialize(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            return version >= VERSION_40 ? deserializePost40(in, peer, version) : deserializePre40(in, peer, version);
        }

        private <T> int serializedSize(Message<T> message, int version)
        {
            return version >= VERSION_40 ? serializedSizePost40(message, version) : serializedSizePre40(message, version);
        }

        /**
         * Size of the next message in the stream. Returns -1 if there aren't sufficient bytes read yet to determine size.
         */
        public int messageSize(ByteBuffer buf, int index, int limit, int version) throws InvalidLegacyProtocolMagic
        {
            return version >= VERSION_40 ? messageSizePost40(buf, index, limit) : messageSizePre40(buf, index, limit);
        }

        public long getCreatedAtNanos(ByteBuffer buf, InetAddressAndPort peer, int version)
        {
            return version >= VERSION_40 ? getCreatedAtNanosPost40(buf, peer) : getCreatedAtNanosPre40(buf, peer);
        }

        public long getExpiresAtNanos(ByteBuffer buf, long createdAtNanos, int version)
        {
            return version >= VERSION_40 ? getExpiresAtNanosPost40(buf, createdAtNanos) : getExpiresAtNanosPre40(buf, createdAtNanos);
        }

        public Verb getVerb(ByteBuffer buf, int version)
        {
            return version >= VERSION_40 ? getVerbPost40(buf) : getVerbPre40(buf);
        }

        /*
         * 4.0 ser/deser
         */

        private <T> void serializePost40(Message<T> message, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(message.id);

            // int cast cuts off the high-order half of the timestamp, which we can assume remains
            // the same between now and when the recipient reconstructs it.
            out.writeInt((int) ApproximateTime.toCurrentTimeMillis(message.createdAtNanos));
            out.writeUnsignedVInt(NANOSECONDS.toMillis(message.expiresAtNanos - message.createdAtNanos));
            out.writeUnsignedVInt(message.verb.id);

            out.writeUnsignedVInt(message.flags);
            serializeParams(message.parameters, out, version);

            int payloadSize = message.payloadSize(version);
            out.writeUnsignedVInt(payloadSize);
            message.verb.serializer().serialize(message.payload, out, version);
        }

        private <T> Message<T> deserializePost40(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            long id = in.readUnsignedVInt();

            long creationTimeNanos = calculateCreationTimeNanos(peer, in.readInt(), ApproximateTime.currentTimeMillis());
            long expiresAtNanos = creationTimeNanos + TimeUnit.MILLISECONDS.toNanos(in.readUnsignedVInt());
            Verb verb = Verb.fromId(Ints.checkedCast(in.readUnsignedVInt()));

            int flags = Ints.checkedCast(in.readUnsignedVInt());
            Map<ParameterType, Object> parameters = deserializeParams(in, version);

            VIntCoding.readUnsignedVInt(in); // payload size, not used here
            T payload = (T) verb.serializer().deserialize(in, version);

            return new Message<>(peer, payload, flags, parameters, verb, creationTimeNanos, expiresAtNanos, id);
        }

        private <T> int serializedSizePost40(Message<T> message, int version)
        {
            long size = 0;

            size += TypeSizes.sizeofUnsignedVInt(message.id);
            size += CREATION_TIME_SIZE;
            size += TypeSizes.sizeofUnsignedVInt(NANOSECONDS.toMillis(message.expiresAtNanos - message.createdAtNanos));
            size += TypeSizes.sizeofUnsignedVInt(message.verb.id);

            size += TypeSizes.sizeofUnsignedVInt(message.flags);
            size += serializedParamsSize(message.parameters, version);

            int payloadSize = message.payloadSize(version);
            size += TypeSizes.sizeofUnsignedVInt(payloadSize);
            size += payloadSize;

            return Ints.checkedCast(size);
        }

        private int messageSizePost40(ByteBuffer buf, int index, int limit)
        {
            int idSize = VIntCoding.computeUnsignedVIntSize(buf, index);
            if (idSize < 0)
                return -1; // not enough bytes to read id
            index += idSize;

            index += CREATION_TIME_SIZE;
            if (index > limit)
                return -1;

            int expirationSize = VIntCoding.computeUnsignedVIntSize(buf, index);
            if (expirationSize < 0)
                return -1;
            index += expirationSize;

            int verbIdSize = VIntCoding.computeUnsignedVIntSize(buf, index);
            if (verbIdSize < 0)
                return -1;
            index += verbIdSize;

            int flagsSize = VIntCoding.computeUnsignedVIntSize(buf, index);
            if (flagsSize < 0)
                return -1;
            index += flagsSize;

            int paramsSize = serializedParamsSizePost40(buf, index);
            if (paramsSize < 0)
                return -1;
            index += paramsSize;

            long payloadSize = VIntCoding.getUnsignedVInt(buf, index);
            if (payloadSize < 0)
                return -1;
            index += VIntCoding.computeUnsignedVIntSize(payloadSize) + payloadSize;

            return index - buf.position();
        }

        private long getCreatedAtNanosPost40(ByteBuffer buf, InetAddressAndPort peer)
        {
            int index = buf.position();
            index += VIntCoding.computeUnsignedVIntSize(buf, index); // id
            return calculateCreationTimeNanos(peer, buf.getInt(index), ApproximateTime.currentTimeMillis());
        }

        private long getExpiresAtNanosPost40(ByteBuffer buf, long createdAtNanos)
        {
            int index = buf.position();
            index += VIntCoding.computeUnsignedVIntSize(buf, index); // id
            index += CREATION_TIME_SIZE;
            return createdAtNanos + TimeUnit.MILLISECONDS.toNanos(VIntCoding.getUnsignedVInt(buf, index));
        }

        private Verb getVerbPost40(ByteBuffer buf)
        {
            int index = buf.position();
            index += VIntCoding.computeUnsignedVIntSize(buf, index); // id
            index += CREATION_TIME_SIZE;
            index += VIntCoding.computeUnsignedVIntSize(buf, index); // expiration
            return Verb.fromId(Ints.checkedCast(VIntCoding.getUnsignedVInt(buf, index)));
        }

        /*
         * legacy ser/deser
         */

        private <T> void serializePre40(Message<T> message, DataOutputPlus out, int version) throws IOException
        {
            if (message.isFailureResponse())
                message = (Message<T>) toPre40FailureResponse((Message<RequestFailureReason>) message);

            out.writeInt(PROTOCOL_MAGIC);
            out.writeInt(Ints.checkedCast(message.id));
            // int cast cuts off the high-order half of the timestamp, which we can assume remains
            // the same between now and when the recipient reconstructs it.
            out.writeInt((int) ApproximateTime.toCurrentTimeMillis(message.createdAtNanos));
            CompactEndpointSerializationHelper.instance.serialize(message.from, out, version);
            out.writeInt(message.verb.toPre40Verb().id);

            serializeParams(addFlagsToLegacyParams(message.parameters, message.flags), out, version);

            if (message.payload != null && message.payload != NoPayload.noPayload)
            {
                int payloadSize = message.payloadSize(version);
                out.writeInt(payloadSize);
                message.verb.serializer().serialize(message.payload, out, version);
            }
            else
            {
                out.writeInt(0);
            }
        }

        private <T> Message<T> deserializePre40(DataInputPlus in, InetAddressAndPort peer, int version) throws IOException
        {
            validateLegacyProtocolMagic(in.readInt());
            int messageId = in.readInt();
            long creationTimeNanos = calculateCreationTimeNanos(peer, in.readInt(), ApproximateTime.currentTimeMillis());
            InetAddressAndPort from = CompactEndpointSerializationHelper.instance.deserialize(in, version);
            Verb verb = Verb.fromId(in.readInt());

            Map<ParameterType, Object> parameters = deserializeParams(in, version);
            int flags = removeFlagsFromLegacyParams(parameters);

            IVersionedAsymmetricSerializer<?, T> payloadSerializer = verb.serializer();
            if (null == payloadSerializer)
            {
                CallbackInfo callback = instance().callbacks.get(messageId);
                if (null != callback)
                    payloadSerializer = callback.verb.responseVerb.serializer();
            }
            int payloadSize = in.readInt();
            T payload = deserializePayloadPre40(in, version, payloadSerializer, payloadSize);

            Message<T> message = new Message<>(from, payload, flags, parameters, verb, creationTimeNanos, verb.expiresAtNanos(creationTimeNanos), messageId);
            return message.parameters.containsKey(ParameterType.FAILURE_RESPONSE)
                 ? (Message<T>) toPost40FailureResponse(message)
                 : message;
        }

        private <T> T deserializePayloadPre40(DataInputPlus in, int version, IVersionedAsymmetricSerializer<?, T> serializer, int payloadSize) throws IOException
        {
            if (payloadSize == 0 || serializer == null)
            {
                // if there's no deserializer for the verb, skip the payload bytes to leave
                // the stream in a clean state (for the next message)
                in.skipBytesFully(payloadSize);
                return null;
            }

            return serializer.deserialize(in, version);
        }

        private <T> int serializedSizePre40(Message<T> message, int version)
        {
            if (message.isFailureResponse())
                message = (Message<T>) toPre40FailureResponse((Message<RequestFailureReason>) message);

            long size = 0;

            size += PRE_40_MESSAGE_PREFIX_SIZE;
            size += CompactEndpointSerializationHelper.instance.serializedSize(message.from, version);
            size += TypeSizes.sizeof(message.verb.id);

            size += serializedParamsSize(addFlagsToLegacyParams(message.parameters, message.flags), version);

            int payloadSize = message.payloadSize(version);
            size += TypeSizes.sizeof(payloadSize);
            size += payloadSize;

            return Ints.checkedCast(size);
        }

        private int messageSizePre40(ByteBuffer buf, int index, int limit) throws InvalidLegacyProtocolMagic
        {
            // protocol magic
            index += 4;
            if (index > limit)
                return -1;
            validateLegacyProtocolMagic(buf.getInt(index - 4));

            // rest of prefix
            index += PRE_40_MESSAGE_PREFIX_SIZE - 4;
            // ip address
            index += 1;
            if (index > limit)
                return -1;
            index += buf.get(index - 1);
            // verb
            index += 4;
            if (index > limit)
                return -1;

            int paramsSize = serializedParamsSizePre40(buf, index);
            if (paramsSize < 0)
                return -1;
            index += paramsSize;

            // payload
            index += 4;

            if (index > limit)
                return -1;
            index += buf.getInt(index - 4);

            return index - buf.position();
        }

        private long getCreatedAtNanosPre40(ByteBuffer buf, InetAddressAndPort peer)
        {
            int index = buf.position();
            index += 4; // protocol magic
            index += 4; // message id
            return calculateCreationTimeNanos(peer, buf.getInt(index), ApproximateTime.currentTimeMillis());
        }

        private long getExpiresAtNanosPre40(ByteBuffer buf, long createdAtNanos)
        {
            return getVerbPre40(buf).expiresAtNanos(createdAtNanos);
        }

        private Verb getVerbPre40(ByteBuffer buf)
        {
            int index = buf.position();
            index += 4;                  // protocol magic
            index += 4;                  // id
            index += 4;                  // creation time
            index += 1 + buf.get(index); // from
            return Verb.fromId(buf.getInt(index));
        }

        private Message<?> toPre40FailureResponse(Message<RequestFailureReason> post40)
        {
            Map<ParameterType, Object> params = new EnumMap<>(ParameterType.class);
            params.putAll(post40.parameters);

            params.put(ParameterType.FAILURE_RESPONSE, ONE_BYTE);
            params.put(ParameterType.FAILURE_REASON, post40.payload);

            return new Message<>(post40.from, NoPayload.noPayload, noFlags(), params, post40.verb.toPre40Verb(), post40.createdAtNanos, post40.expiresAtNanos, post40.id);
        }

        private Message<RequestFailureReason> toPost40FailureResponse(Message<?> pre40)
        {
            Map<ParameterType, Object> params = new EnumMap<>(ParameterType.class);
            params.putAll(pre40.parameters);

            params.remove(ParameterType.FAILURE_RESPONSE);

            RequestFailureReason reason = (RequestFailureReason) params.remove(ParameterType.FAILURE_REASON);
            if (null == reason)
                reason = RequestFailureReason.UNKNOWN;

            return new Message<>(pre40.from, reason, pre40.flags, params, Verb.FAILURE_RSP, pre40.createdAtNanos, pre40.expiresAtNanos, pre40.id);
        }

        /*
         * param ser/deser
         */

        private Map<ParameterType, Object> addFlagsToLegacyParams(Map<ParameterType, Object> params, int flags)
        {
            if (flags == 0)
                return params;

            Map<ParameterType, Object> extended = new EnumMap<>(ParameterType.class);
            extended.putAll(params);
            forEachFlag(flags, f ->
            {
                if (f.legacyParam != null)
                    extended.put(f.legacyParam, f.legacyValue);
            });
            return extended;
        }

        private int removeFlagsFromLegacyParams(Map<ParameterType, Object> params)
        {
            if (params.isEmpty())
                return 0;

            int flags = 0;
            Iterator<ParameterType> iter = params.keySet().iterator();
            while (iter.hasNext())
            {
                ParameterType type = iter.next();
                if (type.flagEquivalent != null)
                {
                    flags = addFlag(flags, type.flagEquivalent);
                    iter.remove();
                }
            }
            return flags;
        }

        private void serializeParams(Map<ParameterType, Object> params, DataOutputPlus out, int version) throws IOException
        {
            if (version >= VERSION_40)
                out.writeUnsignedVInt(params.size());
            else
                out.writeInt(params.size());

            for (Map.Entry<ParameterType, Object> kv : params.entrySet())
            {
                ParameterType type = kv.getKey();
                if (version >= VERSION_40)
                    out.writeUnsignedVInt(type.id);
                else
                    out.writeUTF(type.legacyAlias);

                IVersionedSerializer serializer = type.serializer;
                Object value = kv.getValue();

                int length = Ints.checkedCast(serializer.serializedSize(value, version));
                if (version >= VERSION_40)
                    out.writeUnsignedVInt(length);
                else
                    out.writeInt(length);

                serializer.serialize(value, out, version);
            }
        }

        private Map<ParameterType, Object> deserializeParams(DataInputPlus in, int version) throws IOException
        {
            int count = version >= VERSION_40
                ? Ints.checkedCast(in.readUnsignedVInt())
                : in.readInt();

            if (count == 0)
                return Collections.emptyMap();

            Map<ParameterType, Object> params = new EnumMap<>(ParameterType.class);

            for (int i = 0; i < count; i++)
            {
                ParameterType type = version >= VERSION_40
                    ? ParameterType.lookUpById(Ints.checkedCast(in.readUnsignedVInt()))
                    : ParameterType.lookUpByAlias(in.readUTF());

                int length = version >= VERSION_40
                    ? Ints.checkedCast(in.readUnsignedVInt())
                    : in.readInt();

                if (null != type)
                    params.put(type, type.serializer.deserialize(in, version));
                else
                    in.skipBytes(length); // forward compatibiliy with minor version changes
            }

            return params;
        }

        private long serializedParamsSize(Map<ParameterType, Object> params, int version)
        {
            long size = version >= VERSION_40
                      ? VIntCoding.computeUnsignedVIntSize(params.size())
                      : TypeSizes.sizeof(params.size());

            for (Map.Entry<ParameterType, Object> kv : params.entrySet())
            {
                ParameterType type = kv.getKey();
                Object value = kv.getValue();

                long valueLength = type.serializer.serializedSize(value, version);

                if (version >= VERSION_40)
                    size += TypeSizes.sizeofUnsignedVInt(type.id) + TypeSizes.sizeofUnsignedVInt(valueLength);
                else
                    size += TypeSizes.sizeof(type.legacyAlias) + 4;

                size += valueLength;
            }

            return size;
        }

        private int serializedParamsSizePost40(ByteBuffer buf, int readerIndex)
        {
            int index = readerIndex;

            long paramsCount = VIntCoding.getUnsignedVInt(buf, index);
            if (paramsCount < 0)
                return -1;
            index += VIntCoding.computeUnsignedVIntSize(paramsCount);

            for (int i = 0; i < paramsCount; i++)
            {
                long type = VIntCoding.getUnsignedVInt(buf, index);
                if (type < 0)
                    return -1;
                index += VIntCoding.computeUnsignedVIntSize(type);

                long length = VIntCoding.getUnsignedVInt(buf, index);
                if (length < 0)
                    return -1;
                index += VIntCoding.computeUnsignedVIntSize(length) + length;
            }

            return index - readerIndex;
        }

        private int serializedParamsSizePre40(ByteBuffer buf, int readerIndex)
        {
            int index = readerIndex;
            final int limit = buf.limit();

            index += 4;
            if (index > limit)
                return -1;
            int paramsCount = buf.getInt(index - 4);

            for (int i = 0; i < paramsCount; i++)
            {
                // try to read length and skip to the end of the parameter name
                index += 2;

                if (index > limit)
                    return -1;
                index += buf.getShort(index - 2);
                // try to read length and skip to the end of the parameter value
                index += 4;
                if (index > limit)
                index += buf.getInt(index - 4);
            }

            return index - readerIndex;
        }

        /*
         * helpers
         */

        private <T> int payloadSize(Message<T> message, int version)
        {
            long payloadSize = message.payload != null && message.payload != NoPayload.noPayload
                             ? message.verb.serializer().serializedSize(message.payload, version)
                             : 0;
            return Ints.checkedCast(payloadSize);
        }

        @VisibleForTesting
        long calculateCreationTimeNanos(InetAddressAndPort from, int messageTimestampMillis, long currentTimeMillis)
        {
            // Reconstruct the message construction time sent by the remote host (we sent only the lower 4 bytes, assuming the
            // higher 4 bytes wouldn't change between the sender and receiver)
            long currentHighBits = currentTimeMillis & 0xFFFFFFFF00000000L;
            long reconstructedLowBits = messageTimestampMillis & 0xFFFFFFFFL;
            long sentConstructionTime = currentHighBits | reconstructedLowBits;

            // if we wrap around our timer across the message delivery window, we will give ourselves a creationTime
            // roughly two months in the future; so, if the current time and previous timestamp suggest wrap around
            // (i.e. are each within one minute, on the correct side, of the wrap around point)
            // then assume this is what has happened, and use the prior high bits
            long elapsed = currentTimeMillis - sentConstructionTime;
            if (elapsed <= 0)
            {
                long currentLowBits = currentTimeMillis & 0xFFFFFFFFL;
                long reconstructedTimeToWrapAround = 0xFFFFFFFFL - reconstructedLowBits;
                if (currentLowBits < MINUTES.toNanos(1L) && reconstructedTimeToWrapAround < MINUTES.toNanos(1L))
                    sentConstructionTime -= 0x0000000100000000L;
            }

            // Because nodes may not have their clock perfectly in sync, it's actually possible the sentConstructionTime is
            // later than the currentTime (the received time). If that's the case, as we definitively know there is a lack
            // of proper synchronziation of the clock, we ignore sentConstructionTime. We also ignore that
            // sentConstructionTime if we're told to.
            if (elapsed > 0)
                instance().metrics.addTimeTaken(from, elapsed, MILLISECONDS);

            boolean useSentTime = DatabaseDescriptor.hasCrossNodeTimeout() && elapsed > 0;
            return ApproximateTime.toNanoTime(useSentTime ? sentConstructionTime : currentTimeMillis);
        }
    }

    private int serializedSize30;
    private int serializedSize3014;
    private int serializedSize40;

    public int serializedSize(int version)
    {
        switch (version)
        {
            case VERSION_30:
                if (serializedSize30 == 0)
                    serializedSize30 = serializer.serializedSize(this, VERSION_30);
                return serializedSize30;
            case VERSION_3014:
                if (serializedSize3014 == 0)
                    serializedSize3014 = serializer.serializedSize(this, VERSION_3014);
                return serializedSize3014;
            case VERSION_40:
                if (serializedSize40 == 0)
                    serializedSize40 = serializer.serializedSize(this, VERSION_40);
                return serializedSize40;
            default:
                throw new IllegalStateException();
        }
    }

    private int payloadSize30   = -1;
    private int payloadSize3014 = -1;
    private int payloadSize40   = -1;

    private int payloadSize(int version)
    {
        switch (version)
        {
            case VERSION_30:
                if (payloadSize30 < 0)
                    payloadSize30 = serializer.payloadSize(this, VERSION_30);
                return payloadSize30;
            case VERSION_3014:
                if (payloadSize3014 < 0)
                    payloadSize3014 = serializer.payloadSize(this, VERSION_3014);
                return payloadSize3014;
            case VERSION_40:
                if (payloadSize40 < 0)
                    payloadSize40 = serializer.payloadSize(this, VERSION_40);
                return payloadSize40;
            default:
                throw new IllegalStateException();
        }
    }

    // WARNING: this is inaccurate for messages from pre40 nodes, who can use 0 as an id (but will do so rarely)
    @VisibleForTesting
    public boolean hasId()
    {
        return id != NO_ID;
    }

}
