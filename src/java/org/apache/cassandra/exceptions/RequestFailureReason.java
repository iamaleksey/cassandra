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

package org.apache.cassandra.exceptions;

import java.io.IOException;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.net.MessagingService.VERSION_40;

public enum RequestFailureReason
{
    /**
     * The reason for the failure was none of the below reasons or was not recorded by the data node.
     */
    UNKNOWN                  (0),

    /**
     * The data node read too many tombstones when attempting to execute a read query (see tombstone_failure_threshold).
     */
    READ_TOO_MANY_TOMBSTONES (1),

    /**
     * Unknown table id or column encountered, or otherwise incompatible schema.
     */
    INCOMPATIBLE_SCHEMA      (2);

    public static final Serializer serializer = new Serializer();

    public final int code;
    private static final RequestFailureReason[] VALUES = values();

    RequestFailureReason(final int code)
    {
        this.code = code;
    }

    public static RequestFailureReason fromCode(final int code)
    {
        for (RequestFailureReason reasonCode : VALUES)
        {
            if (reasonCode.code == code)
                return reasonCode;
        }
        throw new IllegalArgumentException("Unknown request failure reason error code: " + code);
    }

    public static RequestFailureReason forException(Throwable t)
    {
        if (t instanceof TombstoneOverwhelmingException)
            return READ_TOO_MANY_TOMBSTONES;

        if (t instanceof UnknownColumnException || t instanceof UnknownTableException)
            return INCOMPATIBLE_SCHEMA;

        return UNKNOWN;
    }

    public static final class Serializer implements IVersionedSerializer<RequestFailureReason>
    {
        private Serializer()
        {
        }

        public void serialize(RequestFailureReason reason, DataOutputPlus out, int version) throws IOException
        {
            if (version < VERSION_40)
                out.writeShort(reason.code);
            else
                out.writeUnsignedVInt(reason.code);
        }

        public RequestFailureReason deserialize(DataInputPlus in, int version) throws IOException
        {
            return fromCode(version < VERSION_40 ? in.readUnsignedShort() : Ints.checkedCast(in.readUnsignedVInt()));
        }

        public long serializedSize(RequestFailureReason reason, int version)
        {
            return version < VERSION_40 ? 2 : VIntCoding.computeVIntSize(reason.code);
        }
    }
}
