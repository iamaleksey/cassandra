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

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.DummyByteVersionedSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDSerializer;

import static java.lang.Math.max;

/**
 * Type names and serializers for various parameters that
 */
public enum ParamType
{
    FORWARD_TO          (0, "FORWARD_TO",    ForwardToSerializer.instance),
    FORWARDED_FROM      (1, "FORWARD_FROM",  CompactEndpointSerializationHelper.instance),

    @Deprecated
    FAILURE_RESPONSE    (2, "FAIL",          DummyByteVersionedSerializer.instance),
    @Deprecated
    FAILURE_REASON      (3, "FAIL_REASON",   RequestFailureReason.serializer),
    @Deprecated
    FAILURE_CALLBACK    (4, "CAL_BAC",       DummyByteVersionedSerializer.instance),

    TRACE_SESSION       (5, "TraceSession",  UUIDSerializer.serializer),
    TRACE_TYPE          (6, "TraceType",     Tracing.traceTypeSerializer),

    @Deprecated
    TRACK_REPAIRED_DATA (7, "TrackRepaired", DummyByteVersionedSerializer.instance);

    public final int id;
    public final String legacyAlias;
    public final IVersionedSerializer serializer;

    private static final ParamType[] idToTypeMap;
    private static final Map<String, ParamType> aliasToTypeMap;

    static
    {
        ParamType[] types = values();

        int max = -1;
        for (ParamType t : types)
            max = max(t.id, max);

        ParamType[] idMap = new ParamType[max + 1];
        Map<String, ParamType> aliasMap = new HashMap<>();

        for (ParamType type : types)
        {
            if (idMap[type.id] != null)
                throw new RuntimeException("Two ParamType-s that map to the same id: " + type.id);
            idMap[type.id] = type;

            if (aliasMap.put(type.legacyAlias, type) != null)
                throw new RuntimeException("Two ParamType-s that map to the same legacy alias: " + type.legacyAlias);
        }

        idToTypeMap = idMap;
        aliasToTypeMap = aliasMap;
    }

    ParamType(int id, String legacyAlias, IVersionedSerializer serializer)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative");

        this.id = id;
        this.legacyAlias = legacyAlias;
        this.serializer = serializer;
    }

    @Nullable
    public static ParamType lookUpById(int id)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative (got " + id + ')');

        return id < idToTypeMap.length ? idToTypeMap[id] : null;
    }

    @Nullable
    public static ParamType lookUpByAlias(String alias)
    {
        return aliasToTypeMap.get(alias);
    }
}
