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

import org.apache.cassandra.io.DummyByteVersionedSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.ShortVersionedSerializer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDSerializer;

import static java.lang.Math.max;

/**
 * Type names and serializers for various parameters that
 */
public enum ParameterType
{
    FORWARD_TO          (0, "FORWARD_TO",    ForwardToSerializer.instance),
    FORWARDED_FROM      (1, "FORWARD_FROM",  CompactEndpointSerializationHelper.instance),
    FAILURE_RESPONSE    (2, "FAIL",          DummyByteVersionedSerializer.instance),
    FAILURE_REASON      (3, "FAIL_REASON",   ShortVersionedSerializer.instance),
    FAILURE_CALLBACK    (4, "CAL_BAC",       DummyByteVersionedSerializer.instance),
    TRACE_SESSION       (5, "TraceSession",  UUIDSerializer.serializer),
    TRACE_TYPE          (6, "TraceType",     Tracing.traceTypeSerializer),
    TRACK_REPAIRED_DATA (7, "TrackRepaired", DummyByteVersionedSerializer.instance);

    public final int id;
    public final String legacyAlias;
    public final IVersionedSerializer serializer;

    private static final ParameterType[] idToTypeMap;
    private static final Map<String, ParameterType> aliasToTypeMap;

    static
    {
        ParameterType[] types = values();

        int max = -1;
        for (ParameterType t : types)
            max = max(t.id, max);

        ParameterType[] idMap = new ParameterType[max + 1];
        Map<String, ParameterType> aliasMap = new HashMap<>();

        for (ParameterType type : types)
        {
            if (idMap[type.id] != null)
                throw new RuntimeException("Two ParamaterType-s that map to the same id: " + type.id);
            idMap[type.id] = type;

            if (aliasMap.put(type.legacyAlias, type) != null)
                throw new RuntimeException("Two ParamaterType-s that map to the same legacy alias: " + type.legacyAlias);
        }

        idToTypeMap = idMap;
        aliasToTypeMap = aliasMap;
    }

    ParameterType(int id, String legacyAlias, IVersionedSerializer serializer)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParameterType id must be non-negative");

        this.id = id;
        this.legacyAlias = legacyAlias;
        this.serializer = serializer;
    }

    @Nullable
    public static ParameterType lookUpById(int id)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParameterType id must be non-negative (got " + id + ')');

        return id < idToTypeMap.length ? idToTypeMap[id] : null;
    }

    @Nullable
    public static ParameterType lookUpByAlias(String alias)
    {
        return aliasToTypeMap.get(alias);
    }
}
