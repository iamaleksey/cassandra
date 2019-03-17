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

import java.util.EnumSet;
import java.util.Set;

public enum MessageFlag
{
    /*
     * The order matters here, since we use enum ordinals to encode the flags.
     */

    FAILURE_CALLBACK    (ParameterType.FAILURE_CALLBACK,    MessagingService.ONE_BYTE),
    FAILURE_RESPONSE    (ParameterType.FAILURE_RESPONSE,    MessagingService.ONE_BYTE),
    TRACK_REPAIRED_DATA (ParameterType.TRACK_REPAIRED_DATA, MessagingService.ONE_BYTE);

    final ParameterType legacyParam;
    final Object legacyValue;

    MessageFlag()
    {
        this(null, null);
    }

    MessageFlag(ParameterType legacyParam, Object legacyValue)
    {
        this.legacyParam = legacyParam;
        this.legacyValue = legacyValue;
    }

    public static EnumSet<MessageFlag> deserialize(int flags)
    {
        EnumSet<MessageFlag> set = EnumSet.noneOf(MessageFlag.class);
        for (int i = 0; i < ALL_VALUES.length; i++)
            if ((flags & (1 << i)) != 0)
                set.add(ALL_VALUES[i]);
        return set;
    }

    public static int serialize(Set<MessageFlag> flags)
    {
        int i = 0;
        for (MessageFlag flag : flags)
            i |= 1 << flag.ordinal();
        return i;
    }

    private static final MessageFlag[] ALL_VALUES = values();
}
