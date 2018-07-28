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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class EmptyMessage
{
    public static final EmptyMessage emptyMessage = new EmptyMessage();
    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<EmptyMessage>
    {
        private Serializer() {}

        public void serialize(EmptyMessage emptyMessage, DataOutputPlus out, int version) throws IOException
        {
            if (emptyMessage != EmptyMessage.emptyMessage)
                throw new IllegalStateException();
        }

        public EmptyMessage deserialize(DataInputPlus in, int version) throws IOException
        {
            return emptyMessage;
        }

        public long serializedSize(EmptyMessage emptyMessage, int version)
        {
            return 0;
        }
    }

    private EmptyMessage() {}

}
