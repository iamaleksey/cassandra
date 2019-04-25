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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.vint.VIntCoding;

public class ForwardToSerializer implements IVersionedSerializer<ForwardToContainer>
{
    public static ForwardToSerializer instance = new ForwardToSerializer();

    private ForwardToSerializer() {}

    public void serialize(ForwardToContainer forwardToContainer, DataOutputPlus out, int version) throws IOException
    {
        List<InetAddressAndPort> targets = forwardToContainer.targets;
        if (version >= MessagingService.VERSION_40)
        {
            out.writeUnsignedVInt(targets.size());
            for (int i = 0; i < forwardToContainer.messageIds.length; i++)
            {
                CompactEndpointSerializationHelper.instance.serialize(targets.get(i), out, version);
                out.writeUnsignedVInt(forwardToContainer.messageIds[i]);
            }
        }
        else
        {
            out.writeInt(targets.size());
            for (int i = 0; i < forwardToContainer.messageIds.length; i++)
            {
                CompactEndpointSerializationHelper.instance.serialize(targets.get(i), out, version);
                out.writeInt(Ints.checkedCast(forwardToContainer.messageIds[i]));
            }
        }
    }

    public ForwardToContainer deserialize(DataInputPlus in, int version) throws IOException
    {
        long[] ids;
        List<InetAddressAndPort> hosts;
        if (version >= MessagingService.VERSION_40)
        {
            int count = Ints.checkedCast(in.readUnsignedVInt());
            ids = new long[count];
            hosts = new ArrayList<>(ids.length);
            for (int i = 0; i < ids.length; i++)
            {
                hosts.add(CompactEndpointSerializationHelper.instance.deserialize(in, version));
                ids[i] = in.readUnsignedVInt();
            }
        }
        else
        {
            int count = in.readInt();
            ids = new long[count];
            hosts = new ArrayList<>(ids.length);
            for (int i = 0; i < ids.length; i++)
            {
                hosts.add(CompactEndpointSerializationHelper.instance.deserialize(in, version));
                ids[i] = in.readInt();
            }
        }
        return new ForwardToContainer(hosts, ids);
    }

    public long serializedSize(ForwardToContainer forwardToContainer, int version)
    {
        if (version >= MessagingService.VERSION_40)
        {
            //Number of forward addresses, 4 bytes per for each id
            long[] ids = forwardToContainer.messageIds;
            List<InetAddressAndPort> hosts = forwardToContainer.targets;
            long size = VIntCoding.computeUnsignedVIntSize(ids.length);
            //Depending on ipv6 or ipv4 the address size is different.
            for (int i = 0 ; i < ids.length ; ++i)
            {
                size += VIntCoding.computeUnsignedVIntSize(ids[i]);
                size += CompactEndpointSerializationHelper.instance.serializedSize(hosts.get(i), version);
            }

            return size;

        }
        else
        {
            //Number of forward addresses, 4 bytes per for each id
            long size = 4 +
                        (4 * forwardToContainer.targets.size());
            //Depending on ipv6 or ipv4 the address size is different.
            for (InetAddressAndPort forwardTo : forwardToContainer.targets)
            {
                size += CompactEndpointSerializationHelper.instance.serializedSize(forwardTo, version);
            }

            return size;
        }
    }
}
