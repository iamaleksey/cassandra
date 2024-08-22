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

package org.apache.cassandra.journal;

import java.nio.ByteBuffer;

import accord.utils.Invariants;
import org.apache.cassandra.io.util.File;

public abstract class IndexedSegment<K, V> extends Segment<K, V>
{
    final File file;
    final Descriptor descriptor;
    final SyncedOffsets syncedOffsets;
    final Metadata metadata;
    final KeySupport<K> keySupport;
    final Index<K> index;
    ByteBuffer buffer;

    IndexedSegment(Descriptor descriptor, SyncedOffsets syncedOffsets, Metadata metadata, KeySupport<K> keySupport, Index<K> index)
    {
        this.file = descriptor.fileFor(Component.DATA);
        this.descriptor = descriptor;
        this.syncedOffsets = syncedOffsets;
        this.metadata = metadata;
        this.keySupport = keySupport;
        this.index = index;
    }

    Index<K> index()
    {
        return index;
    }

    @Override
    Descriptor descriptor()
    {
        return descriptor;
    }

    @Override
    boolean mayContainId(K key)
    {
        return index().mayContainId(key);
    }

    /*
     * Reading entries (by id, by offset, iterate)
     */

    public boolean readFirst(K id, RecordConsumer<K> consumer)
    {
        long offsetAndSize = index().lookUpFirst(id);
        if (offsetAndSize == -1)
            return false;

        EntrySerializer.EntryHolder<K> into = new EntrySerializer.EntryHolder<>();
        int offset = Index.readOffset(offsetAndSize);
        int size = Index.readSize(offset);
        if (read(offset, size, into))
        {
            Invariants.checkState(id.equals(into.key), "Index for %s read incorrect key: expected %s but read %s", descriptor, id, into.key);
            consumer.accept(descriptor.timestamp, offset, id, into.value, into.hosts, descriptor.userVersion);
            return true;
        }
        return false;
    }

    @Override
    public boolean readFirst(K id, EntrySerializer.EntryHolder<K> into)
    {
        long offsetAndSize = index().lookUpFirst(id);
        if (offsetAndSize == -1 || !read(Index.readOffset(offsetAndSize), Index.readSize(offsetAndSize), into))
            return false;
        Invariants.checkState(id.equals(into.key), "Index for %s read incorrect key: expected %s but read %s", descriptor, id, into.key);
        return true;
    }

    @Override
    public void readAll(K id, EntrySerializer.EntryHolder<K> into, Runnable onEntry)
    {
        long[] all = index().lookUpAll(id);

        for (int i = 0; i < all.length; i++)
        {
            int offset = Index.readOffset(all[i]);
            int size = Index.readSize(all[i]);
            Invariants.checkState(read(offset, size, into), "Read should always return true");
            onEntry.run();
        }
    }

    abstract boolean read(int offset, int size, EntrySerializer.EntryHolder<K> into);
}