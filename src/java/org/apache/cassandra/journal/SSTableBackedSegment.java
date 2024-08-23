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

import java.io.IOException;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.StorageHook;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.utils.concurrent.Ref;

public class SSTableBackedSegment<K, V> extends Segment<K, V>
{
    private final Descriptor descriptor;
    private final ColumnFamilyStore cfs;

    private final ColumnMetadata recordColumn;
    private final ColumnMetadata hostsColumn;

    private final SSTableReader sstable;
    private final KeySupport<K> keySupport;

    private final Ref<Segment<K, V>> selfRef;

    public SSTableBackedSegment(SSTableReader sstable, KeySupport<K> keySupport)
    {
        this.cfs = Keyspace.open(AccordKeyspace.metadata().name)
                           .getColumnFamilyStore(AccordKeyspace.JOURNAL);
        this.recordColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("record", false));
        this.hostsColumn = cfs.metadata().getColumn(ColumnIdentifier.getInterned("hosts", false));
        this.sstable = sstable;
        this.descriptor = Descriptor.fromBytes(sstable.descriptor.baseFile(),
                                               sstable.descriptor.id.asBytes());
        this.keySupport = keySupport;
        this.selfRef = new Ref<>(this, new Tidier(descriptor, sstable));
    }

    @Override
    boolean readFirst(K key, EntrySerializer.EntryHolder<K> into)
    {
        into.clear();
        try (UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                               sstable,
                                                                               cfs.decorateKey(keySupport.serialize(key, descriptor.journalVersion)),
                                                                               Slices.ALL,
                                                                               ColumnFilter.all(cfs.metadata()),
                                                                               false,
                                                                               SSTableReadsListener.NOOP_LISTENER))
        {
            while (iter.hasNext())
            {
                Unfiltered unfiltered = iter.next();
                Invariants.checkState(unfiltered.isRow());
                into.key = key;
                Row row = (Row) unfiltered;
                into.value = row.getCell(recordColumn).buffer().duplicate();
                return true;
            }
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, Component.DATA, e);
        }

        return false;
    }

    @Override
    void readAll(K key, EntrySerializer.EntryHolder<K> into, Runnable onEntry)
    {
        into.clear();

        try (UnfilteredRowIterator iter = StorageHook.instance.makeRowIterator(cfs,
                                                                               sstable,
                                                                               cfs.decorateKey(keySupport.serialize(key, descriptor.journalVersion)),
                                                                               Slices.ALL,
                                                                               ColumnFilter.all(cfs.metadata()),
                                                                               false,
                                                                               SSTableReadsListener.NOOP_LISTENER))
        {
            while (iter.hasNext())
            {
                Unfiltered unfiltered = iter.next();
                Invariants.checkState(unfiltered.isRow());
                into.key = key;
                Row row = (Row) unfiltered;
                into.value = row.getCell(recordColumn).buffer();
                onEntry.run();
                into.clear();
            }
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, Component.DATA, e);
        }
    }

    @Override
    Descriptor descriptor()
    {
        return descriptor;
    }

    @Override
    boolean mayContainId(K key)
    {
        try
        {
            DecoratedKey dk = cfs.decorateKey(keySupport.serialize(key, descriptor.journalVersion));
            if (dk.compareTo(sstable.getFirst()) >= 0 &&
                dk.compareTo(sstable.getLast()) <= 0)
            {
                return sstable.mayContainAssumingKeyIsInRange(dk);
            }
        }
        catch (IOException e)
        {
            throw new JournalReadError(descriptor, Component.DATA, e);
        }

        return false;
    }

    @Override
    Kind kind()
    {
        return Kind.SSTABLE_BACKED;
    }

    @Override
    public void close()
    {
        release();
    }

    @Override
    public Ref<Segment<K, V>> tryRef()
    {
        return selfRef.tryRef();
    }

    @Override
    public Ref<Segment<K, V>> ref()
    {
        return selfRef.ref();
    }

    @Override
    void release()
    {
        selfRef.release();
    }

    private static final class Tidier implements Tidy
    {
        private final Descriptor descriptor;
        private final SSTableReader sstable;

        Tidier(Descriptor descriptor, SSTableReader sstable)
        {
            this.descriptor = descriptor;
            this.sstable = sstable;
            sstable.selfRef().tryRef();
        }

        @Override
        public void tidy()
        {
            sstable.selfRef().release();
        }

        @Override
        public String name()
        {
            return descriptor.toString();
        }
    }

    @Override
    public String toString()
    {
        return "SSTableBackedSegment{" +
               "descriptor=" + descriptor +
               '}';
    }
}