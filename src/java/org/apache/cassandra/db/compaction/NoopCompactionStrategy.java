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
package org.apache.cassandra.db.compaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * An {@code AbstractCompactionStrategy} that does nothing.
 * This class must only be used for system views which do not support compaction.
 */
public final class NoopCompactionStrategy extends AbstractCompactionStrategy
{

    public NoopCompactionStrategy(ColumnFamilyStore cfs, Map<String, String> options)
    {
        super(cfs, options);
    }

    @Override
    public AbstractCompactionTask getNextBackgroundTask(int gcBefore)
    {
        return null;
    }

    @Override
    public Collection<AbstractCompactionTask> getMaximalTask(int gcBefore, boolean splitOutput)
    {
        return Collections.emptyList();
    }

    @Override
    public AbstractCompactionTask getUserDefinedTask(Collection<SSTableReader> sstables, int gcBefore)
    {
        return null;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return 0;
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return 0;
    }

    @Override
    public void addSSTable(SSTableReader added)
    {
    }

    @Override
    public void removeSSTable(SSTableReader sstable)
    {
    }

    @Override
    protected Set<SSTableReader> getSSTables()
    {
        return Collections.emptySet();
    }
}
