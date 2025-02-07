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
package org.apache.cassandra.service.tracking;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.db.Mutation;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/*
 * TODO: token -> mutation_id index for the read path
 */
public abstract class CoordinatorLog
{
    protected final CoordinatorLogId id;
    protected final Participants participants;

    /**
     * Id <-> token index for unreconciled mutation ids.
     */
    private final IdTokenIndex index;

    protected final SequenceIds[] witnessedIds;
    protected final ReadWriteLock lock;

    CoordinatorLog(CoordinatorLogId id, Participants participants)
    {
        this.id = id;
        this.participants = participants;
        this.index = new IdTokenIndex();
        this.lock = new ReentrantReadWriteLock();

        SequenceIds[] ids = new SequenceIds[participants.size()];
        for (int i = 0; i < participants.size(); i++)
            ids[i] = new SequenceIds();
        witnessedIds = ids;
    }

    static CoordinatorLog create(int localHostId, CoordinatorLogId id, Participants participants)
    {
        return id.hostId == localHostId ? new CoordinatorLogPrimary(id, participants)
                                        : new CoordinatorLogReplica(id, participants);
    }

    void witnessedMutations(SequenceIds ranges, int onHostId)
    {
        lock.writeLock().lock();
        try
        {
            get(onHostId).merge(ranges);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void witnessedMutation(MutationId mutationId, int onHostId)
    {
        lock.writeLock().lock();
        try
        {
            if (!get(onHostId).add(mutationId.sequenceId()))
                return; // already witnessed

            // see if any other replicas haven't witnessed the id yet
            for (int i = 0; i < participants.size(); i++)
            {
                int hostId = participants.get(i);
                if (hostId != onHostId && !get(hostId).contains(mutationId.sequenceId()))
                    return;
            }

            index.remove(mutationId); // if none do then clean up the index for this mutation id
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void witnessedMutationLocal(MutationId mutationId, Mutation mutation)
    {
        lock.writeLock().lock();
        try
        {
            if (!get(id.hostId).add(mutationId.sequenceId()))
                return; // already witnessed

            // see if any other replicas haven't witnessed the id yet
            boolean allWitnessed = true;
            for (int i = 0; i < participants.size() && allWitnessed; i++)
            {
                int hostId = participants.get(i);
                if (hostId != id.hostId && !get(hostId).contains(mutationId.sequenceId()))
                    allWitnessed = false;
            }

            if (!allWitnessed)
                index.add(mutationId, mutation); // if some haven't witnessed, we should update the token index
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    protected SequenceIds get(int hostId)
    {
        return witnessedIds[participants.indexOf(hostId)];
    }

    public static class CoordinatorLogPrimary extends CoordinatorLog
    {
        AtomicLong sequenceId = new AtomicLong(0);

        CoordinatorLogPrimary(CoordinatorLogId id, Participants participants)
        {
            super(id, participants);
        }

        MutationId nextId()
        {
            return new MutationId(id.asLong(), nextSequenceId());
        }

        private long nextSequenceId()
        {
            while (true)
            {
                long prev = sequenceId.get();
                int prevOffset = MutationId.offset(prev);
                int prevTimestamp = MutationId.timestamp(prev);

                int nextOffset = prevOffset + 1;
                int nextTimestamp = Math.max(prevTimestamp, (int) currentTimeMillis() / 1000);
                long next = MutationId.sequenceId(nextOffset, nextTimestamp);

                if (sequenceId.compareAndSet(prev, next))
                    return next;
            }
        }
    }

    public static class CoordinatorLogReplica extends CoordinatorLog
    {
        CoordinatorLogReplica(CoordinatorLogId id, Participants participants)
        {
            super(id, participants);
        }
    }
}
