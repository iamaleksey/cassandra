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

package org.apache.cassandra.service.reads.tracked;

import java.util.Collection;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

public interface PartialTrackedRead
{
    interface CompletedRead extends AutoCloseable
    {
        TrackedDataResponse response(); // must be called from the read stage
        Future<TrackedDataResponse> followupRead(TrackedDataResponse initialResponse, ConsistencyLevel consistencyLevel, long expiresAtNanos);

        @Override
        void close();

        static TrackedDataResponse createResponse(UnfilteredPartitionIterator partition, ReadCommand command)
        {
            PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, command.nowInSec());
            DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(),
                                                                     false,
                                                                     command.selectsFullPartition(),
                                                                     command.metadata().enforceStrictLiveness()).onlyCount();
            return TrackedDataResponse.create(counter.applyTo(iterator),
                                              command.columnFilter());
        }

        static CompletedRead simple(UnfilteredPartitionIterator partition, ReadCommand command)
        {
            return new CompletedRead()
            {
                @Override
                public TrackedDataResponse response()
                {
                    return createResponse(partition, command);
                }

                @Override
                public Future<TrackedDataResponse> followupRead(TrackedDataResponse initialRead, ConsistencyLevel consistencyLevel, long expiresAtNanos)
                {
                    return null;
                }

                @Override
                public void close()
                {
                    partition.close();
                }
            };
        }
    }

    /**
     * Sets consistency level and expiration info to be used for follow up reads. Needs to be called before making the
     * read available for receiving augmenting mutations
     */
    default void setFollowUpReadContext(ConsistencyLevel consistencyLevel, long expiresAtNanos) {}

    CompletedRead complete();

    default void complete(AsyncPromise<TrackedDataResponse> promise, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        complete(promise, this, consistencyLevel, expiresAtNanos);
    }

    static void complete(AsyncPromise<TrackedDataResponse> promise, PartialTrackedRead read, ConsistencyLevel consistencyLevel, long expiresAtNanos)
    {
        Stage.READ.submit(() -> {
            try (PartialTrackedRead.CompletedRead completedRead = read.complete())
            {
                TrackedDataResponse response = completedRead.response();
                Future<TrackedDataResponse> followUp = completedRead.followupRead(response, consistencyLevel, expiresAtNanos);

                if (followUp != null)
                {
                    followUp.addCallback((newResponse, error) -> {
                        if (error != null)
                        {
                            promise.tryFailure(error);
                            return;
                        }
                        promise.trySuccess(newResponse);
                    });
                }
                else
                {
                    promise.trySuccess(response);
                }
            }
            catch (Exception e)
            {
                promise.tryFailure(e);
                throw e;
            }
            finally
            {
                read.close();
            }
        });
    }

    void augment(Mutation mutation);

    default void augment(Collection<Mutation> mutations)
    {
        mutations.forEach(this::augment);
    }

    ReadExecutionController executionController();

    Index.Searcher searcher();

    ColumnFamilyStore cfs();

    long startTimeNanos();

    ReadCommand command();

    void close();
}
