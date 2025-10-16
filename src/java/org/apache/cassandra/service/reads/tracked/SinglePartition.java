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

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.service.reads.tracked.TrackedRead.Id;
import org.apache.cassandra.service.reads.tracked.LeaderReads.Stage;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

abstract class SinglePartition
{
    /**
     * Initialized single partition read object, ready to perform a local read.
     */
    static final class Created implements LeaderReads.Created
    {
        private final Id id;
        private final ReadCommand command;
        private final int[] summaryNodes;

        private Created(Id id, ReadCommand command, int[] summaryNodes)
        {
            this.id = id;
            this.command = command;
            this.summaryNodes = summaryNodes;
        }

        static Created initialize(Id id, ReadCommand command, int[] summaryNodes)
        {
            return new Created(id, command, summaryNodes);
        }

        @Override
        public Id id()
        {
            return id;
        }

        @Override
        public Stage stage()
        {
            return Stage.INITIALIZED;
        }

        @Override
        public Launched launch(AsyncPromise<TrackedDataResponse> promise)
        {
            MutationSummary initialSummary, secondarySummary;
            initialSummary = command.createMutationSummary(false);
            ReadExecutionController controller = command.executionController(false);
            try
            {
                // TODO: the read
                // catch any mutations that may have arrived during initial read execution
                secondarySummary = command.createMutationSummary(true);
            }
            catch (Throwable t)
            {
                // TODO: error out on the promise
                controller.close();
                throw t;
            }
            return new Launched(id, promise, initialSummary, secondarySummary, summaryNodes);
        }
    }

    /**
     * Local read is completed, now waiting for reconciliation results for augmentation.
     */
    static final class Launched implements LeaderReads.Launched
    {
        private final Id id;
        private final AsyncPromise<TrackedDataResponse> promise;
        private final MutationSummary initialSummary;
        private final MutationSummary secondarySummary;
        private final int[] summaryNodes;

        private Launched(Id id, AsyncPromise<TrackedDataResponse> promise, MutationSummary initialSummary, MutationSummary secondaySummary, int[] summaryNodes)
        {
            this.id = id;
            this.promise = promise;
            this.initialSummary = initialSummary;
            this.secondarySummary = secondaySummary;
            this.summaryNodes = summaryNodes;
        }

        @Override
        public Id id()
        {
            return id;
        }

        @Override
        public Stage stage()
        {
            return Stage.READ;
        }

        @Override
        public Augmented augment(Log2OffsetsMap<?> augmentingOffsets)
        {
            return new Augmented(id, promise);
        }

        @Override
        public MutationSummary initialSummary()
        {
            return initialSummary;
        }

        @Override
        public MutationSummary secondarySummary()
        {
            return secondarySummary;
        }

        @Override
        public int[] summaryNodes()
        {
            return summaryNodes;
        }
    }

    /**
     * Augmented with the necessary mutation ids.
     * This is the terminal state for the single partition read - it doesn't need short read protection.
     */
    static final class Augmented implements LeaderReads.Augmented
    {
        private final Id id;
        private final AsyncPromise<TrackedDataResponse> promise;

        Augmented(Id id, AsyncPromise<TrackedDataResponse> promise)
        {
            this.id = id;
            this.promise = promise;
        }

        @Override
        public Id id()
        {
            return id;
        }

        @Override
        public Stage stage()
        {
            return Stage.AUGMENTED;
        }

        @Override
        public Completed complete()
        {
            return new Completed(id, promise);
        }
    }

    static final class Completed implements LeaderReads.ShortOrCompleted
    {
        private final Id id;
        private final AsyncPromise<TrackedDataResponse> promise;

        Completed(Id id, AsyncPromise<TrackedDataResponse> promise)
        {
            this.id = id;
            this.promise = promise;
        }

        @Override
        public Id id()
        {
            return id;
        }

        @Override
        public Stage stage()
        {
            return Stage.COMPLETED;
        }

        @Override
        public boolean isShort()
        {
            return false;
        }

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public LeaderReads.ShortOrCompleted complete()
        {
            return this;
        }

        @Override
        public void respond()
        {
            // TODO: implement
            promise.trySuccess(null);
        }
    }
}
