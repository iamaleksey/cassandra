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

package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.utils.ApproximateTime;

public abstract class MonitorableImpl implements Monitorable
{
    private MonitoringState state;
    private boolean isSlow;
    private long constructionTimeNanos = -1;
    private long timeout;
    private long slowTimeout;
    private boolean isCrossNode;

    protected MonitorableImpl()
    {
        this.state = MonitoringState.IN_PROGRESS;
        this.isSlow = false;
    }

    /**
     * This setter is ugly but the construction chain to ReadCommand
     * is too complex, it would require passing new parameters to all serializers
     * or specializing the serializers to accept these message properties.
     */
    public void setMonitoringTime(long constructionTime, boolean isCrossNode, long timeout, long slowTimeout)
    {
        assert constructionTime >= 0;
        this.constructionTimeNanos = constructionTime;
        this.isCrossNode = isCrossNode;
        this.timeout = timeout;
        this.slowTimeout = slowTimeout;
    }

    public long constructionTimeNanos()
    {
        return constructionTimeNanos;
    }

    public long timeoutNanos()
    {
        return timeout;
    }

    public boolean isCrossNode()
    {
        return isCrossNode;
    }

    public long slowTimeoutNanos()
    {
        return slowTimeout;
    }

    public boolean isInProgress()
    {
        check();
        return state == MonitoringState.IN_PROGRESS;
    }

    public boolean isAborted()
    {
        check();
        return state == MonitoringState.ABORTED;
    }

    public boolean isCompleted()
    {
        check();
        return state == MonitoringState.COMPLETED;
    }

    public boolean isSlow()
    {
        check();
        return isSlow;
    }

    public boolean abort()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (constructionTimeNanos >= 0)
                MonitoringTask.addFailedOperation(this, ApproximateTime.nanoTime());

            state = MonitoringState.ABORTED;
            return true;
        }

        return state == MonitoringState.ABORTED;
    }

    public boolean complete()
    {
        if (state == MonitoringState.IN_PROGRESS)
        {
            if (isSlow && slowTimeout > 0 && constructionTimeNanos >= 0)
                MonitoringTask.addSlowOperation(this, ApproximateTime.nanoTime());

            state = MonitoringState.COMPLETED;
            return true;
        }

        return state == MonitoringState.COMPLETED;
    }

    private void check()
    {
        if (constructionTimeNanos < 0 || state != MonitoringState.IN_PROGRESS)
            return;

        long elapsed = ApproximateTime.nanoTime() - constructionTimeNanos;

        if (elapsed >= slowTimeout && !isSlow)
            isSlow = true;

        if (elapsed >= timeout)
            abort();
    }
}
