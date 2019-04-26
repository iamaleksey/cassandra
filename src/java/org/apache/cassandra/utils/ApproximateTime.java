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

package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * This class provides approximate time utilities:
 *   - An imprecise nanoTime (monotonic) and currentTimeMillis (non-monotonic), that are faster than their regular counterparts
 *     They have a configured approximate precision (default of 10ms), which is the cadence they will be updated if the system is healthy
 *   - A mechanism for converting between nanoTime and currentTimeMillis measurements.
 *     These conversions may have drifted, and they offer no absolute guarantees on precision
 */
public class ApproximateTime
{
    private static final Logger logger = LoggerFactory.getLogger(ApproximateTime.class);
    private static final int ALMOST_NOW_UPDATE_INTERVAL_MS = Math.max(1, Integer.parseInt(System.getProperty(Config.PROPERTY_PREFIX + "approximate_time_precision_ms", "2")));
    private static final String CONVERSION_UPDATE_INTERVAL_PROPERTY = Config.PROPERTY_PREFIX + "NANOTIMETOMILLIS_TIMESTAMP_UPDATE_INTERVAL";
    private static final long ALMOST_SAME_TIME_UPDATE_INTERVAL_MS = Long.getLong(CONVERSION_UPDATE_INTERVAL_PROPERTY, 10000);

    public static class AlmostSameTime
    {
        final long millis;
        final long nanos;
        final long error; // maximum error of millis measurement (in nanos)

        private AlmostSameTime(long millis, long nanos, long error)
        {
            this.millis = millis;
            this.nanos = nanos;
            this.error = error;
        }

        public long toCurrentTimeMillis(long nanoTime)
        {
            return millis + TimeUnit.NANOSECONDS.toMillis(nanoTime - nanos);
        }

        public long toNanoTime(long currentTimeMillis)
        {
            return nanos + MILLISECONDS.toNanos(currentTimeMillis - millis);
        }
    }

    private static volatile long almostNowMillis;
    private static volatile long almostNowNanos;
    private static volatile AlmostSameTime almostSameTime = new AlmostSameTime(0L, 0L, Long.MAX_VALUE);
    private static double failedAlmostSameTimeUpdateModifier = 1.0;

    private static final Runnable refreshAlmostNow = () -> {
        almostNowMillis = System.currentTimeMillis();
        almostNowNanos = System.nanoTime();
    };

    private static final Runnable refreshAlmostSameTime = () -> {
        final int tries = 3;
        long[] samples = new long[2 * tries + 1];
        samples[0] = System.nanoTime();
        for (int i = 1 ; i < samples.length ; i += 2)
        {
            samples[i] = System.currentTimeMillis();
            samples[i + 1] = System.nanoTime();
        }

        int best = 1;
        // take sample with minimum delta between calls
        for (int i = 3 ; i < samples.length - 1 ; i += 2)
        {
            if ((samples[i+1] - samples[i-1]) < (samples[best+1]-samples[best-1]))
                best = i;
        }

        long millis = samples[best];
        long nanos = (samples[best+1] / 2) + (samples[best-1] / 2);
        long error = (samples[best+1] / 2) - (samples[best-1] / 2);

        AlmostSameTime prev = almostSameTime;
        AlmostSameTime next = new AlmostSameTime(millis, nanos, error);

        if (next.error > prev.error && next.error > prev.error * failedAlmostSameTimeUpdateModifier)
        {
            failedAlmostSameTimeUpdateModifier *= 1.1;
            return;
        }

        failedAlmostSameTimeUpdateModifier = 1.0;
        almostSameTime = next;
    };

    static
    {
        refreshAlmostNow.run();
        refreshAlmostSameTime.run();

        logger.info("Scheduling approximate time-check task with a precision of {} milliseconds", ALMOST_NOW_UPDATE_INTERVAL_MS);
        ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(refreshAlmostNow, ALMOST_NOW_UPDATE_INTERVAL_MS, ALMOST_NOW_UPDATE_INTERVAL_MS, MILLISECONDS);

        logger.info("Scheduling approximate time conversion task with an interval of {} milliseconds", ALMOST_SAME_TIME_UPDATE_INTERVAL_MS);
        ScheduledExecutors.scheduledFastTasks.scheduleWithFixedDelay(refreshAlmostSameTime, ALMOST_SAME_TIME_UPDATE_INTERVAL_MS, ALMOST_SAME_TIME_UPDATE_INTERVAL_MS, MILLISECONDS);
    }

    /**
     * Request an immediate refresh; this shouldn't generally be invoked, except perhaps by tests
     * WARNING: this can break monotonicity of almostNowNanos if invoked directly, so don't do it.
     */
    @VisibleForTesting
    public static void refresh()
    {
        refreshAlmostNow.run();
        refreshAlmostSameTime.run();
    }

    /** no guarantees about relationship to nanoTime; non-monotonic (tracks currentTimeMillis as closely as possible) */
    public static long currentTimeMillis()
    {
        return almostNowMillis;
    }

    /** no guarantees about relationship to currentTimeMillis; monotonic */
    public static long nanoTime()
    {
        return almostNowNanos;
    }

    public static long almostNowPrecision(TimeUnit units)
    {
        return units.convert(ALMOST_NOW_UPDATE_INTERVAL_MS, MILLISECONDS);
    }

    public static long currentTimeMillisPrecision()
    {
        return almostNowPrecision(MILLISECONDS);
    }

    public static long nanoTimePrecision()
    {
        return almostNowPrecision(NANOSECONDS);
    }

    /*
     * System.currentTimeMillis() is 25 nanoseconds. This is 2 nanoseconds (maybe) according to JMH.
     * Faster than calling both currentTimeMillis() and nanoTime().
     *
     * There is also the issue of how scalable nanoTime() and currentTimeMillis() are which is a moving target.
     *
     * These timestamps don't order with System.currentTimeMillis() because currentTimeMillis() can tick over
     * before this one does. I have seen it behind by as much as 2ms on Linux and 25ms on Windows.
     */
    public static long toCurrentTimeMillis(long nanoTime)
    {
        return almostSameTime.toCurrentTimeMillis(nanoTime);
    }

    public static long toNanoTime(long currentTimeMillis)
    {
        return almostSameTime.toNanoTime(currentTimeMillis);
    }

    public static AlmostSameTime snapshot()
    {
        return almostSameTime;
    }

    public static long conversionErrorNanos()
    {
        return almostSameTime.error;
    }

    public static boolean atLeastElapsedSinceNanoTime(long approxNanoTime, long elapsed, TimeUnit elapsedUnits)
    {
        return nanoTime() - approxNanoTime > elapsedUnits.toNanos(elapsed) + nanoTimePrecision();
    }

    public static long minElapsedSinceNanoTime(long approxNanoTime)
    {
        return (nanoTime() - approxNanoTime) - nanoTimePrecision();
    }

}
