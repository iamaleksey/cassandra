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

package org.apache.cassandra.distributed.test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.Cluster;

public class LargeColumnTest extends DistributedTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(LargeColumnTest.class);
    private static String str(int length, Random random)
    {
        char[] chars = new char[length];
        int i = 0;
        int s = 0;
        long v = 0;
        while (i < length)
        {
            if (s == 0)
            {
                v = random.nextLong();
                s = 8;
            }
            chars[i] = (char) (((v & 127) + 32) & 127);
            v >>= 8;
            --s;
            ++i;
        }
        return new String(chars);
    }

    private void testLargeColumns(int nodes, int columnSize, int rowCount) throws Throwable
    {
        Random random = new Random();
        long seed = ThreadLocalRandom.current().nextLong();
        logger.info("Using seed {}", seed);
        random.setSeed(seed);

        try (Cluster cluster = init(Cluster.build(nodes)
                                           .withConfig(config ->
                                                       config.set("commitlog_segment_size_in_mb", 128)
                                                             .set("memtable_heap_space_in_mb", 256)
                                           )
                                           .start()))
        {
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k int, c text, PRIMARY KEY (k))", KEYSPACE));

            for (int i = 0 ; i < rowCount ; ++i)
                cluster.coordinator(1).execute(String.format("INSERT INTO %s.cf (k, c) VALUES (?, ?);", KEYSPACE), ConsistencyLevel.ALL, i, str(columnSize, random));

            for (int i = 0 ; i < rowCount ; ++i)
            {
                Object[][] results = cluster.coordinator(1).execute(String.format("SELECT k, c FROM %s.cf;", KEYSPACE), ConsistencyLevel.ALL);
            }

            System.out.println("Done");
        }
    }

    @Test
    public void test() throws Throwable
    {
        testLargeColumns(2, 63 << 20, 5);
    }

}
