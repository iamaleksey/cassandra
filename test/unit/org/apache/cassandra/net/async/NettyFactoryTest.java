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

package org.apache.cassandra.net.async;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.net.async.NettyFactory.CRC24_INIT;
import static org.apache.cassandra.net.async.NettyFactory.CRC24_POLY;
import static org.apache.cassandra.net.async.NettyFactory.crc24;

public class NettyFactoryTest
{

    private static int crc24naive(long bytes, int len)
    {
        int crc = CRC24_INIT;
        while (len-- > 0)
        {
            crc ^= (bytes & 0xff) << 16;
            bytes >>= 8;

            for (int i = 0; i < 8; i++)
            {
                crc <<= 1;
                if ((crc & 0x1000000) == 0x1000000)
                    crc ^= CRC24_POLY;
            }
        }
        return crc;
    }

    @Test
    public void testCrc24()
    {
        Random random = new Random(0);
        for (int i = 0 ; i < 1000; ++i)
        {
            long value = random.nextLong();
            int bytes = 4 + random.nextInt(4);
            Assert.assertEquals(crc24naive(value, bytes), crc24(value, bytes));
        }
    }

}
