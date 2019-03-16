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

package org.apache.cassandra.test.microbench;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.utils.MerkleTreeTest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.xerial.snappy.Snappy;

import static org.apache.cassandra.utils.MerkleTree.RECOMMENDED_DEPTH;

@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class MerkleTreeSerializerBench
{

    /**
     * If a test assumes that the tree is 8 units wide, then it should set this value
     * to 8.
     */
    public static BigInteger TOKEN_SCALE = new BigInteger("8");
    public static byte[] DUMMY = "blah".getBytes();

    public static final IPartitioner partitioner = RandomPartitioner.instance;
    public static final Range<Token> full = new Range<>(tok(-1), tok(-1));


    @State(Scope.Thread)
    public static class ThreadState
    {
        MerkleTree tree;
        int concurrent;

        @Setup
        public void setup() throws IOException
        {
            tree = new MerkleTree(MerkleTreeSerializerBench.partitioner, full, (byte) 16 , Integer.MAX_VALUE);
            tree.init();
            for (MerkleTree.TreeRange range : tree.invalids())
                range.addAll(new HIterator(range.right));

            for(int i = 0; i < Math.pow(2, 16); i++)
                tree.split(tok(i));

            for(int i = 0; i < Math.pow(2, 16); i++)
                tree.get(tok(i)).hash("12345678901234567890123456789012".getBytes());

            tree.hash(full);
            concurrent = 5;
        }

    }


    public static Token tok(int i)
    {
        if (i == -1)
            return new RandomPartitioner.BigIntegerToken(new BigInteger("-1"));
        BigInteger bint = RandomPartitioner.MAXIMUM.divide(TOKEN_SCALE).multiply(new BigInteger(""+i));
        return new RandomPartitioner.BigIntegerToken(bint);
    }


    @Benchmark
    public void serDeser(ThreadState t) throws IOException
    {
        DatabaseDescriptor.setOffheapMerkleTreesEnabled(false);

        DataOutputBuffer out = new DataOutputBuffer();
        t.tree.serialize(out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        DataInputPlus[] in = new DataInputBuffer[t.concurrent];
        MerkleTree[] restored = new MerkleTree[t.concurrent];
        for(int i = 0; i < t.concurrent; i++)
            in[i] = new DataInputBuffer(serialized);

        for (int i = 0; i < t.concurrent; i++)
             restored[i] = MerkleTree.deserialize(in[i], MessagingService.current_version);
    }

    @Benchmark
    public void serDeserOffheap(ThreadState t) throws IOException
    {
        DatabaseDescriptor.setOffheapMerkleTreesEnabled(true);

        DataOutputBuffer out = new DataOutputBuffer();
        t.tree.serialize(out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        DataInputPlus[] in = new DataInputBuffer[t.concurrent];
        MerkleTree[] restored = new MerkleTree[t.concurrent];
        for(int i = 0; i < t.concurrent; i++)
            in[i] = new DataInputBuffer(serialized);

        for (int i = 0; i < t.concurrent; i++)
            restored[i] = MerkleTree.deserialize(in[i], MessagingService.current_version);
    }

    @Benchmark
    public void serDeserHash(ThreadState t) throws IOException
    {
        DatabaseDescriptor.setOffheapMerkleTreesEnabled(false);

        DataOutputBuffer out = new DataOutputBuffer();
        t.tree.serialize(out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        DataInputPlus[] in = new DataInputBuffer[t.concurrent];
        MerkleTree[] restored = new MerkleTree[t.concurrent];
        for(int i = 0; i < t.concurrent; i++)
            in[i] = new DataInputBuffer(serialized);

        for (int i = 0; i < t.concurrent; i++)
        {
            restored[i] = MerkleTree.deserialize(in[i], MessagingService.current_version);
            restored[i].hash(full);
        }
    }

    @Benchmark
    public void serDeserOffheapHash(ThreadState t) throws IOException
    {
        DatabaseDescriptor.setOffheapMerkleTreesEnabled(true);

        DataOutputBuffer out = new DataOutputBuffer();
        t.tree.serialize(out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        DataInputPlus[] in = new DataInputBuffer[t.concurrent];
        MerkleTree[] restored = new MerkleTree[t.concurrent];
        for(int i = 0; i < t.concurrent; i++)
            in[i] = new DataInputBuffer(serialized);

        for (int i = 0; i < t.concurrent; i++)
        {
            restored[i] = MerkleTree.deserialize(in[i], MessagingService.current_version);
            restored[i].hash(full);
        }
    }


    @Benchmark
    public void serDeserHashDiff(ThreadState t) throws IOException
    {
        DatabaseDescriptor.setOffheapMerkleTreesEnabled(false);

        DataOutputBuffer out = new DataOutputBuffer();
        t.tree.serialize(out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        DataInputPlus[] in = new DataInputBuffer[t.concurrent];
        DataInputPlus[] in2 = new DataInputBuffer[t.concurrent];

        MerkleTree[] restored = new MerkleTree[t.concurrent];
        MerkleTree[] restored2 = new MerkleTree[t.concurrent];
        for(int i = 0; i < t.concurrent; i++)
        {
            in[i] = new DataInputBuffer(serialized);
            in2[i] = new DataInputBuffer(serialized);
        }

        for (int i = 0; i < t.concurrent; i++)
        {
            restored[i] = MerkleTree.deserialize(in[i], MessagingService.current_version);
            restored[i].hash(full);
            restored2[i] = MerkleTree.deserialize(in2[i], MessagingService.current_version);
            restored2[i].hash(full);
            MerkleTree.difference(restored[i], restored2[i]);
        }
    }

    @Benchmark
    public void serDeserOffheapHashDiff(ThreadState t) throws IOException
    {
        DatabaseDescriptor.setOffheapMerkleTreesEnabled(true);

        DataOutputBuffer out = new DataOutputBuffer();
        t.tree.serialize(out, MessagingService.current_version);
        byte[] serialized = out.toByteArray();

        DataInputPlus[] in = new DataInputBuffer[t.concurrent];
        DataInputPlus[] in2 = new DataInputBuffer[t.concurrent];

        MerkleTree[] restored = new MerkleTree[t.concurrent];
        MerkleTree[] restored2 = new MerkleTree[t.concurrent];
        for(int i = 0; i < t.concurrent; i++)
        {
            in[i] = new DataInputBuffer(serialized);
            in2[i] = new DataInputBuffer(serialized);
        }

        for (int i = 0; i < t.concurrent; i++)
        {
            restored[i] = MerkleTree.deserialize(in[i], MessagingService.current_version);
            restored[i].hash(full);
            restored2[i] = MerkleTree.deserialize(in2[i], MessagingService.current_version);
            restored2[i].hash(full);
            MerkleTree.difference(restored[i], restored2[i]);
        }
    }



    static class HIterator extends AbstractIterator<MerkleTree.RowHash>
    {
        private Iterator<Token> tokens;

        public HIterator(int... tokens)
        {
            List<Token> tlist = new LinkedList<Token>();
            for (int token : tokens)
                tlist.add(tok(token));
            this.tokens = tlist.iterator();
        }

        public HIterator(Token... tokens)
        {
            this.tokens = Arrays.asList(tokens).iterator();
        }

        public MerkleTree.RowHash computeNext()
        {
            if (tokens.hasNext())
                return new MerkleTree.RowHash(tokens.next(), DUMMY, DUMMY.length);
            return endOfData();
        }
    }

}
