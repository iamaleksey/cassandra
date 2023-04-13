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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.SyncUtil;

public interface MappedBuffer
{
    enum Type { MAPPED, MOCKED }

    String KEY_TYPE = "cassandra.test.io.mapped.type";
    Type type = Type.valueOf(System.getProperty(KEY_TYPE, "mapped").toUpperCase());

    static MappedBuffer open(FileChannel channel, FileChannel.MapMode mode, int position, long size) throws IOException
    {
        return open(type, channel, mode, position, size);
    }

    static MappedBuffer open(Type type, FileChannel channel, FileChannel.MapMode mode, int position, long size) throws IOException
    {
        switch (type)
        {
            case MAPPED:
                return new Default(channel.map(mode, position, size));
            case MOCKED:
                return new Mocked(channel, mode, position, Math.toIntExact(size));
            default:
                throw new AssertionError("Unexpected type: " + type);
        }
    }

    int position();
    MappedBuffer position(int position);

    int limit();
    MappedBuffer limit(int limit);
    int capacity();

    int remaining();
    boolean hasRemaining();
    void putInt(int value);
    int getInt(int offset);

    ByteBuffer read(int offset, int length);

    MappedBuffer duplicate();

    void clean();
    void syncForce();

    DataInputBuffer in();
    DataOutputStreamPlus out();

    class Default implements MappedBuffer
    {
        private final MappedByteBuffer buffer;

        public Default(MappedByteBuffer buffer)
        {
            this.buffer = buffer;
        }

        @Override
        public int position()
        {
            return buffer.position();
        }

        @Override
        public MappedBuffer position(int position)
        {
            buffer.position(position);
            return this;
        }

        @Override
        public int limit()
        {
            return buffer.limit();
        }

        @Override
        public MappedBuffer limit(int limit)
        {
            buffer.limit(limit);
            return this;
        }

        @Override
        public int capacity()
        {
            return buffer.capacity();
        }

        @Override
        public int remaining()
        {
            return buffer.remaining();
        }

        @Override
        public boolean hasRemaining()
        {
            return buffer.hasRemaining();
        }

        @Override
        public void putInt(int offset)
        {
            buffer.putInt(offset);
        }

        @Override
        public int getInt(int offset)
        {
            return buffer.getInt(offset);
        }

        @Override
        public ByteBuffer read(int offset, int length)
        {
            return (ByteBuffer) buffer.duplicate().position(offset).limit(offset + length);
        }

        @Override
        public MappedBuffer duplicate()
        {
            return new Default((MappedByteBuffer) buffer.duplicate());
        }

        @Override
        public void clean()
        {
            FileUtils.clean(buffer);
        }

        @Override
        public void syncForce()
        {
            SyncUtil.force(buffer);
        }

        @Override
        public DataInputBuffer in()
        {
            return new DataInputBuffer(buffer, false);
        }

        @Override
        public DataOutputStreamPlus out()
        {
            return new DataOutputBufferFixed(buffer);
        }
    }

    class Mocked implements MappedBuffer
    {
        private final int capacity;
        private final FileChannel channel;
        private int position;
        private int limit;

        public Mocked(FileChannel channel, FileChannel.MapMode mode, int position, int size)
        {
            this.position = position;
            this.capacity = size;
            this.limit = size;
            this.channel = channel;
        }

        private Mocked(Mocked mocked)
        {
            this.channel = mocked.channel;
            this.position = mocked.position;
            this.capacity = mocked.capacity;
            this.limit = mocked.limit;
        }

        @Override
        public int position()
        {
            return position;
        }

        @Override
        public MappedBuffer position(int position)
        {
            this.position = position;
            return this;
        }

        @Override
        public int limit()
        {
            return limit;
        }

        @Override
        public MappedBuffer limit(int limit)
        {
            if (limit > capacity || limit < 0)
                throw new IllegalArgumentException();
            this.limit = limit;
            if (position > limit)
                position = limit;
            return this;
        }

        @Override
        public int capacity()
        {
            return capacity;
        }

        @Override
        public int remaining()
        {
            int rem = limit - position;
            return rem > 0 ? rem : 0;
        }

        @Override
        public boolean hasRemaining()
        {
            return position < limit;
        }

        @Override
        public void putInt(int value)
        {
            try
            {
                ByteBuffer buffer = ByteBufferUtil.bytes(value);
                while (buffer.hasRemaining())
                    position += channel.write(buffer, position);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public int getInt(int offset)
        {
            ByteBuffer bb = ByteBuffer.allocate(4);
            readFully(bb, offset);
            bb.flip();
            return bb.getInt();
        }

        @Override
        public ByteBuffer read(int offset, int length)
        {
            ByteBuffer bb = ByteBuffer.allocate(length);
            readFully(bb, offset);
            bb.flip();
            return bb;
        }

        private void readFully(ByteBuffer bb, int offset)
        {
            try
            {
                while (bb.hasRemaining())
                    offset += channel.read(bb, offset);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public MappedBuffer duplicate()
        {
            return new Mocked(this);
        }

        @Override
        public void clean()
        {

        }

        @Override
        public void syncForce()
        {

        }

        @Override
        public DataInputBuffer in()
        {
            return new DataInputBuffer(ByteBuffer.allocate(0), false) {
                @Override
                protected void reBuffer()
                {
                    ByteBuffer bb = ByteBuffer.allocate(1);
                    try
                    {
                        while (bb.hasRemaining())
                            position += channel.read(bb, position);
                    }
                    catch (IOException e)
                    {
                        throw new UncheckedIOException(e);
                    }
                    bb.flip();
                    this.buffer = bb;
                }
            };
        }

        @Override
        public DataOutputStreamPlus out()
        {
            WritableByteChannel proxy = new WritableByteChannel() {
                @Override
                public boolean isOpen()
                {
                    return channel.isOpen();
                }

                @Override
                public void close()
                {
                    // ignore
                }

                @Override
                public int write(ByteBuffer src) throws IOException
                {
                    int size = channel.write(src);
                    position += size;
                    return size;
                }
            };
            return new BufferedDataOutputStreamPlus(proxy);
        }
    }
}
