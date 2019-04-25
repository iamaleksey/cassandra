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

import java.io.EOFException;
import java.util.Iterator;

import org.apache.cassandra.io.util.RebufferingInputStream;

class ChunkedInputPlus extends RebufferingInputStream
{
    private SharedBytes current;
    private final Iterator<SharedBytes> remaining;

    private ChunkedInputPlus(SharedBytes current, Iterator<SharedBytes> remaining)
    {
        super(current.get());
        this.current = current;
        this.remaining = remaining;
    }

    static ChunkedInputPlus of(Iterable<SharedBytes> buffers)
    {
        Iterator<SharedBytes> iter = buffers.iterator();
        if (!iter.hasNext())
            throw new IllegalArgumentException();
        SharedBytes first = iter.next();

        return new ChunkedInputPlus(first, iter);
    }

    @Override
    protected void reBuffer() throws EOFException
    {
        buffer = null;
        current.release();
        current = null;

        if (!remaining.hasNext())
            throw new EOFException();

        current = remaining.next();
        buffer = current.get();
    }

    @Override
    public void close()
    {
        if (null != current)
        {
            buffer = null;
            current.release();
            current = null;
        }

        remaining.forEachRemaining(SharedBytes::release);
    }
}
