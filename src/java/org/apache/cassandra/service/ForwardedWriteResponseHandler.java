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

package org.apache.cassandra.service;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;

// idk if this is really necessary
public class ForwardedWriteResponseHandler extends AbstractWriteResponseHandler<NoPayload>
{
    private final AbstractWriteResponseHandler<NoPayload> delegate;

    private ForwardedWriteResponseHandler(AbstractWriteResponseHandler<NoPayload> delegate)
    {
        super(delegate.replicaPlan, delegate.callback, delegate.writeType, () -> null, delegate.getRequestTime());
        this.delegate = delegate;
    }

    public static ForwardedWriteResponseHandler wrap(AbstractWriteResponseHandler<NoPayload> handler)
    {
        return new ForwardedWriteResponseHandler(handler);
    }

    @Override
    protected int ackCount()
    {
        return delegate.ackCount();
    }

    @Override
    public void onResponse(Message<NoPayload> msg)
    {
        delegate.onResponse(msg);
    }
}
