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

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Message.Header;

public interface InboundMessageCallbacks
{
    InboundMessageCallbacks NOOP = new InboundMessageCallbacks()
    {
        public void onArrived(int messageSize, Header header, long timeElapsed, TimeUnit unit) {}
        public void onDispatched(int messageSize, Header header) {}
        public void onExecuting(int messageSize, Header header, long timeElapsed, TimeUnit unit) {}
        public boolean shouldProcess(int messageSize, Message message) { return true; }
        public void onProcessing(int messageSize, Message message) {}
        public void onProcessed(int messageSize, Header header) {}
        public void onExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit) {}
        public void onArrivedExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit) {}
        public void onFailedDeserialize(int messageSize, Header header, Throwable t) {}
    };

    void onArrived(int messageSize, Header header, long timeElapsed, TimeUnit unit);
    void onDispatched(int messageSize, Header header);
    void onExecuting(int messageSize, Header header, long timeElapsed, TimeUnit unit);
    boolean shouldProcess(int messageSize, Message message);
    void onProcessing(int messageSize, Message message);
    void onProcessed(int messageSize, Header header);
    void onExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit);
    void onArrivedExpired(int messageSize, Header header, long timeElapsed, TimeUnit unit);
    void onFailedDeserialize(int messageSize, Header header, Throwable t);
}
