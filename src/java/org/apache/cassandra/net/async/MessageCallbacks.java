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

import org.apache.cassandra.net.Verb;

public interface MessageCallbacks
{
    public static final MessageCallbacks NOOP = new MessageCallbacks()
    {
        public void onProcessed(int messageSize) {}
        public void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit) {}
        public void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit) {}
        public void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t) {}
    };

    void onProcessed(int messageSize);
    void onExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit);
    void onArrivedExpired(int messageSize, long id, Verb verb, long timeElapsed, TimeUnit unit);
    void onFailedDeserialize(int messageSize, long id, long expiresAtNanos, boolean callBackOnFailure, Throwable t);
}
