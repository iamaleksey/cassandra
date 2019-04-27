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

import java.util.function.Consumer;

import org.apache.cassandra.net.Message;

public interface OutboundMessageCallbacks
{
    static OutboundMessageCallbacks invokeOnDrop(Consumer<Message<?>> onDrop)
    {
        return new OutboundMessageCallbacks()
        {
            public void onSendFrame(int messageCount, int payloadSizeInBytes) {}
            public void onSentFrame(int messageCount, int payloadSizeInBytes) {}
            public void onFailedFrame(int messageCount, int payloadSizeInBytes) {}
            public void onOverloaded(Message<?> message) { onDrop.accept(message); }
            public void onExpired(Message<?> message) { onDrop.accept(message); }
            public void onFailedSerialize(Message<?> message) { onDrop.accept(message); }
            public void onClosed(Message<?> message) { onDrop.accept(message); }
        };
    }

    /** A complete has been handed to Netty to write to the wire */
    void onSendFrame(int messageCount, int payloadSizeInBytes);

    /** A complete has been serialized to the wire */
    void onSentFrame(int messageCount, int payloadSizeInBytes);

    /** Failed to send an entire frame due to network problems; presumed to be invoked in same order as onSendFrame */
    void onFailedFrame(int messageCount, int payloadSizeInBytes);

    /** A message was not enqueued to the link because too many messages are already waiting to send */
    void onOverloaded(Message<?> message);

    /** A message was not serialized to a frame because it had expired */
    void onExpired(Message<?> message);

    /** A message was not serialized to a frame because an exception was thrown */
    void onFailedSerialize(Message<?> message);

    /** A message was not sent because the connection was forcibly closed */
    void onClosed(Message<?> message);
}
