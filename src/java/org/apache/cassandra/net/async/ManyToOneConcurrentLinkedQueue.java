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

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

/**
 * A concurrent many-producers-to-single-consumer linked queue.
 *
 * Based roughly on {@link java.util.concurrent.ConcurrentLinkedQueue}, except with simpler/cheaper consumer-side
 * method implementations ({@link #poll()}, {@link #remove()}, {@link #drain(Consumer)}, and padding added
 * to prevent false sharing.
 *
 * {@link #offer(Object)} provides volatile visibility semantics, and both {@link #offer(Object)} and {@link #poll()}
 * are non-blocking.
 */
class ManyToOneConcurrentLinkedQueue<E> extends ManyToOneConcurrentLinkedQueueHead<E>
{
    @SuppressWarnings("unused")
    protected long p31, p32, p33, p34, p35, p36, p37, p38, p39, p40, p41, p42, p43, p44, p45;

    ManyToOneConcurrentLinkedQueue()
    {
        head = tail = new Node<>(null);
    }

    /**
     * A false positive return can *NOT* occur, but a false negative is possible if invoked from a non-consumer thread.
     */
    boolean relaxedIsEmpty()
    {
        return null == head.next;
    }

    E peek()
    {
        Node<E> next = head.next;
        if (null == next)
            return null;
        return next.item;
    }

    E poll()
    {
        Node<E> head = this.head;
        Node<E> next = head.next;

        if (null == next)
            return null;

        this.lazySetHead(next); // update head reference to next before making previous head node unreachable,
        head.lazySetNext(head); // to maintain the guarantee of tail being always reachable from head

        E item = next.item;
        next.item = null;
        return item;
    }

    E remove()
    {
        E item = poll();
        if (null == item)
            throw new NoSuchElementException("Queue is empty");
        return item;
    }

    void drain(Consumer<E> consumer)
    {
        E item;
        while ((item = poll()) != null)
            consumer.accept(item);
    }

    boolean offer(E e)
    {
        internalOffer(e); return true;
    }

    private E internalOffer(E e)
    {
        if (null == e)
            throw new NullPointerException();

        final Node<E> node = new Node<>(e);

        for (Node<E> t = tail, p = t;;)
        {
            Node<E> q = p.next;
            if (q == null)
            {
                // p is last node
                if (p.casNext(null, node))
                {
                    // successful CAS is the linearization point for e to become an element of this queue and for node to become "live".
                    if (p != t) // hop two nodes at a time
                        casTail(t, node); // failure is ok
                    return p.item;
                }
                // lost CAS race to another thread; re-read next
            }
            else if (p == q)
            {
                /*
                 * We have fallen off list (p was. If tail is unchanged, it will also be off-list, in which case we need to
                 * jump to head, from which all live nodes are always reachable. Else the new tail is a better bet.
                 */
                p = (t != (t = tail)) ? t : head;
            }
            else
            {
                // check for tail updates after two hops
                p = (p != t && t != (t = tail)) ? t : q;
            }
        }
    }
}

class ManyToOneConcurrentLinkedQueueHead<E> extends ManyToOneConcurrentLinkedQueuePadding2<E>
{
    protected volatile ManyToOneConcurrentLinkedQueue.Node<E> head;

    private static final AtomicReferenceFieldUpdater<ManyToOneConcurrentLinkedQueueHead, Node> headUpdater =
        AtomicReferenceFieldUpdater.newUpdater(ManyToOneConcurrentLinkedQueueHead.class, Node.class, "head");

    @SuppressWarnings("WeakerAccess")
    protected void lazySetHead(Node<E> val)
    {
        headUpdater.lazySet(this, val);
    }
}

class ManyToOneConcurrentLinkedQueuePadding2<E> extends ManyToOneConcurrentLinkedQueueTail<E>
{
    @SuppressWarnings("unused")
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;
}

class ManyToOneConcurrentLinkedQueueTail<E> extends ManyToOneConcurrentLinkedQueuePadding1
{
    protected volatile ManyToOneConcurrentLinkedQueue.Node<E> tail;

    private static final AtomicReferenceFieldUpdater<ManyToOneConcurrentLinkedQueueTail, Node> tailUpdater =
        AtomicReferenceFieldUpdater.newUpdater(ManyToOneConcurrentLinkedQueueTail.class, Node.class, "tail");

    @SuppressWarnings({ "WeakerAccess", "UnusedReturnValue" })
    protected boolean casTail(Node<E> expect, Node<E> update)
    {
        return tailUpdater.compareAndSet(this, expect, update);
    }
}

class ManyToOneConcurrentLinkedQueuePadding1
{
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;

    static final class Node<E>
    {
        E item;
        volatile Node<E> next;

        private static final AtomicReferenceFieldUpdater<Node, Node> nextUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

        Node(E item)
        {
            this.item = item;
        }

        @SuppressWarnings("SameParameterValue")
        boolean casNext(Node<E> expect, Node<E> update)
        {
            return nextUpdater.compareAndSet(this, expect, update);
        }

        void lazySetNext(Node<E> val)
        {
            nextUpdater.lazySet(this, val);
        }
    }
}
