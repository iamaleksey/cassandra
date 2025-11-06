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

package org.apache.cassandra.tools;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.cassandra.utils.ReflectionUtils;

public class FieldUtil
{
    private static final int JAVA_VERSION;
    private static final sun.misc.Unsafe UNSAFE;

    static
    {
        String version = System.getProperty("java.version");
        if (version.startsWith("1."))
        {
            // Java 8 or earlier: 1.8.0
            JAVA_VERSION = Integer.parseInt(version.substring(2, 3));
        }
        else
        {
            // Java 9+: 9.0.1, 11.0.2, 21.0.1, etc.
            int dotIndex = version.indexOf('.');
            int dashIndex = version.indexOf('-');
            int endIndex = dotIndex > 0 ? dotIndex : (dashIndex > 0 ? dashIndex : version.length());
            JAVA_VERSION = Integer.parseInt(version.substring(0, endIndex));
        }

        // Initialize Unsafe for Java 21+
        if (JAVA_VERSION >= 21)
        {
            try
            {
                Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                UNSAFE = (sun.misc.Unsafe) f.get(null);
            }
            catch (Throwable e)
            {
                throw new ExceptionInInitializerError("Failed to initialize Unsafe for Java " + JAVA_VERSION + ": " + e);
            }
        }
        else
        {
            UNSAFE = null;
        }
        System.err.println("  Java Version: " + JAVA_VERSION);
    }

    public static void setInstanceUnsafe(Class<?> klass, Object v, String fieldName)
    {
        try
        {
            setInstanceUnsafeThrowing(klass, v, fieldName);
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void setInstanceUnsafeThrowing(Class<?> klass, Object v, String fieldName) throws Throwable
    {
        Field field = ReflectionUtils.getField(klass, fieldName);
        field.setAccessible(true);

        //noinspection ResultOfMethodCallIgnored
        field.get(null);

        if (JAVA_VERSION >= 21)
        {
            // Java 21+: Use Unsafe to bypass module restrictions
            Object base = UNSAFE.staticFieldBase(field);
            long offset = UNSAFE.staticFieldOffset(field);

            // Critical: Use putObjectVolatile AND add memory barriers
            // This ensures the write is visible and prevents JIT from caching old values
            UNSAFE.putObjectVolatile(base, offset, v);

            // Force a full memory barrier to ensure all threads see the new value
            UNSAFE.fullFence();

            // Verify the field was actually set (helps catch issues early)
            Object readBack = UNSAFE.getObjectVolatile(base, offset);

            if (readBack != v)
            {
                throw new RuntimeException("Failed to set field " + fieldName + " on " + klass +
                                         ": expected " + v + " but read back " + readBack);
            }

            // Force JVM to drop all JIT-compiled code that may have inlined this field
            // This is necessary because static final fields can be constant-folded by the compiler
            try
            {
                // Trigger a GC which can cause deoptimization
                System.gc();
                System.gc();

                // Small sleep to allow GC and deoptimization to complete
                // This is a hack but necessary when fighting JIT inlining
                Thread.sleep(100);
            }
            catch (InterruptedException ignored)
            {
                Thread.currentThread().interrupt();
            }
        }
        else
        {
            // Pre-Java 21: Use traditional reflection approach
            Field modifiers = ReflectionUtils.getModifiersField();
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(null, v);
        }
    }

    public static void transferFields(Object sourceInstance, Class<?> klass)
    {
        for (Field sourceField : sourceInstance.getClass().getDeclaredFields())
        {
            sourceField.setAccessible(true);
            try
            {
                setInstanceUnsafe(klass, sourceField.get(sourceInstance), sourceField.getName());
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Failed to transfer field: " + sourceField.getName(), e);
            }
        }
    }
}