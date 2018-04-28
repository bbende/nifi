/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.util;


import java.util.function.Consumer;

public class ThreadUtils {

    /**
     * Returns a Thread running in the system with a given name.
     * @param threadName The name of the Thread
     * @return the Thread or null
     */
    public static Thread getNamedThread(String threadName) throws Exception{
        if (threadName == null || threadName.isEmpty()) {
            throw new IllegalArgumentException("Illegal threadName");
        }

        ThreadGroup threadGroup = Thread.currentThread().getThreadGroup();
        while (threadGroup.getParent() != null) {
            threadGroup = threadGroup.getParent();
        }

        Thread[] threads = new Thread[threadGroup.activeCount()];
        threadGroup.enumerate(threads);

        for (Thread thread : threads) {
            if (thread.getName().equalsIgnoreCase(threadName)) {
                return thread;
            }
        }
        return null;
    }


    /**
     * Runs a provided {@code Consumer} against a Thread running in the system with a given name.
     * @param threadName The name of the Thread
     * @param consumer A {@code Consumer} to operate on the thread
     */
    public static void consumeNamedThread(String threadName, Consumer<Thread> consumer) throws Exception{
        if (consumer == null) {
            throw new IllegalArgumentException("Invalid consumer");
        }

        Thread thread = getNamedThread(threadName);
        if(thread != null) {
            consumer.accept(thread);
        }
    }

}
