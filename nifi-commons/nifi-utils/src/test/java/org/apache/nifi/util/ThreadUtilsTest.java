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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;


public class ThreadUtilsTest {
    private static final String THREAD_NAME = "theThread";
    private Thread theThread;
    private AtomicBoolean go;
    @Before
    public void setup() {
        go = new AtomicBoolean(true);
        theThread = new Thread(() -> {
            while (go.get()) {
                int x = 0;
            }
        });
        theThread.setName(THREAD_NAME);

        theThread.start();

    }

    @After
    public void cleanup() {
        if ( theThread != null && theThread.isAlive()) {
            go.set(false);
            try {
                theThread.join();
            } catch (Exception e) {
                // exit
            }
            theThread = null;
        }
    }

    @Test
    public void getNamedThread() throws Exception {
        Thread found = ThreadUtils.getNamedThread(THREAD_NAME);
        Assert.assertEquals(found, theThread);
    }

    @Test
    public void consumeNamedThread() throws Exception {
        AtomicBoolean called = new AtomicBoolean(false);
        ThreadUtils.consumeNamedThread(THREAD_NAME, (thread)-> called.set(true));
        Assert.assertTrue(called.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullName() throws Exception {
        ThreadUtils.getNamedThread(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptName() throws Exception {
        ThreadUtils.getNamedThread("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullConsumer() throws Exception {
        ThreadUtils.consumeNamedThread("foo",null);
    }
}