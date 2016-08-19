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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.nifi.logging.ComponentLog;

import java.io.Closeable;
import java.io.IOException;

/**
 * A wrapper for a Kafka Consumer obtained from a ConsumerPool. Client's should call poison() to indicate that this
 * consumer should no longer be used by other clients, and should always call close(). Calling close() will pass
 * this lease back to the pool and the pool will determine the appropriate handling based on whether the underlying
 * consumer has been poisoned and whether the pool is already full.
 */
public class ConsumerLease implements Closeable {

    private final ComponentLog logger;
    private final Consumer<byte[],byte[]> consumer;
    private final ConsumerPool consumerPool;
    private boolean poisoned = false;

    /**
     * @param consumer the Kafka Consumer
     * @param consumerPool the ConsumerPool this ConsumerLease was obtained from
     * @param logger the logger to report any errors/warnings
     */
    public ConsumerLease(final Consumer<byte[], byte[]> consumer, final ConsumerPool consumerPool, final ComponentLog logger) {
        this.logger = logger;
        this.consumer = consumer;
        this.consumerPool = consumerPool;
    }

    /**
     * Polls the underlying consumer with the given timeout, returning any records that were obtained.
     *
     * @param timeout the timeout to poll with
     * @return records that were obtained when polling
     */
    public ConsumerRecords<byte[], byte[]> poll(final long timeout) {
        return consumer.poll(timeout);
    }

    /**
     * Performs a synchronous commit on the underlying consumer.
     */
    public void commit() {
        consumer.commitSync();
    }

    /**
     * Sets the poison flag for this consumer to true.
     */
    public void poison() {
        poisoned = true;
    }

    /**
     * @return true if this consumer has been poisoned, false otherwise
     */
    public boolean isPoisoned() {
        return poisoned;
    }

    /**
     * Notifies the pool that this lease is done being used.
     *
     * @throws IOException should not be thrown
     */
    @Override
    public void close() throws IOException {
        consumerPool.returnLease(this);
    }

    /**
     * Closes the underlying consumer. Generally clients should be relying on the close() method to pass
     * this lease back to the ConsumerPool and the ConsumerPool will determine if this method needs to be called.
     */
    public void closeConsumer() {
        try {
            consumer.unsubscribe();
        } catch (Exception e) {
            logger.warn("Failed while unsubscribing " + consumer, e);
        }

        try {
            consumer.close();
        } catch (Exception e) {
            logger.warn("Failed while closing " + consumer, e);
        }
    }

}
