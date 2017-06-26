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
package org.apache.nifi.processors.standard;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.processor.util.put.sender.SocketChannelSender;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TestPutTCPRecord {

    private PutTCPRecord proc;
    private CapturingChannelSender sender;
    private MockRecordParser readerService;
    private RecordSetWriterFactory writerService;
    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        ComponentLog logger = Mockito.mock(ComponentLog.class);
        sender = new CapturingChannelSender("localhost", 12345, 0, logger);
        proc = new TestablePutTCPRecord(sender);

        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(PutTCPRecord.PORT, "12345");

        final String readerId = "record-reader";
        readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);

        final String writerId = "record-writer";
        writerService = new MockRecordWriter(null);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(PutTCPRecord.RECORD_READER, readerId);
        runner.setProperty(PutTCPRecord.RECORD_WRITER, writerId);
    }

    @Test
    public void testAllRecordsSuccessfulAndConnectionClosed() {
        runner.setProperty(PutTCPRecord.CONNECTION_PER_FLOWFILE, "true");

        readerService.addRecord("alice", "25");
        readerService.addRecord("bob", "22");
        readerService.addRecord("john", "30");

        runner.enqueue("trigger data");
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(PutTCPRecord.REL_SUCCESS, 1);

        // verify the successful count attribute isn't there
        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutTCPRecord.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeNotExists(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier());

        // verify the sender was closed
        Assert.assertFalse(sender.isConnected());
        runner.shutdown();

        // verify the records were sent correctly
        final byte[] data = sender.getAllData();
        Assert.assertNotNull(data);
        Assert.assertTrue(data.length > 0);

        final String recordData = new String(data, StandardCharsets.UTF_8);
        final String[] records = recordData.split("[\n]");
        Assert.assertEquals(3, records.length);
        Assert.assertEquals("\"alice\",\"25\"", records[0]);
        Assert.assertEquals("\"bob\",\"22\"", records[1]);
        Assert.assertEquals("\"john\",\"30\"", records[2]);

        // verify we reports a provenance SEND event with the correct uri
        Assert.assertEquals(1, runner.getProvenanceEvents().size());
        final ProvenanceEventRecord provEvent = runner.getProvenanceEvents().get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        Assert.assertEquals("TCP://localhost:12345", provEvent.getTransitUri());
        Assert.assertEquals("Records 1 to 3", provEvent.getDetails());
    }

    @Test
    public void testAllRecordsSuccessfulAndConnectionKeptOpen() {
        runner.setProperty(PutTCPRecord.CONNECTION_PER_FLOWFILE, "false");

        readerService.addRecord("alice", "25");
        readerService.addRecord("bob", "22");
        readerService.addRecord("john", "30");

        runner.enqueue("trigger data");
        runner.run(1, false);
        runner.assertAllFlowFilesTransferred(PutTCPRecord.REL_SUCCESS, 1);

        // verify the successful count attribute isn't there
        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutTCPRecord.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeNotExists(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier());

        // verify the sender wasn't closed
        Assert.assertTrue(sender.isConnected());
        runner.shutdown();

        // verify the records were sent correctly
        final byte[] data = sender.getAllData();
        Assert.assertNotNull(data);
        Assert.assertTrue(data.length > 0);

        final String recordData = new String(data, StandardCharsets.UTF_8);
        final String[] records = recordData.split("[\n]");
        Assert.assertEquals(3, records.length);
        Assert.assertEquals("\"alice\",\"25\"", records[0]);
        Assert.assertEquals("\"bob\",\"22\"", records[1]);
        Assert.assertEquals("\"john\",\"30\"", records[2]);

        // verify we reports a provenance SEND event with the correct uri
        Assert.assertEquals(1, runner.getProvenanceEvents().size());
        final ProvenanceEventRecord provEvent = runner.getProvenanceEvents().get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        Assert.assertEquals("TCP://localhost:12345", provEvent.getTransitUri());
        Assert.assertEquals("Records 1 to 3", provEvent.getDetails());
    }

    @Test
    public void testPartialSuccess() throws InitializationException {
        // re-create the writer and set it to fail after 2 records are written
        final String writerId = "record-writer";
        writerService = new MockRecordWriter(null, true,2);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(PutTCPRecord.RECORD_WRITER, writerId);

        readerService.addRecord("alice", "25");
        readerService.addRecord("bob", "22");
        readerService.addRecord("john", "30");

        runner.enqueue("trigger data");
        runner.run();
        runner.assertAllFlowFilesTransferred(PutTCPRecord.REL_FAILURE, 1);

        // verify that the successful count attribute was written with a value of 2
        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutTCPRecord.REL_FAILURE).get(0);
        mockFlowFile.assertAttributeEquals(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier(), "2");

        // verify the records were sent correctly
        final byte[] data = sender.getAllData();
        Assert.assertNotNull(data);
        Assert.assertTrue(data.length > 0);

        final String recordData = new String(data, StandardCharsets.UTF_8);
        final String[] records = recordData.split("[\n]");
        Assert.assertEquals(2, records.length);
        Assert.assertEquals("\"alice\",\"25\"", records[0]);
        Assert.assertEquals("\"bob\",\"22\"", records[1]);

        // verify we still reported a provenance SEND event with the correct uri
        Assert.assertEquals(1, runner.getProvenanceEvents().size());
        final ProvenanceEventRecord provEvent = runner.getProvenanceEvents().get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        Assert.assertEquals("TCP://localhost:12345", provEvent.getTransitUri());
        Assert.assertEquals("Records 1 to 2", provEvent.getDetails());
    }

    @Test
    public void testStartFromLastSuccessfulRecord() throws InitializationException {
        readerService.addRecord("alice", "25");
        readerService.addRecord("bob", "22");
        readerService.addRecord("john", "30");

        // simulate retrying a failure where we already wrote two records
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier(), "2");

        runner.enqueue("trigger data", attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutTCPRecord.REL_SUCCESS, 1);

        // verify that the successful count attribute was removed
        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutTCPRecord.REL_SUCCESS).get(0);
        mockFlowFile.assertAttributeNotExists(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier());

        // verify the records were sent correctly
        final byte[] data = sender.getAllData();
        Assert.assertNotNull(data);
        Assert.assertTrue(data.length > 0);

        final String recordData = new String(data, StandardCharsets.UTF_8);
        final String[] records = recordData.split("[\n]");
        Assert.assertEquals(1, records.length);
        Assert.assertEquals("\"john\",\"30\"", records[0]);

        // verify we still reported a provenance SEND event with the correct uri
        Assert.assertEquals(1, runner.getProvenanceEvents().size());
        final ProvenanceEventRecord provEvent = runner.getProvenanceEvents().get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        Assert.assertEquals("TCP://localhost:12345", provEvent.getTransitUri());
        Assert.assertEquals("Records 3 to 3", provEvent.getDetails());
    }

    @Test
    public void testStartFromLastSuccessfulWithAnotherFailure() throws InitializationException {
        // re-create the writer and set it to fail after 1 records are written
        final String writerId = "record-writer";
        writerService = new MockRecordWriter(null, true,1);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(PutTCPRecord.RECORD_WRITER, writerId);

        readerService.addRecord("alice", "25");
        readerService.addRecord("bob", "22");
        readerService.addRecord("john", "30");
        readerService.addRecord("mike", "35");

        // simulate retrying a failure where we already wrote two records
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier(), "2");

        runner.enqueue("trigger data", attributes);
        runner.run();
        runner.assertAllFlowFilesTransferred(PutTCPRecord.REL_FAILURE, 1);

        // verify that the successful count attribute was updated to 3
        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutTCPRecord.REL_FAILURE).get(0);
        mockFlowFile.assertAttributeEquals(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier(), "3");

        // verify the records were sent correctly
        final byte[] data = sender.getAllData();
        Assert.assertNotNull(data);
        Assert.assertTrue(data.length > 0);

        final String recordData = new String(data, StandardCharsets.UTF_8);
        final String[] records = recordData.split("[\n]");
        Assert.assertEquals(1, records.length);
        Assert.assertEquals("\"john\",\"30\"", records[0]);

        // verify we still reported a provenance SEND event with the correct uri
        Assert.assertEquals(1, runner.getProvenanceEvents().size());
        final ProvenanceEventRecord provEvent = runner.getProvenanceEvents().get(0);
        Assert.assertEquals(ProvenanceEventType.SEND, provEvent.getEventType());
        Assert.assertEquals("TCP://localhost:12345", provEvent.getTransitUri());
        Assert.assertEquals("Records 3 to 3", provEvent.getDetails());
    }

    @Test
    public void testAllFailed() throws InitializationException {
        // re-create the writer and set it to fail after 1 records are written
        final String writerId = "record-writer";
        writerService = new MockRecordWriter(null, true,0);
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);
        runner.setProperty(PutTCPRecord.RECORD_WRITER, writerId);

        readerService.addRecord("alice", "25");
        readerService.addRecord("bob", "22");
        readerService.addRecord("john", "30");
        readerService.addRecord("mike", "35");

        runner.enqueue("trigger data");
        runner.run();
        runner.assertAllFlowFilesTransferred(PutTCPRecord.REL_FAILURE, 1);

        // verify that the successful count attribute doesn't exist
        final MockFlowFile mockFlowFile = runner.getFlowFilesForRelationship(PutTCPRecord.REL_FAILURE).get(0);
        mockFlowFile.assertAttributeNotExists(PutTCPRecord.RECORD_SUCCESSFUL_COUNT + "." + proc.getIdentifier());

        // verify the records were sent correctly
        final byte[] data = sender.getAllData();
        Assert.assertNotNull(data);
        Assert.assertEquals(0, data.length);

        // verify we did not report a provenance event
        Assert.assertEquals(0, runner.getProvenanceEvents().size());
    }

    /**
     * Extend PutTCPRecord to use the CapturingChannelSender.
     */
    private static class TestablePutTCPRecord extends PutTCPRecord {

        private final ChannelSender sender;

        public TestablePutTCPRecord(final ChannelSender channelSender) {
            this.sender = channelSender;
        }

        @Override
        protected ChannelSender createSender(String protocol, String host, int port, int timeout, int maxSendBufferSize, SSLContext sslContext) throws IOException {
            return sender;
        }
    }

    /**
     * A ChannelSender that captures each message that was sent.
     */
    private static class CapturingChannelSender extends SocketChannelSender {

        private boolean closed = false;
        private ByteArrayOutputStream out = new ByteArrayOutputStream();

        public CapturingChannelSender(String host, int port, int maxSendBufferSize, ComponentLog logger) {
            super(host, port, maxSendBufferSize, logger);
        }

        @Override
        public void open() throws IOException {

        }

        @Override
        protected void write(byte[] data) throws IOException {
            out.write(data);
        }

        @Override
        public boolean isConnected() {
            return !closed;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public OutputStream getOutputStream() {
            return out;
        }

        public byte[] getAllData() {
            return out.toByteArray();
        }
    }

}
