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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.put.AbstractPutEventProcessor;
import org.apache.nifi.processor.util.put.sender.ChannelSender;
import org.apache.nifi.processor.util.put.sender.SocketChannelSender;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.NonCloseableOutputStream;
import org.apache.nifi.util.StopWatch;

import javax.net.ssl.SSLContext;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"remote", "egress", "put", "tcp", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Reads records from an incoming flow file using a configured record reader, and writes the records " +
        "to a TCP connection using a configured record writer. Depending on the type of data being sent and the behavior of " +
        "the destination, using a connection per flow file may be required. For example, when using PutTCPRecord with a JSON " +
        "writer to send to a ListenTCPRecord with a JSON reader, each flow file will be written as a JSON array, and the " +
        "reader will only be able to read one array per connection.")
@TriggerWhenEmpty // trigger when empty so we have a chance to close idle connections
@DefaultSettings(yieldDuration = "100 ms") // decrease the default yield since we are triggering when empty
@ReadsAttribute(attribute = "record.successful.count.<processor-id>", description = "The number of records sent successfully during previous " +
        "processing of this flow file. If present, the processor will start sending records after skipping over the number of records already " +
        "sent successfully. If not present, all records will be sent starting from the beginning.")
@WritesAttribute(attribute = "record.successful.count.<processor-id>", description = "Contains the number of records that were sent successfully " +
        "before encountering an error. If all records were sent successfully this attribute will not be present.")
public class PutTCPRecord extends AbstractPutEventProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before writing to a FlowFile")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(false)
            .required(true)
            .build();

    static final String RECORD_SUCCESSFUL_COUNT = "record.successful.count";

    private volatile String recordSuccessfulCountAttribute;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        this.recordSuccessfulCountAttribute = RECORD_SUCCESSFUL_COUNT + "." + getIdentifier();
    }

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                new PropertyDescriptor.Builder()
                        .fromPropertyDescriptor(CONNECTION_PER_FLOWFILE)
                        .defaultValue("true")
                        .build(),
                TIMEOUT,
                RECORD_READER,
                RECORD_WRITER,
                SSL_CONTEXT_SERVICE
        );
    }

    @Override
    protected ChannelSender createSender(final ProcessContext context) throws IOException {
        final String protocol = TCP_VALUE.getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        final int timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final int bufferSize = context.getProperty(MAX_SOCKET_SEND_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final SSLContextService sslContextService = (SSLContextService) context.getProperty(SSL_CONTEXT_SERVICE).asControllerService();

        SSLContext sslContext = null;
        if (sslContextService != null) {
            sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
        }

        return createSender(protocol, hostname, port, timeout, bufferSize, sslContext);
    }

    @Override
    protected String createTransitUri(final ProcessContext context) {
        final String protocol = TCP_VALUE.getValue();
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();

        return new StringBuilder().append(protocol).append("://").append(host).append(":").append(port).toString();
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            pruneIdleSenders(context.getProperty(IDLE_EXPIRATION).asTimePeriod(TimeUnit.MILLISECONDS).longValue());
            context.yield();
            return;
        }

        final ChannelSender sender = acquireSender(context, session, flowFile);
        if (sender == null) {
            return;
        }

        // really shouldn't happen since we know the protocol is TCP here, but this is more graceful so we
        // can cast to a SocketChannelSender later in order to obtain the OutputStream
        if (!(sender instanceof SocketChannelSender)) {
            getLogger().error("Processor can only be used with a SocketChannelSender, but obtained: " + sender.getClass().getCanonicalName());
            context.yield();
            return;
        }

        int recordCount = 0;
        int successfulRecordCount = 0;
        final int retryIndex = getRetryIndex(flowFile);
        final StopWatch stopWatch = new StopWatch(false);
        final boolean closeConnection = context.getProperty(CONNECTION_PER_FLOWFILE).asBoolean();
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        try {
            // need this inner try so that the FlowFile InputStream can be closed before session.commit()
            try (final InputStream rawIn = session.read(flowFile);
                 final BufferedInputStream in = new BufferedInputStream(rawIn);
                 final RecordReader recordReader = readerFactory.createRecordReader(flowFile, in, getLogger())) {

                final RecordSet recordSet = recordReader.createRecordSet();
                final RecordSchema writeSchema = writerFactory.getSchema(flowFile, recordSet.getSchema());

                // we don't want the output stream to close when we close the writer, only if/when we actually close the sender
                final OutputStream out = new NonCloseableOutputStream(((SocketChannelSender)sender).getOutputStream());

                Record record;
                stopWatch.start();
                try (final RecordSetWriter recordWriter = writerFactory.createWriter(getLogger(), writeSchema, flowFile, out)) {
                    recordWriter.beginRecordSet();
                    while ((record = recordSet.next()) != null) {
                        // if there was a retryIndex we only want to start sending from there
                        if (recordCount >= retryIndex) {
                            try {
                                recordWriter.write(record);
                                recordWriter.flush();
                                successfulRecordCount++;
                            } catch (final Exception e) {
                                // since we already call beginRecordSet, attempt to call finish so that any records
                                // written up to this point will make a valid set, then throw the original exception
                                try {
                                    recordWriter.finishRecordSet();
                                } catch (final Exception frse) {
                                    getLogger().error(frse.getMessage(), frse);
                                }
                                // we want to throw the original exception that got us into the catch block
                                throw e;
                            }
                        }
                        recordCount++;
                    }
                    recordWriter.finishRecordSet();
                }
                stopWatch.stop();
            }

            final FlowFile successFlowFile = session.removeAttribute(flowFile, recordSuccessfulCountAttribute);
            reportProvenanceSendEvent(session, successFlowFile, transitUri, retryIndex, successfulRecordCount, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(successFlowFile, REL_SUCCESS);
            session.commit();

        } catch (Exception e) {
            getLogger().error("Exception while processing records, transferring {} to failure.", new Object[] {flowFile}, e);
            FlowFile failureFlowFile = flowFile;
            if (recordCount > 0) {
                failureFlowFile = session.putAttribute(flowFile, recordSuccessfulCountAttribute, String.valueOf(retryIndex + successfulRecordCount));
                reportProvenanceSendEvent(session, failureFlowFile, transitUri, retryIndex, successfulRecordCount, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            }
            session.transfer(session.penalize(failureFlowFile), REL_FAILURE);
            session.commit();
            context.yield();
        } finally {
            // If we are going to use this sender again, then relinquish it back to the pool.
            if (closeConnection) {
                sender.close();
            } else {
                relinquishSender(sender);
            }
        }
    }

    private void reportProvenanceSendEvent(final ProcessSession session, final FlowFile flowFile, String transitUri, int startIndex, int recordCount, long duration) {
        final String details = "Records " + (startIndex + 1) + " to " + (startIndex + recordCount);
        session.getProvenanceReporter().send(flowFile, transitUri, details, duration);
    }

    private int getRetryIndex(final FlowFile flowFile) {
        int retryIndex = 0;
        final String retryIndexAttr = flowFile.getAttribute(recordSuccessfulCountAttribute);
        if (retryIndexAttr != null) {
            retryIndex = Integer.parseInt(retryIndexAttr);
        }
        return retryIndex;
    }

}
