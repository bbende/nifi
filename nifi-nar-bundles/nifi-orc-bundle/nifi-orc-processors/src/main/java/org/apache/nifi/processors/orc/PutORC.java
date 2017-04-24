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
package org.apache.nifi.processors.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.exception.InvalidSchemaException;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;

import java.io.IOException;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "ORC", "hadoop", "HDFS", "filesystem", "restricted"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to an ORC file. The schema for the ORC file must be provided in the processor properties. This processor will " +
        "first write a temporary dot file and upon successfully writing every record to the dot file, it will rename the " +
        "dot file to it's final name. If any error occurs while reading records from the input, or writing records to the output, " +
        "the entire dot file will be removed and the flow file will be routed to failure or retry, depending on the error.")
@ReadsAttribute(attribute = "filename", description = "The name of the file to write comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file is stored in this attribute.")
})
@SeeAlso({FetchORC.class})
@Restricted("Provides operator the ability to write to any file that NiFi has access to in HDFS or the local filesystem.")
public class PutORC extends AbstractPutHDFSRecord {


    @Override
    public AllowableValue[] getDestinationSchemaTypes(ProcessorInitializationContext context) {
        return new AllowableValue[0];
    }

    @Override
    public String getDefaultDestinationSchemaType(ProcessorInitializationContext context) {
        return null;
    }

    @Override
    public AllowableValue[] getCompressionTypes(ProcessorInitializationContext context) {
        return new AllowableValue[0];
    }

    @Override
    public String getDefaultCompressionType(ProcessorInitializationContext context) {
        return null;
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf,
                                                   final Path path, final String schema)
            throws IOException, InvalidSchemaException {
        return null;
    }

}
