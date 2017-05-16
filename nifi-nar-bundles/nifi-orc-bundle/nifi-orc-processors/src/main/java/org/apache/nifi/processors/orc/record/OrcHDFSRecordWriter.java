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
package org.apache.nifi.processors.orc.record;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.processors.orc.OrcTypeUtil;
import org.apache.nifi.serialization.record.Record;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;

public class OrcHDFSRecordWriter implements HDFSRecordWriter {

    private final Writer writer;
    private final TypeDescription schema;
    private final VectorizedRowBatch currRowBatch;

    public OrcHDFSRecordWriter(final Writer writer, final TypeDescription schema) {
        this(writer, schema, VectorizedRowBatch.DEFAULT_SIZE);
    }

    public OrcHDFSRecordWriter(final Writer writer, final TypeDescription schema, final int rowBatchMaxSize) {
        this.writer = writer;
        this.schema = schema;
        this.currRowBatch = schema.createRowBatch(rowBatchMaxSize);
        this.currRowBatch.reset();
    }

    @Override
    public void write(final Record record) throws IOException {
        if (currRowBatch.size == currRowBatch.getMaxSize()) {
            writer.addRowBatch(currRowBatch);
            currRowBatch.reset();
        }

        for (int i=0; i < currRowBatch.numCols; i++) {
            final String fieldName = schema.getFieldNames().get(i);
            final TypeDescription fieldType = schema.getChildren().get(i);
            final Object rawValue = record.getValue(fieldName);
            final ColumnVector columnVector = currRowBatch.cols[i];

            OrcTypeUtil.populateColumnVector(fieldName, fieldType, rawValue, columnVector, currRowBatch.size);
            currRowBatch.size++;
        }
    }

    @Override
    public void close() throws IOException {
        if (currRowBatch.size > 0) {
            writer.addRowBatch(currRowBatch);
            currRowBatch.reset();
        }
        writer.close();
    }

}
