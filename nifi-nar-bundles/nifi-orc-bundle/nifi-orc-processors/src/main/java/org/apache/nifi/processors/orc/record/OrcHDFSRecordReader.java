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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.nifi.processors.hadoop.record.HDFSRecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;

public class OrcHDFSRecordReader implements HDFSRecordReader {

    private final Reader orcReader;

    private RecordReader rows;
    private VectorizedRowBatch batch;
    private int currRow = -1;
    private boolean hasNextBatch = false;
    private boolean closed = false;

    public OrcHDFSRecordReader(final Reader orcReader) throws IOException {
        this.orcReader = orcReader;
        this.rows = orcReader.rows();
        this.batch = orcReader.getSchema().createRowBatch();
    }

    @Override
    public Record nextRecord() throws IOException {
        if (closed) {
            throw new IOException("This reader has already been closed.");
        }

        // first time through so initialize and read the first batch
        if (currRow < 0) {
            hasNextBatch = rows.nextBatch(batch);
            currRow = 0;
        }

        // if we reached the end of a batch and there are more batches, then read the next batch and reset the row count
        if (currRow >= batch.size && hasNextBatch) {
            hasNextBatch = rows.nextBatch(batch);
            currRow = 0;
        }

        if (currRow < batch.size) {
            for (int i=0; i < batch.numCols; i++) {
                //batch.cols[i];
            }
            
            // TODO get the row and convert to a Record
            currRow++;
            return null;
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        try{
            if (rows != null) {
                rows.close();
                rows = null;
            }
        } finally {
            closed = true;
        }
    }

}
