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
package org.apache.nifi.processors.parquet.record;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * HDFSRecordWriter that writes Parquet files using Avro as the schema representation.
 */
public class AvroParquetHDFSRecordWriter implements HDFSRecordWriter {

    final Schema avroSchema;
    final ParquetWriter<GenericRecord> parquetWriter;

    public AvroParquetHDFSRecordWriter(final ParquetWriter<GenericRecord> parquetWriter, final Schema avroSchema) {
        this.avroSchema = avroSchema;
        this.parquetWriter = parquetWriter;
    }

    @Override
    public void write(final Record record) throws IOException {
        final GenericRecord genericRecord = createAvroRecord(record, avroSchema);
        parquetWriter.write(genericRecord);
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
    }

    // TODO all the code below was copied from code in nifi-record-serialization-services and needs to come from a common util

    private GenericRecord createAvroRecord(final Record record, final Schema avroSchema) throws IOException {
        final GenericRecord rec = new GenericData.Record(avroSchema);
        final RecordSchema recordSchema = record.getSchema();

        for (final String fieldName : recordSchema.getFieldNames()) {
            final Object rawValue = record.getValue(fieldName);

            final Schema.Field field = avroSchema.getField(fieldName);
            if (field == null) {
                continue;
            }

            final Object converted = convertToAvroObject(rawValue, field.schema());
            rec.put(fieldName, converted);
        }

        return rec;
    }

    private Object convertToAvroObject(final Object rawValue, final Schema fieldSchema) throws IOException {
        if (rawValue == null) {
            return null;
        }

        switch (fieldSchema.getType()) {
            case INT: {
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType == null) {
                    return DataTypeUtils.toInteger(rawValue);
                }

                if (LogicalTypes.date().getName().equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(new Date(0L).toInstant(), date.toInstant());
                    final long days = duration.toDays();
                    return (int) days;
                } else if (LogicalTypes.timeMillis().getName().equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
                    final long millisSinceMidnight = duration.toMillis();
                    return (int) millisSinceMidnight;
                }

                return DataTypeUtils.toInteger(rawValue);
            }
            case LONG: {
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType == null) {
                    return DataTypeUtils.toLong(rawValue);
                }

                if (LogicalTypes.timeMicros().getName().equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
                    return duration.toMillis() * 1000L;
                } else if (LogicalTypes.timestampMillis().getName().equals(logicalType.getName())) {
                    return DataTypeUtils.toLong(rawValue);
                } else if (LogicalTypes.timestampMicros().getName().equals(logicalType.getName())) {
                    return DataTypeUtils.toLong(rawValue) * 1000L;
                }

                return DataTypeUtils.toLong(rawValue);
            }
            case BYTES:
            case FIXED:
                if (rawValue instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) rawValue);
                }
                if (rawValue instanceof Object[]) {
                    return convertByteArray((Object[]) rawValue);
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a ByteBuffer");
                }
            case MAP:
                if (rawValue instanceof Record) {
                    final Record recordValue = (Record) rawValue;
                    final Map<String, Object> map = new HashMap<>();
                    for (final String recordFieldName : recordValue.getSchema().getFieldNames()) {
                        final Object v = recordValue.getValue(recordFieldName);
                        if (v != null) {
                            map.put(recordFieldName, v);
                        }
                    }

                    return map;
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a Map");
                }
            case RECORD:
                final GenericData.Record avroRecord = new GenericData.Record(fieldSchema);

                final Record record = (Record) rawValue;
                for (final String recordFieldName : record.getSchema().getFieldNames()) {
                    final Object recordFieldValue = record.getValue(recordFieldName);

                    final Schema.Field field = fieldSchema.getField(recordFieldName);
                    if (field == null) {
                        continue;
                    }

                    final Object converted = convertToAvroObject(recordFieldValue, field.schema());
                    avroRecord.put(recordFieldName, converted);
                }
                return avroRecord;
            case ARRAY:
                final Object[] objectArray = (Object[]) rawValue;
                final List<Object> list = new ArrayList<>(objectArray.length);
                for (final Object o : objectArray) {
                    final Object converted = convertToAvroObject(o, fieldSchema.getElementType());
                    list.add(converted);
                }
                return list;
            case BOOLEAN:
                return DataTypeUtils.toBoolean(rawValue);
            case DOUBLE:
                return DataTypeUtils.toDouble(rawValue);
            case FLOAT:
                return DataTypeUtils.toFloat(rawValue);
            case NULL:
                return null;
            case ENUM:
                return new GenericData.EnumSymbol(fieldSchema, rawValue);
            case STRING:
                return DataTypeUtils.toString(rawValue, RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat());
        }

        return rawValue;
    }

    public static ByteBuffer convertByteArray(final Object[] bytes) {
        final ByteBuffer bb = ByteBuffer.allocate(bytes.length);
        for (final Object o : bytes) {
            if (o instanceof Byte) {
                bb.put(((Byte) o).byteValue());
            } else {
                throw new IllegalTypeConversionException("Cannot convert value " + bytes + " of type " + bytes.getClass() + " to ByteBuffer");
            }
        }
        bb.flip();
        return bb;
    }

}
