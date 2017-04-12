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
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.processors.hadoop.record.HDFSRecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * HDFSRecordReader that reads Parquet files using Avro.
 */
public class AvroParquetHDFSRecordReader implements HDFSRecordReader {

    private GenericRecord lastRecord;
    private RecordSchema recordSchema;
    private boolean initialized = false;

    private final ParquetReader<GenericRecord> parquetReader;

    public AvroParquetHDFSRecordReader(final ParquetReader<GenericRecord> parquetReader) {
        this.parquetReader = parquetReader;
    }

    @Override
    public Record nextRecord() throws IOException {
        if (initialized && lastRecord == null) {
            return null;
        }

        lastRecord = parquetReader.read();
        initialized = true;

        if (lastRecord == null) {
            return null;
        }

        if (recordSchema == null) {
            recordSchema = createSchema(lastRecord.getSchema());
        }

        final Map<String, Object> values = convertAvroRecordToMap(lastRecord, recordSchema);
        return new MapRecord(recordSchema, values);
    }


    @Override
    public void close() throws IOException {
        parquetReader.close();
    }

    // TODO refactor this from AvroRecordReader to a common module

    private Map<String, Object> convertAvroRecordToMap(final GenericRecord avroRecord, final RecordSchema recordSchema) {
        final Map<String, Object> values = new HashMap<>(recordSchema.getFieldCount());

        for (final String fieldName : recordSchema.getFieldNames()) {
            final Object value = avroRecord.get(fieldName);

            final Schema.Field avroField = avroRecord.getSchema().getField(fieldName);
            if (avroField == null) {
                values.put(fieldName, null);
                continue;
            }

            final Schema fieldSchema = avroField.schema();
            final Object rawValue = normalizeValue(value, fieldSchema);

            final DataType desiredType = recordSchema.getDataType(fieldName).get();
            final Object coercedValue = DataTypeUtils.convertType(rawValue, desiredType);

            values.put(fieldName, coercedValue);
        }

        return values;
    }

    private Object normalizeValue(final Object value, final Schema avroSchema) {
        if (value == null) {
            return null;
        }

        switch (avroSchema.getType()) {
            case INT: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return value;
                }

                final String logicalName = logicalType.getName();
                if (LogicalTypes.date().getName().equals(logicalName)) {
                    // date logical name means that the value is number of days since Jan 1, 1970
                    return new java.sql.Date(TimeUnit.DAYS.toMillis((int) value));
                } else if (LogicalTypes.timeMillis().equals(logicalName)) {
                    // time-millis logical name means that the value is number of milliseconds since midnight.
                    return new java.sql.Time((int) value);
                }

                break;
            }
            case LONG: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return value;
                }

                final String logicalName = logicalType.getName();
                if (LogicalTypes.timeMicros().getName().equals(logicalName)) {
                    return new java.sql.Time(TimeUnit.MICROSECONDS.toMillis((long) value));
                } else if (LogicalTypes.timestampMillis().getName().equals(logicalName)) {
                    return new java.sql.Timestamp((long) value);
                } else if (LogicalTypes.timestampMicros().getName().equals(logicalName)) {
                    return new java.sql.Timestamp(TimeUnit.MICROSECONDS.toMillis((long) value));
                }
                break;
            }
            case UNION:
                if (value instanceof GenericData.Record) {
                    final GenericData.Record avroRecord = (GenericData.Record) value;
                    return normalizeValue(value, avroRecord.getSchema());
                }
                break;
            case RECORD:
                final GenericData.Record record = (GenericData.Record) value;
                final Schema recordSchema = record.getSchema();
                final List<Schema.Field> recordFields = recordSchema.getFields();
                final Map<String, Object> values = new HashMap<>(recordFields.size());
                for (final Schema.Field field : recordFields) {
                    final Object avroFieldValue = record.get(field.name());
                    final Object fieldValue = normalizeValue(avroFieldValue, field.schema());
                    values.put(field.name(), fieldValue);
                }
                final RecordSchema childSchema = createSchema(recordSchema);
                return new MapRecord(childSchema, values);
            case BYTES:
                final ByteBuffer bb = (ByteBuffer) value;
                return convertByteArray(bb.array());
            case FIXED:
                final GenericFixed fixed = (GenericFixed) value;
                return convertByteArray(fixed.bytes());
            case ENUM:
                return value.toString();
            case NULL:
                return null;
            case STRING:
                return value.toString();
            case ARRAY:
                final GenericData.Array<?> array = (GenericData.Array<?>) value;
                final Object[] valueArray = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    final Schema elementSchema = avroSchema.getElementType();
                    valueArray[i] = normalizeValue(array.get(i), elementSchema);
                }
                return valueArray;
            case MAP:
                final Map<?, ?> avroMap = (Map<?, ?>) value;
                final Map<String, Object> map = new HashMap<>(avroMap.size());
                for (final Map.Entry<?, ?> entry : avroMap.entrySet()) {
                    Object obj = entry.getValue();
                    if (obj instanceof Utf8 || obj instanceof CharSequence) {
                        obj = obj.toString();
                    }

                    final String key = entry.getKey().toString();
                    obj = normalizeValue(obj, avroSchema.getValueType());

                    map.put(key, obj);
                }

                final DataType elementType = determineDataType(avroSchema.getValueType());
                final List<RecordField> mapFields = new ArrayList<>();
                for (final String key : map.keySet()) {
                    mapFields.add(new RecordField(key, elementType));
                }
                final RecordSchema mapSchema = new SimpleRecordSchema(mapFields);
                return new MapRecord(mapSchema, map);
        }

        return value;
    }

    public static DataType determineDataType(final Schema avroSchema) {
        final Schema.Type avroType = avroSchema.getType();

        switch (avroType) {
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case ARRAY:
                final DataType elementType = determineDataType(avroSchema.getElementType());
                return RecordFieldType.ARRAY.getArrayDataType(elementType);
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case INT: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return RecordFieldType.INT.getDataType();
                }

                if (LogicalTypes.date().getName().equals(logicalType.getName())) {
                    return RecordFieldType.DATE.getDataType();
                } else if (LogicalTypes.timeMillis().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIME.getDataType();
                }

                return RecordFieldType.INT.getDataType();
            }
            case LONG: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return RecordFieldType.LONG.getDataType();
                }

                if (LogicalTypes.timestampMillis().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIMESTAMP.getDataType();
                } else if (LogicalTypes.timestampMicros().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIMESTAMP.getDataType();
                } else if (LogicalTypes.timeMicros().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIME.getDataType();
                }

                return RecordFieldType.LONG.getDataType();
            }
            case RECORD: {
                final List<Schema.Field> avroFields = avroSchema.getFields();
                final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                for (final Schema.Field field : avroFields) {
                    final String fieldName = field.name();
                    final Schema fieldSchema = field.schema();
                    final DataType fieldType = determineDataType(fieldSchema);
                    recordFields.add(new RecordField(fieldName, fieldType));
                }

                final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            }
            case NULL:
            case MAP:
                return RecordFieldType.RECORD.getDataType();
            case UNION: {
                final List<Schema> nonNullSubSchemas = avroSchema.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .collect(Collectors.toList());

                if (nonNullSubSchemas.size() == 1) {
                    return determineDataType(nonNullSubSchemas.get(0));
                }

                final List<DataType> possibleChildTypes = new ArrayList<>(nonNullSubSchemas.size());
                for (final Schema subSchema : nonNullSubSchemas) {
                    final DataType childDataType = determineDataType(subSchema);
                    possibleChildTypes.add(childDataType);
                }

                return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
            }
        }

        return null;
    }

    public static RecordSchema createSchema(final Schema avroSchema) {
        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Schema.Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final DataType dataType = determineDataType(field.schema());
            recordFields.add(new RecordField(fieldName, dataType));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
        return recordSchema;
    }

    public static Object[] convertByteArray(final byte[] bytes) {
        final Object[] array = new Object[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            array[i] = Byte.valueOf(bytes[i]);
        }
        return array;
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
