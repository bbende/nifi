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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.IntervalDayTimeColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MultiValuedColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to convert between Apache ORC and NiFi's Record abstraction.
 */
public class OrcTypeUtil {

    public static final String ORC_SCHEMA_FORMAT = "orc";

    /**
     * Converts the given ORC Schema (TypeDescription) to a NiFi RecordSchema.
     *
     * @param orcSchema
     * @return
     */
    public static RecordSchema createRecordSchema(final TypeDescription orcSchema) {
        if (orcSchema == null) {
            throw new IllegalArgumentException("ORC Schema cannot be null");
        }

        final RecordSchema recordSchema = createStructRecordSchema(orcSchema);
        return recordSchema;
    }

    /**
     * Converts the given ORC Struct to a NiFi RecordSchema.
     *
     * @param orcSchema the ORC struct
     * @return the NiFi RecordSchema
     */
    private static RecordSchema createStructRecordSchema(final TypeDescription orcSchema) {
        if (!orcSchema.getCategory().equals(TypeDescription.Category.STRUCT)) {
            throw new IllegalArgumentException("Category must be STRUCT, but was " + orcSchema.getCategory().getName());
        }

        final List<String> orcFieldNames = orcSchema.getFieldNames();
        final List<TypeDescription> orcFieldTypes = orcSchema.getChildren();

        if (orcFieldNames.size() != orcFieldTypes.size()) {
            throw new IllegalStateException("STRUCT must have the same number of field names and child types");
        }

        final List<RecordField> recordFields = new ArrayList<>();

        for (int i=0; i < orcFieldNames.size(); i++) {
            final String orcFieldName = orcFieldNames.get(i);
            final TypeDescription orcFieldType = orcFieldTypes.get(i);

            final DataType dataType = determineDataType(orcFieldType);
            recordFields.add(new RecordField(orcFieldName, dataType));
        }

        return new SimpleRecordSchema(recordFields, orcSchema.toJson(), ORC_SCHEMA_FORMAT, SchemaIdentifier.EMPTY);
    }

    /**
     * Converts the given ORC Schema to it's corresponding NiFi DataType.
     *
     * @param orcSchema the ORC schema
     * @return the NiFi DataType
     */
    private static DataType determineDataType(final TypeDescription orcSchema) {
        switch (orcSchema.getCategory()) {
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case BYTE:
                return RecordFieldType.BYTE.getDataType();
            case SHORT:
                return RecordFieldType.SHORT.getDataType();
            case INT:
                return RecordFieldType.INT.getDataType();
            case LONG:
                return RecordFieldType.LONG.getDataType();
            case DATE:
                return RecordFieldType.DATE.getDataType();
            case TIMESTAMP:
                return RecordFieldType.TIMESTAMP.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case DOUBLE:
            case DECIMAL:
                return RecordFieldType.DOUBLE.getDataType();
            case STRING:
            case VARCHAR:
                return RecordFieldType.STRING.getDataType();
            case BINARY:
                return RecordFieldType.BYTE.getDataType();
            case CHAR:
                return RecordFieldType.CHAR.getDataType();
            case STRUCT:
                final RecordSchema recordSchema = createStructRecordSchema(orcSchema);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            case UNION:
                final List<DataType> choiceDataTypes = new ArrayList<>();
                for (final TypeDescription orcUnionType : orcSchema.getChildren()) {
                    choiceDataTypes.add(determineDataType(orcUnionType));
                }
                return RecordFieldType.CHOICE.getChoiceDataType(choiceDataTypes);
            case LIST:
                final List<DataType> listTypes = new ArrayList<>();
                for (final TypeDescription orcUnionType : orcSchema.getChildren()) {
                    listTypes.add(determineDataType(orcUnionType));
                }
                return RecordFieldType.ARRAY.getChoiceDataType(listTypes);
            case MAP:
                if (orcSchema.getChildren().size() < 2) {
                    throw new IllegalStateException("Map must have key and value as children");
                }

                final DataType keyType = determineDataType(orcSchema.getChildren().get(0));
                if (!RecordFieldType.STRING.equals(keyType.getFieldType())) {
                    throw new IllegalStateException("Map key type must be String, but was " + keyType.getFieldType().name());
                }

                final DataType valueType = determineDataType(orcSchema.getChildren().get(1));
                return RecordFieldType.MAP.getMapDataType(valueType);
            default:
                throw new IllegalArgumentException("Unknown type: " + orcSchema.getCategory().getName());
        }
    }

    /**
     * Converts the given NiFi RecordSchema to an ORC schema.
     *
     * @param recordSchema the NiFi RecordSchema
     * @return the corresponding ORC schema
     * @throws SchemaNotFoundException if RecordSchema is null
     */
    public static TypeDescription createORCSchema(final RecordSchema recordSchema) throws SchemaNotFoundException {
        if (recordSchema == null) {
            throw new IllegalArgumentException("RecordSchema cannot be null");
        }

        final TypeDescription struct = TypeDescription.createStruct();

        for (final RecordField recordField : recordSchema.getFields()) {
            final DataType dataType = recordField.getDataType();
            struct.addField(recordField.getFieldName(), getOrcField(dataType));
        }

        return struct;
    }

    /**
     * Converts the given NiFi DataType to an ORC type.
     *
     * @param dataType the NiFi DataType
     * @return the ORC schema
     * @throws SchemaNotFoundException if a Record type doesn't provide a RecordSchema
     */
    private static TypeDescription getOrcField(final DataType dataType) throws SchemaNotFoundException {
        final RecordFieldType fieldType = dataType.getFieldType();

        switch (fieldType) {
            case STRING:
                return TypeDescription.createString();
            case BOOLEAN:
                return TypeDescription.createBoolean();
            case BYTE:
                return TypeDescription.createByte();
            case CHAR:
                return TypeDescription.createChar();
            case SHORT:
                return TypeDescription.createShort();
            case INT:
                return TypeDescription.createInt();
            case BIGINT:
                return TypeDescription.createString();
            case LONG:
                return TypeDescription.createLong();
            case FLOAT:
                return TypeDescription.createFloat();
            case DOUBLE:
                return TypeDescription.createDouble();
            case DATE:
                return TypeDescription.createDate();
            case TIME:
            case TIMESTAMP:
                return TypeDescription.createTimestamp();
            case RECORD:
                final RecordDataType recordDataType = (RecordDataType) dataType;
                final RecordSchema recordSchema = recordDataType.getChildSchema();
                return createORCSchema(recordSchema);
            case CHOICE:
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                final List<DataType> subTypes = choiceDataType.getPossibleSubTypes();

                final TypeDescription orcUnionType = TypeDescription.createUnion();
                for (final DataType subType : subTypes) {
                    orcUnionType.addUnionChild(getOrcField(subType));
                }
                return orcUnionType;
            case ARRAY:
                final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                final DataType elementType = arrayDataType.getElementType();

                final TypeDescription elementOrcType = getOrcField(elementType);
                return TypeDescription.createList(elementOrcType);
            case MAP:
                final MapDataType mapDataType = (MapDataType) dataType;
                final DataType valueDataType = mapDataType.getValueType();

                final TypeDescription orcMapKeyType = TypeDescription.createString();
                final TypeDescription orcMapValueType = getOrcField(valueDataType);
                return TypeDescription.createMap(orcMapKeyType, orcMapValueType);
            default:
                throw new IllegalArgumentException("Did not recognize DataType: " + dataType.toString());
        }
    }


    public static void populateColumnVector(final String fieldName,
                                            final TypeDescription fieldType,
                                            final Object rawValue,
                                            final ColumnVector columnVector,
                                            final int rowIndex) {
        if (columnVector instanceof BytesColumnVector) {
            final BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
            // TODO

        } else if (columnVector instanceof DecimalColumnVector) {
            final DecimalColumnVector decimalColumnVector = (DecimalColumnVector) columnVector;
            // TODO

        } else if (columnVector instanceof DoubleColumnVector) {
            final Double doubleValue = DataTypeUtils.toDouble(rawValue, fieldName);
            final DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
            doubleColumnVector.vector[rowIndex] = doubleValue;

        } else if (columnVector instanceof ListColumnVector) {
            final ListColumnVector listColumnVector = (ListColumnVector) columnVector;
            // TODO

        } else if (columnVector instanceof LongColumnVector) {
            final Long longValue = DataTypeUtils.toLong(rawValue, fieldName);
            final LongColumnVector longColumnVector = (LongColumnVector) columnVector;
            longColumnVector.vector[rowIndex] = longValue;

        } else if (columnVector instanceof MapColumnVector) {
            final MapColumnVector mapColumnVector = (MapColumnVector) columnVector;
            // TODO

        } else if (columnVector instanceof StructColumnVector) {
            final StructColumnVector structColumnVector = (StructColumnVector) columnVector;
            //TODO

        } else if (columnVector instanceof TimestampColumnVector) {
            final TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columnVector;
            // TODO

        } else if (columnVector instanceof UnionColumnVector) {
            final UnionColumnVector unionColumnVector = (UnionColumnVector) columnVector;
            // TODO

        } else {
            throw new IllegalStateException("Unknown type of ColumnVector: " + columnVector.getClass().getName());
        }
    }


    private static final Object convertToORCValue(final String fieldName, final TypeDescription fieldType, final Object rawValue) {
        switch (fieldType.getCategory()) {
            case BOOLEAN:
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return DataTypeUtils.toLong(rawValue, fieldName);
            case TIMESTAMP:

            case DOUBLE:
            case FLOAT:
                return DataTypeUtils.toDouble(rawValue, fieldName);
            case DECIMAL:
                //
            case STRING:
            case BINARY:
            case CHAR:
            case VARCHAR:
                return RecordFieldType.BYTE.getDataType(); // TODO need BYTES
            case STRUCT:
                final RecordSchema recordSchema = createStructRecordSchema(fieldType);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            case UNION:
                final List<DataType> choiceDataTypes = new ArrayList<>();
                for (final TypeDescription orcUnionType : fieldType.getChildren()) {
                    choiceDataTypes.add(determineDataType(orcUnionType));
                }
                return RecordFieldType.CHOICE.getChoiceDataType(choiceDataTypes);
            case LIST:
                final List<DataType> listTypes = new ArrayList<>();
                for (final TypeDescription orcUnionType : fieldType.getChildren()) {
                    listTypes.add(determineDataType(orcUnionType));
                }
                return RecordFieldType.ARRAY.getChoiceDataType(listTypes);
            case MAP:
                if (fieldType.getChildren().size() < 2) {
                    throw new IllegalStateException("Map must have key and value as children");
                }

                final DataType keyType = determineDataType(fieldType.getChildren().get(0));
                if (!RecordFieldType.STRING.equals(keyType.getFieldType())) {
                    throw new IllegalStateException("Map key type must be String, but was " + keyType.getFieldType().name());
                }

                final DataType valueType = determineDataType(fieldType.getChildren().get(1));
                return RecordFieldType.MAP.getMapDataType(valueType);
            default:
                throw new IllegalArgumentException("Unknown type: " + fieldType.getCategory().getName());
        }
    };
}
