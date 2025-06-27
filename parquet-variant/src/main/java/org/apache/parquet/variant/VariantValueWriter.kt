/*
 * Copyright (c) The Apache Software Foundation.
 * Copyright (c) GAHOJIN, Inc.
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.variant

import org.apache.parquet.Preconditions
import org.apache.parquet.io.api.Binary.Companion.fromConstantByteArray
import org.apache.parquet.io.api.Binary.Companion.fromConstantByteBuffer
import org.apache.parquet.io.api.Binary.Companion.fromReusedByteBuffer
import org.apache.parquet.io.api.Binary.Companion.fromString
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import java.nio.ByteBuffer

/**
 * Class to write Variant values to a shredded schema.
 */
class VariantValueWriter internal constructor(
    private val recordConsumer: RecordConsumer,
    private val metadataBuffer: ByteBuffer
) {
    // We defer initializing the ImmutableMetata until it's needed. There is a construction cost to deserialize the
    // metadata binary into a Map, and if all object fields are shredded into typed_value, it will never be used.
    var metadata: ImmutableMetadata? = null
        get() {
            if (field == null) {
                field = ImmutableMetadata(metadataBuffer)
            }
            return field
        }
        private set

    /**
     * Write a Variant value to a shredded schema. The caller is responsible for calling startGroup()
     * and endGroup(), and writing metadata if this is the top level of the Variant group.
     */
    fun write(schema: GroupType, value: Variant) {
        var typedValueField: Type? = null
        if (schema.containsField("typed_value")) {
            typedValueField = schema.getType("typed_value")
        }

        val variantType = value.type

        // Handle typed_value if present
        if (isTypeCompatible(variantType, typedValueField, value)) {
            val typedValueIdx = schema.getFieldIndex("typed_value")
            recordConsumer.startField("typed_value", typedValueIdx)
            var residual: ByteBuffer? = null
            if (typedValueField!!.isPrimitive) {
                writeScalarValue(value)
            } else if (typedValueField.logicalTypeAnnotation is ListLogicalTypeAnnotation) {
                writeArrayValue(value, typedValueField.asGroupType())
            } else {
                residual = writeObjectValue(value, typedValueField.asGroupType())
            }
            recordConsumer.endField("typed_value", typedValueIdx)

            if (residual != null) {
                val valueIdx = schema.getFieldIndex("value")
                recordConsumer.startField("value", valueIdx)
                recordConsumer.addBinary(fromConstantByteBuffer(residual))
                recordConsumer.endField("value", valueIdx)
            }
        } else {
            val valueIdx = schema.getFieldIndex("value")
            recordConsumer.startField("value", valueIdx)
            recordConsumer.addBinary(fromReusedByteBuffer(value.value))
            recordConsumer.endField("value", valueIdx)
        }
    }

    private fun writeScalarValue(variant: Variant) {
        when (variant.type) {
            Variant.Type.BOOLEAN -> recordConsumer.addBoolean(variant.boolean)
            Variant.Type.BYTE -> recordConsumer.addInteger(variant.byte.toInt())
            Variant.Type.SHORT -> recordConsumer.addInteger(variant.short.toInt())
            Variant.Type.INT -> recordConsumer.addInteger(variant.int)
            Variant.Type.LONG -> recordConsumer.addLong(variant.long)
            Variant.Type.FLOAT -> recordConsumer.addFloat(variant.float)
            Variant.Type.DOUBLE -> recordConsumer.addDouble(variant.double)
            Variant.Type.DECIMAL4 -> recordConsumer.addInteger(variant.decimal.unscaledValue().toInt())
            Variant.Type.DECIMAL8 -> recordConsumer.addLong(variant.decimal.unscaledValue().toLong())
            Variant.Type.DECIMAL16 -> recordConsumer.addBinary(
                fromConstantByteArray(variant.decimal.unscaledValue().toByteArray())
            )

            Variant.Type.DATE -> recordConsumer.addInteger(variant.int)
            Variant.Type.TIME -> recordConsumer.addLong(variant.long)
            Variant.Type.TIMESTAMP_TZ -> recordConsumer.addLong(variant.long)
            Variant.Type.TIMESTAMP_NTZ -> recordConsumer.addLong(variant.long)
            Variant.Type.TIMESTAMP_NANOS_TZ -> recordConsumer.addLong(variant.long)
            Variant.Type.TIMESTAMP_NANOS_NTZ -> recordConsumer.addLong(variant.long)
            Variant.Type.STRING -> recordConsumer.addBinary(fromString(variant.string))
            Variant.Type.BINARY -> recordConsumer.addBinary(fromReusedByteBuffer(variant.binary))
            else -> throw IllegalArgumentException("Unsupported scalar type: ${variant.type}")
        }
    }

    private fun writeArrayValue(variant: Variant, arrayType: GroupType) {
        require(variant.type == Variant.Type.ARRAY) {
            "Cannot write variant type ${variant.type} as array"
        }

        // Validate that it's a 3-level array.
        require(
            !(arrayType.fieldCount != 1 || arrayType.repetition === Type.Repetition.REPEATED || arrayType.getType(0).isPrimitive
                    || (arrayType.getFieldName(0) != LIST_REPEATED_NAME))) {
            "Variant list must be a three-level list structure: $arrayType"
        }

        // Get the element type from the array schema
        val repeatedType = arrayType.getType(0).asGroupType()

        require(
            !(repeatedType.fieldCount != 1 || repeatedType.repetition !== Type.Repetition.REPEATED || repeatedType.getType(0).isPrimitive
                    || (repeatedType.getFieldName(0) != LIST_ELEMENT_NAME))) {
            "Variant list must be a three-level list structure: $arrayType"
        }

        val elementType = repeatedType.getType(0).asGroupType()

        // List field, annotated as LIST
        recordConsumer.startGroup()
        val numElements = variant.numArrayElements()
        // Can only call startField if there is at least one element.
        if (numElements > 0) {
            recordConsumer.startField(LIST_REPEATED_NAME, 0)
            // Write each array element
            for (i in 0..<numElements) {
                // Repeated group.
                recordConsumer.startGroup()
                recordConsumer.startField(LIST_ELEMENT_NAME, 0)

                // Element group. Can never be null for shredded Variant.
                recordConsumer.startGroup()
                write(elementType, variant.getElementAtIndex(i)!!)
                recordConsumer.endGroup()

                recordConsumer.endField(LIST_ELEMENT_NAME, 0)
                recordConsumer.endGroup()
            }
            recordConsumer.endField(LIST_REPEATED_NAME, 0)
        }
        recordConsumer.endGroup()
    }

    /**
     * Write an object to typed_value
     *
     * @return the residual value that must be written to the value column, or null if all values were written
     * to typed_value.
     */
    private fun writeObjectValue(variant: Variant, objectType: GroupType): ByteBuffer? {
        require(variant.type == Variant.Type.OBJECT) {
            "Cannot write variant type ${variant.type} as object"
        }

        var residualBuilder: VariantBuilder? = null
        // The residualBuilder, if created, is always a single object. This is that object's builder.
        var objectBuilder: VariantObjectBuilder? = null

        // Write each object field.
        recordConsumer.startGroup()
        for (i in 0..<variant.numObjectElements()) {
            val field = variant.getFieldAtIndex(i)

            if (objectType.containsField(field.key)) {
                val fieldIndex = objectType.getFieldIndex(field.key)
                val fieldType = objectType.getType(fieldIndex)

                recordConsumer.startField(field.key, fieldIndex)
                recordConsumer.startGroup()
                write(fieldType.asGroupType(), field.value)
                recordConsumer.endGroup()
                recordConsumer.endField(field.key, objectType.getFieldIndex(field.key))
            } else {
                if (residualBuilder == null) {
                    residualBuilder = VariantBuilder(this.metadata!!)
                    objectBuilder = residualBuilder.startObject()
                }
                objectBuilder!!.appendKey(field.key)
                objectBuilder.appendEncodedValue(field.value.value)
            }
        }
        recordConsumer.endGroup()

        return residualBuilder?.let {
            it.endObject()
            it.build().value
        }
    }

    companion object {
        private const val LIST_REPEATED_NAME = "list"
        private const val LIST_ELEMENT_NAME = "element"

        /**
         * Write a Variant value to a shredded schema.
         */
        fun write(recordConsumer: RecordConsumer, schema: GroupType, value: Variant) {
            recordConsumer.startGroup()
            val metadataIndex = schema.getFieldIndex("metadata")
            recordConsumer.startField("metadata", metadataIndex)
            recordConsumer.addBinary(fromConstantByteBuffer(value.metadata))
            recordConsumer.endField("metadata", metadataIndex)
            val writer = VariantValueWriter(recordConsumer, value.metadata)
            writer.write(schema, value)
            recordConsumer.endGroup()
        }

        // Return true if the logical type is a decimal with the same scale as the provided value, with enough
        // precision to hold the value. The provided value must be a decimal.
        private fun compatibleDecimalType(value: Variant, logicalType: LogicalTypeAnnotation?): Boolean {
            if (logicalType !is DecimalLogicalTypeAnnotation) {
                return false
            }
            val decimalType =
                logicalType

            val decimal = value.decimal
            return decimal.scale() == decimalType.scale && decimal.precision() <= decimalType.precision
        }

        private fun isTypeCompatible(variantType: Variant.Type, typedValueField: Type?, value: Variant): Boolean {
            if (typedValueField == null) {
                return false
            }
            return if (typedValueField.isPrimitive) {
                val primitiveType = typedValueField.asPrimitiveType()
                val logicalType = primitiveType.logicalTypeAnnotation
                val primitiveTypeName = primitiveType.primitiveTypeName

                when (variantType) {
                    Variant.Type.BOOLEAN -> primitiveTypeName === PrimitiveTypeName.BOOLEAN
                    Variant.Type.BYTE -> primitiveTypeName === PrimitiveTypeName.INT32 && logicalType is IntLogicalTypeAnnotation
                            && logicalType.isSigned
                            && logicalType.bitWidth == 8

                    Variant.Type.SHORT -> primitiveTypeName === PrimitiveTypeName.INT32 && logicalType is IntLogicalTypeAnnotation
                            && logicalType.isSigned
                            && logicalType.bitWidth == 16

                    Variant.Type.INT -> primitiveTypeName === PrimitiveTypeName.INT32
                            && (logicalType == null
                            || (logicalType is IntLogicalTypeAnnotation
                            && logicalType.isSigned
                            && (logicalType
                        .bitWidth == 32)))

                    Variant.Type.LONG -> primitiveTypeName === PrimitiveTypeName.INT64
                            && (logicalType == null
                            || (logicalType is IntLogicalTypeAnnotation
                            && logicalType.isSigned
                            && (logicalType
                        .bitWidth == 64)))

                    Variant.Type.FLOAT -> primitiveTypeName === PrimitiveTypeName.FLOAT
                    Variant.Type.DOUBLE -> primitiveTypeName === PrimitiveTypeName.DOUBLE
                    Variant.Type.DECIMAL4 -> primitiveTypeName === PrimitiveTypeName.INT32
                            && compatibleDecimalType(value, logicalType)

                    Variant.Type.DECIMAL8 -> primitiveTypeName === PrimitiveTypeName.INT64
                            && compatibleDecimalType(value, logicalType)

                    Variant.Type.DECIMAL16 -> primitiveTypeName === PrimitiveTypeName.BINARY
                            && compatibleDecimalType(value, logicalType)

                    Variant.Type.DATE -> primitiveTypeName === PrimitiveTypeName.INT32
                            && logicalType is DateLogicalTypeAnnotation

                    Variant.Type.TIME -> primitiveTypeName === PrimitiveTypeName.INT64
                            && logicalType is TimeLogicalTypeAnnotation

                    Variant.Type.TIMESTAMP_NTZ, Variant.Type.TIMESTAMP_NANOS_NTZ, Variant.Type.TIMESTAMP_TZ, Variant.Type.TIMESTAMP_NANOS_TZ -> if (primitiveTypeName === PrimitiveTypeName.INT64
                        && logicalType is TimestampLogicalTypeAnnotation
                    ) {
                        val annotation = logicalType
                        val micros = annotation.unit == LogicalTypeAnnotation.TimeUnit.MICROS
                        val nanos = annotation.unit == LogicalTypeAnnotation.TimeUnit.NANOS
                        val adjustedToUTC = annotation.isAdjustedToUTC
                        (variantType == Variant.Type.TIMESTAMP_TZ && micros && adjustedToUTC)
                                || (variantType == Variant.Type.TIMESTAMP_NTZ && micros && !adjustedToUTC)
                                || (variantType == Variant.Type.TIMESTAMP_NANOS_TZ && nanos && adjustedToUTC)
                                || (variantType == Variant.Type.TIMESTAMP_NANOS_NTZ && nanos && !adjustedToUTC)
                    } else {
                        false
                    }

                    Variant.Type.STRING -> primitiveTypeName === PrimitiveTypeName.BINARY
                            && logicalType is StringLogicalTypeAnnotation

                    Variant.Type.BINARY -> primitiveTypeName === PrimitiveTypeName.BINARY
                            && logicalType == null
                    else -> false
                }
            } else if (typedValueField.logicalTypeAnnotation is ListLogicalTypeAnnotation) {
                variantType == Variant.Type.ARRAY
            } else {
                variantType == Variant.Type.OBJECT
            }
        }
    }
}
