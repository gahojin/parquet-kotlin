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
import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.variant.VariantUtil.getType
import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer

object VariantConverters {
    fun newVariantConverter(
        variantGroup: GroupType,
        metadata: (ByteBuffer) -> Unit,
        parent: ParentConverter<VariantBuilder>,
    ): GroupConverter {
        val converter = newValueConverter(variantGroup, parent)

        // if there is a metadata field, add a converter for it
        if (variantGroup.containsField("metadata")) {
            val metadataIndex = variantGroup.getFieldIndex("metadata")
            val metadataField = variantGroup.getType("metadata")
            Preconditions.checkArgument(isBinary(metadataField), "Invalid metadata field: " + metadataField)
            converter.setConverter(metadataIndex, VariantMetadataConverter(metadata))
        }

        return converter
    }

    fun newValueConverter(
        valueGroup: GroupType,
        parent: ParentConverter<out VariantBuilder>,
    ): ValueConverter {
        if (valueGroup.containsField("typed_value")) {
            val typedValueField = valueGroup.getType("typed_value")
            if (isShreddedObject(typedValueField)) {
                return ShreddedObjectConverter(valueGroup, parent)
            }
        }

        return ShreddedValueConverter(valueGroup, parent)
    }

    // Return an appropriate converter for the given Parquet type.
    fun newScalarConverter(
        primitive: PrimitiveType,
        parent: ParentConverter<out VariantBuilder>,
    ): Converter {
        val annotation = primitive.logicalTypeAnnotation
        val primitiveType = primitive.primitiveTypeName
        return if (primitiveType === PrimitiveTypeName.BOOLEAN) {
            VariantBooleanConverter(parent)
        } else if (annotation is IntLogicalTypeAnnotation) {
            if (!annotation.isSigned) {
                throw UnsupportedOperationException("Unsupported shredded value type: $annotation")
            }
            when (annotation.bitWidth) {
                8 -> VariantByteConverter(parent)
                16 -> VariantShortConverter(parent)
                32 -> VariantIntConverter(parent)
                64 -> VariantLongConverter(parent)
                else -> throw UnsupportedOperationException("Unsupported shredded value type: $annotation")
            }
        } else if (annotation == null && primitiveType === PrimitiveTypeName.INT32) {
            VariantIntConverter(parent)
        } else if (annotation == null && primitiveType === PrimitiveTypeName.INT64) {
            VariantLongConverter(parent)
        } else if (primitiveType === PrimitiveTypeName.FLOAT) {
            VariantFloatConverter(parent)
        } else if (primitiveType === PrimitiveTypeName.DOUBLE) {
            VariantDoubleConverter(parent)
        } else if (annotation is DecimalLogicalTypeAnnotation) {
            VariantDecimalConverter(parent, annotation.scale)
        } else if (annotation is DateLogicalTypeAnnotation) {
            VariantDateConverter(parent)
        } else if (annotation is TimestampLogicalTypeAnnotation) {
            if (annotation.isAdjustedToUTC) {
                when (annotation.unit) {
                    LogicalTypeAnnotation.TimeUnit.MILLIS -> throw UnsupportedOperationException("MILLIS not supported by Variant")
                    LogicalTypeAnnotation.TimeUnit.MICROS -> VariantTimestampConverter(parent)
                    LogicalTypeAnnotation.TimeUnit.NANOS -> VariantTimestampNanosConverter(parent)
                }
            } else {
                when (annotation.unit) {
                    LogicalTypeAnnotation.TimeUnit.MILLIS -> throw UnsupportedOperationException("MILLIS not supported by Variant")
                    LogicalTypeAnnotation.TimeUnit.MICROS -> VariantTimestampNtzConverter(parent)
                    LogicalTypeAnnotation.TimeUnit.NANOS -> VariantTimestampNanosNtzConverter(parent)
                }
            }
        } else if (annotation is TimeLogicalTypeAnnotation) {
            if (annotation.isAdjustedToUTC || annotation.unit != LogicalTypeAnnotation.TimeUnit.MICROS) {
                throw UnsupportedOperationException("$annotation not supported by Variant")
            } else {
                VariantTimeConverter(parent)
            }
        } else if (annotation is UUIDLogicalTypeAnnotation) {
            VariantUUIDConverter(parent)
        } else if (annotation is StringLogicalTypeAnnotation) {
            VariantStringConverter(parent)
        } else if (primitiveType === PrimitiveTypeName.BINARY) {
            VariantBinaryConverter(parent)
        } else {
            throw UnsupportedOperationException("Unsupported shredded value type: $primitive")
        }
    }

    private fun isBinary(field: Type): Boolean {
        return field.isPrimitive && field.asPrimitiveType().getPrimitiveTypeName() === PrimitiveTypeName.BINARY
    }

    private fun isShreddedArray(typedValueField: Type): Boolean {
        return typedValueField.logicalTypeAnnotation is ListLogicalTypeAnnotation
    }

    private fun isShreddedObject(typedValueField: Type): Boolean {
        return !typedValueField.isPrimitive && !isShreddedArray(typedValueField)
    }

    fun interface ParentConverter<T : VariantBuilder> {
        fun build(buildConsumer: (T) -> Unit)
    }

    /**
     * Converter for metadata
     */
    internal class VariantMetadataConverter(
        private val handleMetadata: (ByteBuffer) -> Unit,
    ) : BinaryConverter() {
        override fun handleBinary(value: Binary) {
            handleMetadata(value.toByteBuffer())
        }
    }

    /**
     * Converter for a non-object value field; appends the encoded value to the builder
     */
    internal class VariantValueConverter(
        private val parent: ParentConverter<out VariantBuilder>,
    ) : BinaryConverter() {
        override fun handleBinary(value: Binary) {
            parent.build { it.appendEncodedValue(value.toByteBuffer()) }
        }
    }

    open class ValueConverter(group: GroupType) : GroupConverter() {
        private val converters: Array<Converter?> = arrayOfNulls(group.fieldCount)

        fun setConverter(index: Int, converter: Converter) {
            this.converters[index] = converter
        }

        override fun getConverter(fieldIndex: Int): Converter {
            return checkNotNull(converters[fieldIndex])
        }

        override fun start() = Unit

        override fun end() = Unit
    }

    internal class ShreddedValueConverter(
        group: GroupType,
        parent: ParentConverter<out VariantBuilder>,
    ) : ValueConverter(group) {
        init {
            if (group.containsField("typed_value")) {
                val typedValueIndex = group.getFieldIndex("typed_value")
                val valueField = group.getType(typedValueIndex)
                if (valueField.isPrimitive) {
                    setConverter(typedValueIndex, newScalarConverter(valueField.asPrimitiveType(), parent))
                } else if (isShreddedArray(valueField)) {
                    setConverter(typedValueIndex, ShreddedArrayConverter(valueField.asGroupType(), parent))
                }
            }

            if (group.containsField("value")) {
                val valueIndex = group.getFieldIndex("value")
                val typedField = group.getType(valueIndex)
                require(typedField.isPrimitive && typedField.asPrimitiveType().primitiveTypeName === PrimitiveTypeName.BINARY) {
                    "Invalid variant value type: $typedField"
                }

                setConverter(valueIndex, VariantValueConverter(parent))
            }
        }
    }

    internal class ShreddedObjectConverter(
        group: GroupType,
        private val parent: ParentConverter<out VariantBuilder>,
    ) : ValueConverter(group) {
        init {
            val typedValueIndex = group.getFieldIndex("typed_value")
            val typedField = group.getType(typedValueIndex)
            require(isShreddedObject(typedField)) {
                "Invalid typed_value for shredded object: $typedField"
            }

            val fieldsConverter = PartiallyShreddedFieldsConverter(typedField.asGroupType(), parent)
            setConverter(typedValueIndex, fieldsConverter)

            if (group.containsField("value")) {
                val valueIndex = group.getFieldIndex("value")
                val valueField = group.getType(valueIndex)
                Preconditions.checkArgument(isBinary(valueField), "Invalid variant value type: $valueField")

                setConverter(
                    index = valueIndex,
                    converter = PartiallyShreddedValueConverter(parent, fieldsConverter.shreddedFieldNames()),
                )
            }
        }

        override fun end() {
            parent.build(VariantBuilder::endObjectIfExists)
        }
    }

    /**
     * Converter for a partially shredded object's value field; copies object fields or appends an encoded non-object.
     *
     *
     * This must be wrapped by [ShreddedObjectConverter] to finalize the object.
     */
    internal class PartiallyShreddedValueConverter(
        private val parent: ParentConverter<out VariantBuilder>,
        private val suppressedKeys: Set<String>,
    ) : BinaryConverter() {
        override fun handleBinary(value: Binary) {
            parent.build {
                val buffer = value.toByteBuffer()
                if (getType(buffer) == Variant.Type.OBJECT) {
                    it.startOrContinuePartialObject(value.toByteBuffer(), suppressedKeys)
                } else {
                    it.appendEncodedValue(buffer)
                }
            }
        }
    }

    /**
     * Converter for a shredded object's shredded fields.
     *
     *
     * This must be wrapped by [ShreddedObjectConverter] to finalize the object.
     */
    internal class PartiallyShreddedFieldsConverter(
        fieldsType: GroupType,
        private val parent: ParentConverter<out VariantBuilder>
    ) : GroupConverter() {
        private val converters: Array<Converter>
        private val shreddedFieldNames = mutableSetOf<String>()
        private var objectBuilder: VariantObjectBuilder? = null

        init {
            converters = (0..fieldsType.fieldCount).map { index ->
                val field = fieldsType.getType(index)
                require(!field.isPrimitive) { "Invalid field group: $field" }

                val name = field.name
                shreddedFieldNames.add(name)
                val newParent = ParentConverter { converter -> objectBuilder?.also {
                    converter(it)
                } }
                FieldValueConverter(name, field.asGroupType(), newParent)
            }.toTypedArray()
        }

        internal fun shreddedFieldNames(): MutableSet<String> {
            return shreddedFieldNames
        }

        override fun getConverter(fieldIndex: Int): Converter {
            return converters[fieldIndex]
        }

        override fun start() {
            try {
                // the builder may already have an object builder from the value field
                parent.build {
                    objectBuilder = it.startOrContinueObject()
                }
            } catch (e: IllegalStateException) {
                throw IllegalStateException("Invalid variant, conflicting value and typed_value", e)
            }
        }

        override fun end() {
            this.objectBuilder = null
        }

        private class FieldValueConverter(
            private val fieldName: String,
            field: GroupType,
            private val parent: ParentConverter<out VariantObjectBuilder>,
        ) : GroupConverter() {
            private val converter: GroupConverter = newValueConverter(field.asGroupType(), ParentConverter { consumer ->
                parent.build { consumer(it) }
            })
            private var numFields: Long? = null

            override fun getConverter(fieldIndex: Int): Converter {
                return converter.getConverter(fieldIndex)
            }

            override fun start() {
                converter.start()
                parent.build {
                    this.numFields = it.numValues
                    it.appendKey(fieldName)
                }
            }

            override fun end() {
                parent.build {
                    if (it.numValues == numFields) {
                        it.dropLastKey()
                    }
                }
                this.numFields = null
                converter.end()
            }
        }
    }

    /**
     * Converter for a shredded array
     */
    internal class ShreddedArrayConverter(
        list: GroupType,
        parent: ParentConverter<out VariantBuilder>,
    ) : GroupConverter() {
        private val parent: ParentConverter<out VariantBuilder>
        private val repeatedConverter: ShreddedArrayRepeatedConverter
        private var arrayBuilder: VariantArrayBuilder? = null

        init {
            require(list.fieldCount == 1) {
                "Invalid list type: $list"
            }
            this.parent = parent

            val repeated = list.getType(0)
            require(repeated.isRepetition(Repetition.REPEATED) &&
                    !repeated.isPrimitive && repeated.asGroupType().fieldCount == 1) {
                "Invalid repeated type in list: $repeated"
            }

            repeatedConverter = ShreddedArrayRepeatedConverter(repeated.asGroupType()) {
                arrayBuilder?.also { builder -> it(builder) }
            }
        }

        override fun getConverter(fieldIndex: Int): Converter {
            if (fieldIndex > 1) {
                throw ArrayIndexOutOfBoundsException(fieldIndex)
            }

            return repeatedConverter
        }

        override fun start() {
            try {
                parent.build { arrayBuilder = it.startArray() }
            } catch (e: IllegalStateException) {
                throw IllegalStateException("Invalid variant, conflicting value and typed_value", e)
            }
        }

        override fun end() {
            parent.build { it.endArray() }
            arrayBuilder = null
        }
    }

    /**
     * Converter for the repeated field of a shredded array
     */
    internal class ShreddedArrayRepeatedConverter(
        repeated: GroupType,
        parent: ParentConverter<VariantArrayBuilder>,
    ) : GroupConverter() {
        private val elementConverter: GroupConverter
        private val parent: ParentConverter<VariantArrayBuilder>
        private var numValues: Long = 0L

        init {
            val element = repeated.getType(0)
            require(!element.isPrimitive) {
                "Invalid element type in variant array: $element"
            }

            val newParent = ParentConverter { consumer ->
                parent.build(consumer)
            }
            this.elementConverter = newValueConverter(element.asGroupType(), newParent)
            this.parent = parent
        }

        override fun getConverter(fieldIndex: Int): Converter {
            if (fieldIndex > 1) {
                throw ArrayIndexOutOfBoundsException(fieldIndex)
            }

            return elementConverter
        }

        override fun start() {
            parent.build { numValues = it.numValues }
        }

        override fun end() {
            parent.build { builder: VariantArrayBuilder ->
                val valuesWritten = builder.numValues - numValues
                check(valuesWritten <= 1) { "Invalid variant, conflicting value and typed_value" }
                if (valuesWritten == 0L) {
                    builder.appendNull()
                }
            }
        }
    }

    // Base class for converting primitive typed_value fields.
    internal open class ShreddedScalarConverter(
        protected var parent: ParentConverter<out VariantBuilder>,
    ) : PrimitiveConverter()

    internal class VariantStringConverter(
        parent: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parent) {
        override fun addBinary(value: Binary) {
            parent.build { it.appendString(value.toStringUsingUTF8()) }
        }
    }

    internal class VariantBinaryConverter(
        parent: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parent) {
        override fun addBinary(value: Binary) {
            parent.build { it.appendBinary(value.toByteBuffer()) }
        }
    }

    internal class VariantDecimalConverter(
        parent: ParentConverter<out VariantBuilder>,
        private val scale: Int,
    ) : ShreddedScalarConverter(parent) {
        override fun addBinary(value: Binary) {
            parent.build {
                it.appendDecimal(BigDecimal(BigInteger(value.bytes), scale))
            }
        }

        override fun addLong(value: Long) {
            val decimal = BigDecimal.valueOf(value, scale)
            parent.build { it.appendDecimal(decimal) }
        }

        override fun addInt(value: Int) {
            val decimal = BigDecimal.valueOf(value.toLong(), scale)
            parent.build { it.appendDecimal(decimal) }
        }
    }

    internal class VariantUUIDConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addBinary(value: Binary) {
            parent.build { it.appendUUIDBytes(value.toByteBuffer()) }
        }
    }

    internal class VariantBooleanConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addBoolean(value: Boolean) {
            parent.build { it.appendBoolean(value) }
        }
    }

    internal class VariantDoubleConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addDouble(value: Double) {
            parent.build { it.appendDouble(value) }
        }
    }

    internal class VariantFloatConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addFloat(value: Float) {
            parent.build { it.appendFloat(value) }
        }
    }

    internal class VariantByteConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addInt(value: Int) {
            parent.build { it.appendByte(value.toByte()) }
        }
    }

    internal class VariantShortConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) :
        ShreddedScalarConverter(parentBuilder) {
        override fun addInt(value: Int) {
            parent.build { it.appendShort(value.toShort()) }
        }
    }

    internal class VariantIntConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addInt(value: Int) {
            parent.build { it.appendInt(value) }
        }
    }

    internal class VariantLongConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addLong(value: Long) {
            parent.build { it.appendLong(value) }
        }
    }

    internal class VariantDateConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addInt(value: Int) {
            parent.build { it.appendDate(value) }
        }
    }

    internal class VariantTimeConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addLong(value: Long) {
            parent.build { it.appendTime(value) }
        }
    }

    internal class VariantTimestampConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addLong(value: Long) {
            parent.build { it.appendTimestampTz(value) }
        }
    }

    internal class VariantTimestampNtzConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addLong(value: Long) {
            parent.build { it.appendTimestampNtz(value) }
        }
    }

    internal class VariantTimestampNanosConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addLong(value: Long) {
            parent.build { it.appendTimestampNanosTz(value) }
        }
    }

    internal class VariantTimestampNanosNtzConverter(
        parentBuilder: ParentConverter<out VariantBuilder>,
    ) : ShreddedScalarConverter(parentBuilder) {
        override fun addLong(value: Long) {
            parent.build { it.appendTimestampNanosNtz(value) }
        }
    }

    /**
     * Base class for value and metadata converters, that both return a binary.
     */
    internal abstract class BinaryConverter : PrimitiveConverter() {
        lateinit var dict: Array<Binary>

        protected abstract fun handleBinary(value: Binary)

        override fun hasDictionarySupport() = true

        override fun setDictionary(dictionary: Dictionary) {
            dict = (0..dictionary.maxId).map {
                dictionary.decodeToBinary(it)
            }.toTypedArray()
        }

        override fun addBinary(value: Binary) {
            handleBinary(value)
        }

        override fun addValueFromDictionary(dictionaryId: Int) {
            handleBinary(dict[dictionaryId])
        }
    }
}
