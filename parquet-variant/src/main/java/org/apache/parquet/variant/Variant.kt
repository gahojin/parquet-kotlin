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

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.util.UUID

/**
 * This Variant class holds the Variant-encoded value and metadata binary values.
 */
class Variant(value: ByteBuffer, metadata: ByteBuffer) {
    /**
     * The buffer that contains the Variant value.
     * The buffers are read a single-byte at a time, so the endianness of the input buffers is not important.
     */
    @JvmField
    val value: ByteBuffer = value.asReadOnlyBuffer()

    /**
     * The buffer that contains the Variant metadata.
     */
    @JvmField
    val metadata: ByteBuffer = metadata.asReadOnlyBuffer()

    init {
        // There is currently only one allowed version.
        if ((metadata[metadata.position()].toInt() and VariantUtil.VERSION_MASK) != VariantUtil.VERSION) {
            throw UnsupportedOperationException(
                "Unsupported variant metadata version: %d".format(metadata[metadata.position()].toInt() and VariantUtil.VERSION_MASK))
        }
    }

    /** the boolean value */
    val boolean: Boolean
        get() = VariantUtil.getBoolean(value)

    /** the byte value */
    val byte: Byte
        get() = VariantUtil.getByte(value)

    /** the short value */
    val short: Short
        get() = VariantUtil.getShort(value)

    /** the int value */
    val int: Int
        get() = VariantUtil.getInt(value)

    /** the long value */
    val long: Long
        get() = VariantUtil.getLong(value)

    /** the double value */
    val double: Double
        get() = VariantUtil.getDouble(value)

    /** the decimal value */
    val decimal: BigDecimal?
        get() = VariantUtil.getDecimal(value)

    /** the float value */
    val float: Float
        get() = VariantUtil.getFloat(value)

    /** the binary value */
    val binary: ByteBuffer
        get() = VariantUtil.getBinary(value)

    /** the UUID value */
    val uUID: UUID
        get() = VariantUtil.getUUID(value)

    /** the string value */
    val string: String
        get() = VariantUtil.getString(value)

    /**
     * The value type of Variant value. It is determined by the header byte.
     */
    enum class Type {
        OBJECT,
        ARRAY,
        NULL,
        BOOLEAN,
        BYTE,
        SHORT,
        INT,
        LONG,
        STRING,
        DOUBLE,
        DECIMAL4,
        DECIMAL8,
        DECIMAL16,
        DATE,
        TIMESTAMP_TZ,
        TIMESTAMP_NTZ,
        FLOAT,
        BINARY,
        TIME,
        TIMESTAMP_NANOS_TZ,
        TIMESTAMP_NANOS_NTZ,
        UUID,
    }

    /** the type of the variant value */
    val type: Type?
        get() = VariantUtil.getType(value)

    constructor(
        value: ByteBuffer,
        metadata: Metadata,
    ) : this(
        value = value,
        metadata = metadata.encodedBuffer,
    )

    constructor(value: ByteArray, metadata: ByteArray) : this(
        value = value,
        valuePos = 0,
        valueLength = value.size,
        metadata = metadata,
        metadataPos = 0,
        metadataLength = metadata.size,
    )

    constructor(
        value: ByteArray,
        valuePos: Int,
        valueLength: Int,
        metadata: ByteArray,
        metadataPos: Int,
        metadataLength: Int,
    ) : this(
        value = ByteBuffer.wrap(value, valuePos, valueLength),
        metadata = ByteBuffer.wrap(metadata, metadataPos, metadataLength),
    )

    /**
     * @return the number of object fields in the variant
     * @throws IllegalArgumentException if `getType()` does not return `Type.OBJECT`
     */
    fun numObjectElements(): Int {
        return VariantUtil.getObjectInfo(value).numElements
    }

    /**
     * Returns the object field Variant value whose key is equal to `key`.
     * Returns null if the key is not found.
     * @param key the key to look up
     * @return the field value whose key is equal to `key`, or null if key is not found
     * @throws IllegalArgumentException if `getType()` does not return `Type.OBJECT`
     */
    fun getFieldByKey(key: String): Variant? {
        val info = VariantUtil.getObjectInfo(value)
        // Use linear search for a short list. Switch to binary search when the length reaches
        // `BINARY_SEARCH_THRESHOLD`.
        if (info.numElements < BINARY_SEARCH_THRESHOLD) {
            for (i in 0..<info.numElements) {
                val field: ObjectField = getFieldAtIndex(
                    i,
                    value,
                    metadata,
                    info.idSize,
                    info.offsetSize,
                    value.position() + info.idStartOffset,
                    value.position() + info.offsetStartOffset,
                    value.position() + info.dataStartOffset,
                )
                if (field.key == key) {
                    return field.value
                }
            }
        } else {
            var low = 0
            var high = info.numElements - 1
            while (low <= high) {
                // Use unsigned right shift to compute the middle of `low` and `high`. This is not only a
                // performance optimization, because it can properly handle the case where `low + high`
                // overflows int.
                val mid = (low + high) ushr 1
                val field: ObjectField = getFieldAtIndex(
                    mid,
                    value,
                    metadata,
                    info.idSize,
                    info.offsetSize,
                    value.position() + info.idStartOffset,
                    value.position() + info.offsetStartOffset,
                    value.position() + info.dataStartOffset,
                )
                val cmp = field.key.compareTo(key)
                if (cmp < 0) {
                    low = mid + 1
                } else if (cmp > 0) {
                    high = mid - 1
                } else {
                    return field.value
                }
            }
        }
        return null
    }

    /**
     * A field in a Variant object.
     */
    class ObjectField(val key: String, val value: Variant)

    /**
     * @return the number of array elements
     * @throws IllegalArgumentException if `getType()` does not return `Type.ARRAY`
     */
    fun numArrayElements(): Int {
        return VariantUtil.getArrayInfo(value).numElements
    }

    /**
     * Returns the array element Variant value at the `index` slot. Returns null if `index` is
     * out of the bound of `[0, arraySize())`.
     *
     * @param index the index of the array element to get
     * @return the array element Variant at the `index` slot, or null if `index` is out of bounds
     * @throws IllegalArgumentException if `getType()` does not return `Type.ARRAY`
     */
    fun getElementAtIndex(index: Int): Variant? {
        val info = VariantUtil.getArrayInfo(value)
        if (index < 0 || index >= info.numElements) {
            return null
        }
        return getElementAtIndex(
            index,
            value,
            metadata,
            info.offsetSize,
            value.position() + info.offsetStartOffset,
            value.position() + info.dataStartOffset,
        )
    }

    /**
     * Returns the field at index idx, lexicographically ordered.
     *
     * @param idx the index to look up
     * @return the field value whose key is equal to `key`, or null if key is not found
     * @throws IllegalArgumentException if `getType()` does not return `Type.OBJECT`
     */
    fun getFieldAtIndex(idx: Int): ObjectField {
        val info = VariantUtil.getObjectInfo(value)

        val pos = value.position()

        // Use linear search for a short list. Switch to binary search when the length reaches
        // `BINARY_SEARCH_THRESHOLD`.
        return getFieldAtIndex(
            idx,
            value,
            metadata,
            info.idSize,
            info.idStartOffset,
            pos + info.idStartOffset,
            pos + info.offsetStartOffset,
            pos + info.dataStartOffset,
        )
    }

    companion object {
        /**
         * The threshold to switch from linear search to binary search when looking up a field by key in
         * an object. This is a performance optimization to avoid the overhead of binary search for a
         * short list.
         */
        const val BINARY_SEARCH_THRESHOLD: Int = 32

        private fun getFieldAtIndex(
            index: Int,
            value: ByteBuffer,
            metadata: ByteBuffer,
            idSize: Int,
            offsetSize: Int,
            idStart: Int,
            offsetStart: Int,
            dataStart: Int,
        ): ObjectField {
            // idStart, offsetStart, and dataStart are absolute positions in the `value` buffer.
            val id = VariantUtil.readUnsigned(value, idStart + idSize * index, idSize)
            val offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize)
            val key = VariantUtil.getMetadataKey(metadata, id)
            val v = Variant(VariantUtil.slice(value, dataStart + offset), metadata)
            return ObjectField(key, v)
        }

        private fun getElementAtIndex(
            index: Int,
            value: ByteBuffer,
            metadata: ByteBuffer,
            offsetSize: Int,
            offsetStart: Int,
            dataStart: Int,
        ): Variant {
            // offsetStart and dataStart are absolute positions in the `value` buffer.
            val offset = VariantUtil.readUnsigned(value, offsetStart + offsetSize * index, offsetSize)
            return Variant(VariantUtil.slice(value, dataStart + offset), metadata)
        }
    }
}
