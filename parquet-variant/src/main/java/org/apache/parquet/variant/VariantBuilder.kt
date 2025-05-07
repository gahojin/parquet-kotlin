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

import org.apache.parquet.variant.VariantUtil.arrayHeader
import org.apache.parquet.variant.VariantUtil.objectHeader
import org.apache.parquet.variant.VariantUtil.shortStrHeader
import org.apache.parquet.variant.VariantUtil.writeLong
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.util.UUID

/**
 * Builder for creating Variant value and metadata.
 */
open class VariantBuilder {
    /** The buffer for building the Variant value. The first `writePos` bytes have been written.  */
    protected var writeBuffer = ByteArray(1024)

    protected var writePos: Int = 0

    /** The dictionary for mapping keys to monotonically increasing ids.  */
    private val dictionary = hashMapOf<String, Int>()

    /** The keys in the dictionary, in id order.  */
    private val dictionaryKeys = ArrayList<ByteArray>()

    /**
     * These are used to build nested objects and arrays, via startObject() and startArray().
     * Only one of these can be non-null at a time. If one of these is non-null, then no append()
     * methods can be called on this builder, until endObject() or endArray() is called.
     */
    protected var objectBuilder: VariantObjectBuilder? = null

    protected var arrayBuilder: VariantArrayBuilder? = null

    /**
     * @return the Variant value
     */
    fun build(): Variant {
        check(objectBuilder == null) { "Cannot call build() while an object is being built. Must call endObject() first." }
        check(arrayBuilder == null) { "Cannot call build() while an array is being built. Must call endArray() first." }
        val numKeys = dictionaryKeys.size
        // Use long to avoid overflow in accumulating lengths.
        var dictionaryTotalDataSize: Long = 0
        for (key in dictionaryKeys) {
            dictionaryTotalDataSize += key.size.toLong()
        }
        // Determine the number of bytes required per offset entry.
        // The largest offset is the one-past-the-end value, which is total data size. It's very
        // unlikely that the number of keys could be larger, but incorporate that into the calculation
        // in case of pathological data.
        val maxSize = maxOf(dictionaryTotalDataSize, numKeys.toLong())
        val offsetSize = getMinIntegerSize(maxSize.toInt())

        val offsetListOffset = 1 + offsetSize
        val dataOffset = offsetListOffset + (numKeys + 1) * offsetSize
        val metadataSize = dataOffset + dictionaryTotalDataSize

        val metadata = ByteArray(metadataSize.toInt())
        // Only unsorted dictionary keys are supported.
        // TODO: Support sorted dictionary keys.
        val headerByte = VariantUtil.VERSION or ((offsetSize - 1) shl 6)
        writeLong(metadata, 0, headerByte.toLong(), 1)
        writeLong(metadata, 1, numKeys.toLong(), offsetSize)
        var currentOffset = 0
        for (i in 0..<numKeys) {
            writeLong(metadata, offsetListOffset + i * offsetSize, currentOffset.toLong(), offsetSize)
            val key = dictionaryKeys[i]
            System.arraycopy(key, 0, metadata, dataOffset + currentOffset, key.size)
            currentOffset += key.size
        }
        writeLong(metadata, offsetListOffset + numKeys * offsetSize, currentOffset.toLong(), offsetSize)
        // Copying the data to a new buffer, to retain only the required data length, not the capacity.
        // TODO: Reduce the copying, and look into builder reuse.
        return Variant(writeBuffer.copyOfRange(0, writePos), metadata)
    }

    /**
     * Appends a string value to the Variant builder.
     * @param str the string value to append
     */
    fun appendString(str: String) {
        onAppend()
        val data = str.toByteArray(StandardCharsets.UTF_8)
        val longStr = data.size > VariantUtil.MAX_SHORT_STR_SIZE
        checkCapacity((if (longStr) 1 + VariantUtil.U32_SIZE else 1) + data.size)
        if (longStr) {
            writeBuffer[writePos] = VariantUtil.HEADER_LONG_STRING
            writePos += 1
            writeLong(writeBuffer, writePos, data.size.toLong(), VariantUtil.U32_SIZE)
            writePos += VariantUtil.U32_SIZE
        } else {
            writeBuffer[writePos] = shortStrHeader(data.size)
            writePos += 1
        }
        System.arraycopy(data, 0, writeBuffer, writePos, data.size)
        writePos += data.size
    }

    /**
     * Appends a null value to the Variant builder.
     */
    fun appendNull() {
        onAppend()
        checkCapacity(1)
        writeBuffer[writePos] = VariantUtil.HEADER_NULL
        writePos += 1
    }

    /**
     * Appends a boolean value to the Variant builder.
     * @param b the boolean value to append
     */
    fun appendBoolean(b: Boolean) {
        onAppend()
        checkCapacity(1)
        writeBuffer[writePos] = if (b) VariantUtil.HEADER_TRUE else VariantUtil.HEADER_FALSE
        writePos += 1
    }

    /**
     * Appends a long value to the variant builder.
     * @param l the long value to append
     */
    fun appendLong(l: Long) {
        onAppend()
        checkCapacity(1 /* header size */ + 8)
        writeBuffer[writePos] = VariantUtil.HEADER_INT64
        writeLong(writeBuffer, writePos + 1, l, 8)
        writePos += 9
    }

    /**
     * Appends an int value to the variant builder.
     * @param i the int to append
     */
    fun appendInt(i: Int) {
        onAppend()
        checkCapacity(1 /* header size */ + 4)
        writeBuffer[writePos] = VariantUtil.HEADER_INT32
        writeLong(writeBuffer, writePos + 1, i.toLong(), 4)
        writePos += 5
    }

    /**
     * Appends a short value to the variant builder.
     * @param s the short to append
     */
    fun appendShort(s: Short) {
        onAppend()
        checkCapacity(1 /* header size */ + 2)
        writeBuffer[writePos] = VariantUtil.HEADER_INT16
        writeLong(writeBuffer, writePos + 1, s.toLong(), 2)
        writePos += 3
    }

    /**
     * Appends a byte value to the variant builder.
     * @param b the byte to append
     */
    fun appendByte(b: Byte) {
        onAppend()
        checkCapacity(1 /* header size */ + 1)
        writeBuffer[writePos] = VariantUtil.HEADER_INT8
        writeLong(writeBuffer, writePos + 1, b.toLong(), 1)
        writePos += 2
    }

    /**
     * Appends a double value to the variant builder.
     * @param d the double to append
     */
    fun appendDouble(d: Double) {
        onAppend()
        checkCapacity(1 /* header size */ + 8)
        writeBuffer[writePos] = VariantUtil.HEADER_DOUBLE
        writeLong(writeBuffer, writePos + 1, java.lang.Double.doubleToLongBits(d), 8)
        writePos += 9
    }

    /**
     * Appends a decimal value to the variant builder. The actual encoded decimal type depends on the
     * precision and scale of the decimal value.
     * @param d the decimal value to append
     */
    fun appendDecimal(d: BigDecimal) {
        onAppend()
        val unscaled = d.unscaledValue()
        if (d.scale() <= VariantUtil.MAX_DECIMAL4_PRECISION && d.precision() <= VariantUtil.MAX_DECIMAL4_PRECISION) {
            checkCapacity(2 /* header and scale size */ + 4)
            writeBuffer[writePos] = VariantUtil.HEADER_DECIMAL4
            writeBuffer[writePos + 1] = d.scale().toByte()
            writeLong(writeBuffer, writePos + 2, unscaled.intValueExact().toLong(), 4)
            writePos += 6
        } else if (d.scale() <= VariantUtil.MAX_DECIMAL8_PRECISION
            && d.precision() <= VariantUtil.MAX_DECIMAL8_PRECISION
        ) {
            checkCapacity(2 /* header and scale size */ + 8)
            writeBuffer[writePos] = VariantUtil.HEADER_DECIMAL8
            writeBuffer[writePos + 1] = d.scale().toByte()
            writeLong(writeBuffer, writePos + 2, unscaled.longValueExact(), 8)
            writePos += 10
        } else {
            assert(
                d.scale() <= VariantUtil.MAX_DECIMAL16_PRECISION
                        && d.precision() <= VariantUtil.MAX_DECIMAL16_PRECISION
            )
            checkCapacity(2 /* header and scale size */ + 16)
            writeBuffer[writePos] = VariantUtil.HEADER_DECIMAL16
            writeBuffer[writePos + 1] = d.scale().toByte()
            writePos += 2
            // `toByteArray` returns a big-endian representation. We need to copy it reversely and sign
            // extend it to 16 bytes.
            val bytes = unscaled.toByteArray()
            for (i in bytes.indices) {
                writeBuffer[writePos + i] = bytes[bytes.size - 1 - i]
            }
            val sign = (if (bytes[0] < 0) -1 else 0).toByte()
            for (i in bytes.size..15) {
                writeBuffer[writePos + i] = sign
            }
            writePos += 16
        }
    }

    /**
     * Appends a date value to the variant builder. The date is represented as the number of days
     * since the epoch.
     * @param daysSinceEpoch the number of days since the epoch
     */
    fun appendDate(daysSinceEpoch: Int) {
        onAppend()
        checkCapacity(1 /* header size */ + 4)
        writeBuffer[writePos] = VariantUtil.HEADER_DATE
        writeLong(writeBuffer, writePos + 1, daysSinceEpoch.toLong(), 4)
        writePos += 5
    }

    /**
     * Appends a TimestampTz value to the variant builder. The timestamp is represented as the number
     * of microseconds since the epoch.
     * @param microsSinceEpoch the number of microseconds since the epoch
     */
    fun appendTimestampTz(microsSinceEpoch: Long) {
        onAppend()
        checkCapacity(1 /* header size */ + 8)
        writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_TZ
        writeLong(writeBuffer, writePos + 1, microsSinceEpoch, 8)
        writePos += 9
    }

    /**
     * Appends a TimestampNtz value to the variant builder. The timestamp is represented as the number
     * of microseconds since the epoch.
     * @param microsSinceEpoch the number of microseconds since the epoch
     */
    fun appendTimestampNtz(microsSinceEpoch: Long) {
        onAppend()
        checkCapacity(1 /* header size */ + 8)
        writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NTZ
        writeLong(writeBuffer, writePos + 1, microsSinceEpoch, 8)
        writePos += 9
    }

    /**
     * Appends a Time value to the variant builder. The time is represented as the number of
     * microseconds since midnight.
     * @param microsSinceMidnight the number of microseconds since midnight
     */
    fun appendTime(microsSinceMidnight: Long) {
        require(microsSinceMidnight >= 0) { "Time value (%d) cannot be negative.".format(microsSinceMidnight) }
        onAppend()
        checkCapacity(1 /* header size */ + 8)
        writeBuffer[writePos] = VariantUtil.HEADER_TIME
        writeLong(writeBuffer, writePos + 1, microsSinceMidnight, 8)
        writePos += 9
    }

    /**
     * Appends a TimestampNanosTz value to the variant builder. The timestamp is represented as the
     * number of nanoseconds since the epoch.
     * @param nanosSinceEpoch the number of nanoseconds since the epoch
     */
    fun appendTimestampNanosTz(nanosSinceEpoch: Long) {
        onAppend()
        checkCapacity(1 /* header size */ + 8)
        writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NANOS_TZ
        writeLong(writeBuffer, writePos + 1, nanosSinceEpoch, 8)
        writePos += 9
    }

    /**
     * Appends a TimestampNanosNtz value to the variant builder. The timestamp is represented as the
     * number of nanoseconds since the epoch.
     * @param nanosSinceEpoch the number of nanoseconds since the epoch
     */
    fun appendTimestampNanosNtz(nanosSinceEpoch: Long) {
        onAppend()
        checkCapacity(1 /* header size */ + 8)
        writeBuffer[writePos] = VariantUtil.HEADER_TIMESTAMP_NANOS_NTZ
        writeLong(writeBuffer, writePos + 1, nanosSinceEpoch, 8)
        writePos += 9
    }

    /**
     * Appends a float value to the variant builder.
     * @param f the float to append
     */
    fun appendFloat(f: Float) {
        onAppend()
        checkCapacity(1 /* header size */ + 4)
        writeBuffer[writePos] = VariantUtil.HEADER_FLOAT
        writeLong(writeBuffer, writePos + 1, java.lang.Float.floatToIntBits(f).toLong(), 8)
        writePos += 5
    }

    /**
     * Appends binary data to the variant builder.
     * @param binary the binary data to append
     */
    fun appendBinary(binary: ByteBuffer) {
        onAppend()
        val binarySize = binary.remaining()
        checkCapacity(1 /* header size */ + VariantUtil.U32_SIZE + binarySize)
        writeBuffer[writePos] = VariantUtil.HEADER_BINARY
        writePos += 1
        writeLong(writeBuffer, writePos, binarySize.toLong(), VariantUtil.U32_SIZE)
        writePos += VariantUtil.U32_SIZE
        ByteBuffer.wrap(writeBuffer, writePos, binarySize).put(binary)
        writePos += binarySize
    }

    /**
     * Appends a UUID value to the variant builder.
     * @param uuid the UUID to append
     */
    fun appendUUID(uuid: UUID) {
        onAppend()
        checkCapacity(1 /* header size */ + VariantUtil.UUID_SIZE)
        writeBuffer[writePos] = VariantUtil.HEADER_UUID
        writePos += 1

        val bb =
            ByteBuffer.wrap(writeBuffer, writePos, VariantUtil.UUID_SIZE).order(ByteOrder.BIG_ENDIAN)
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        writePos += VariantUtil.UUID_SIZE
    }

    /**
     * Starts appending an object to this variant builder. The returned VariantObjectBuilder is used
     * to append object keys and values. startObject() must be called before endObject().
     * No append*() methods can be called in between startObject() and endObject().
     *
     * Example usage:
     * VariantBuilder builder = new VariantBuilder();
     * VariantObjectBuilder objBuilder = builder.startObject();
     * objBuilder.appendKey("key1");
     * objBuilder.appendString("value1");
     * builder.endObject();
     *
     * @return a VariantObjectBuilder to build an object
     */
    fun startObject(): VariantObjectBuilder {
        onStartNested()
        check(objectBuilder == null) { "Cannot call startObject() without calling endObject() first." }
        check(arrayBuilder == null) { "Cannot call startObject() without calling endArray() first." }
        return VariantObjectBuilder(this).also {
            this.objectBuilder = it
        }
    }

    /**
     * Finishes appending the object to this builder. This method must be called after startObject(),
     * before other append*() methods can be called on this builder.
     */
    internal fun endObject() {
        val objectBuilder = checkNotNull(objectBuilder) { "Cannot call endObject() without calling startObject() first." }
        val fields: ArrayList<FieldEntry> = objectBuilder.validateAndGetFields()
        var numFields = fields.size
        fields.sort()
        var maxId = if (numFields == 0) 0 else fields[0].id
        var dataSize = if (numFields == 0) 0 else fields[0].valueSize

        var distinctPos = 0
        // Maintain a list of distinct keys in-place.
        for (i in 1..<numFields) {
            maxId = maxOf(maxId, fields[i].id)
            if (fields[i].id == fields[i - 1].id) {
                // Found a duplicate key. Keep the field with the greater offset, because it was written last.
                if (fields[distinctPos].offset < fields[i].offset) {
                    fields[distinctPos] = fields[i]
                }
            } else {
                // Found a distinct key. Add the field to the list.
                distinctPos++
                fields[distinctPos] = fields[i]
                dataSize += fields[i].valueSize
            }
        }

        if (distinctPos + 1 < fields.size) {
            numFields = distinctPos + 1
            // Resize `fields` to `size`.
            fields.subList(numFields, fields.size).clear()
        }

        val largeSize = numFields > VariantUtil.U8_MAX
        val sizeBytes = if (largeSize) VariantUtil.U32_SIZE else 1
        val idSize = getMinIntegerSize(maxId)
        val offsetSize = getMinIntegerSize(dataSize)
        // The data starts after: the header byte, object size, id list, and offset list.
        val dataOffset = 1 + sizeBytes + numFields * idSize + (numFields + 1) * offsetSize
        checkCapacity(dataOffset + dataSize)

        // Write the header byte and size entry.
        writeBuffer[writePos] = objectHeader(largeSize, idSize, offsetSize)
        writeLong(writeBuffer, writePos + 1, numFields.toLong(), sizeBytes)

        val idStart = writePos + 1 + sizeBytes
        val offsetStart = idStart + numFields * idSize
        var currOffset = 0

        // Loop over all fields and write the key id, offset, and data to the appropriate offsets
        for (i in 0..<numFields) {
            val field = fields[i]
            writeLong(writeBuffer, idStart + i * idSize, field.id.toLong(), idSize)
            writeLong(writeBuffer, offsetStart + i * offsetSize, currOffset.toLong(), offsetSize)
            System.arraycopy(
                objectBuilder.writeBuffer,
                field.offset,
                writeBuffer,
                writePos + dataOffset + currOffset,
                field.valueSize
            )
            currOffset += field.valueSize
        }
        writeLong(writeBuffer, offsetStart + numFields * offsetSize, dataSize.toLong(), offsetSize)
        writePos += dataOffset + dataSize
        this.objectBuilder = null
    }

    /**
     * Starts appending an array to this variant builder. The returned VariantArrayBuilder is used to
     * append values ot the array. startArray() must be called before endArray(). No append*() methods
     * can be called in between startArray() and endArray().
     *
     * Example usage:
     * VariantBuilder builder = new VariantBuilder();
     * VariantArrayBuilder arrayBuilder = builder.startArray();
     * arrayBuilder.appendString("value1");
     * arrayBuilder.appendString("value2");
     * builder.endArray();
     *
     * @return a VariantArrayBuilder to use for startArrayElement() and endArray().
     */
    fun startArray(): VariantArrayBuilder {
        onStartNested()
        check(objectBuilder == null) { "Cannot call startArray() without calling endObject() first." }
        check(arrayBuilder == null) { "Cannot call startArray() without calling endArray() first." }
        return VariantArrayBuilder(this).also {
            this.arrayBuilder = it
        }
    }

    /**
     * Ends appending an array to this variant builder. This method must be called after all elements
     * have been added to the array.
     */
    fun endArray() {
        val arrayBuilder = checkNotNull(arrayBuilder) { "Cannot call endArray() without calling startArray() first." }
        val offsets = arrayBuilder.validateAndGetOffsets()
        val numElements = offsets.size
        val dataSize: Int = arrayBuilder.writePos
        val largeSize = numElements > VariantUtil.U8_MAX
        val sizeBytes = if (largeSize) VariantUtil.U32_SIZE else 1
        val offsetSize = getMinIntegerSize(dataSize)
        // The data starts after: the header byte, object size, and offset list.
        val dataOffset = 1 + sizeBytes + (numElements + 1) * offsetSize
        checkCapacity(dataOffset + dataSize)

        // Copy all the element data to the write buffer.
        System.arraycopy(arrayBuilder.writeBuffer, 0, writeBuffer, writePos + dataOffset, dataSize)

        writeBuffer[writePos] = arrayHeader(largeSize, offsetSize)
        writeLong(writeBuffer, writePos + 1, numElements.toLong(), sizeBytes)

        val offsetStart = writePos + 1 + sizeBytes
        for (i in 0..<numElements) {
            writeLong(writeBuffer, offsetStart + i * offsetSize, offsets[i].toLong(), offsetSize)
        }
        writeLong(writeBuffer, offsetStart + numElements * offsetSize, dataSize.toLong(), offsetSize)
        writePos += dataOffset + dataSize
        this.arrayBuilder = null
    }

    protected open fun onAppend() {
        checkAppendWhileNested()
        check(writePos <= 0) { "Cannot call multiple append() methods." }
    }

    protected open fun onStartNested() {
        checkMultipleNested("Cannot call startObject()/startArray() without calling endObject()/endArray() first.")
    }

    protected fun checkMultipleNested(message: String) {
        check(!(objectBuilder != null || arrayBuilder != null)) { message }
    }

    protected fun checkAppendWhileNested() {
        check(objectBuilder == null) { "Cannot call append() methods while an object is being built. Must call endObject() first." }
        check(arrayBuilder == null) { "Cannot call append() methods while an array is being built. Must call endArray() first." }
    }

    /**
     * Adds a key to the Variant dictionary. If the key already exists, the dictionary is unmodified.
     * @param key the key to add
     * @return the id of the key
     */
    open fun addDictionaryKey(key: String): Int {
        return dictionary.computeIfAbsent(key) { newKey ->
            dictionaryKeys.size.also {
                dictionaryKeys.add(newKey.toByteArray(StandardCharsets.UTF_8))
            }
        }
    }

    /**
     * @return the current write position of the variant builder
     */
    fun writePos(): Int {
        return writePos
    }

    /**
     * Class to store the information of a Variant object field. We need to collect all fields of
     * an object, sort them by their keys, and build the Variant object in sorted order.
     */
    class FieldEntry(val key: String, val id: Int, val offset: Int) : Comparable<FieldEntry> {
        var valueSize: Int = 0

        fun updateValueSize(size: Int) {
            valueSize = size
        }

        override fun compareTo(other: FieldEntry): Int {
            return key.compareTo(other.key)
        }
    }

    private fun checkCapacity(additionalBytes: Int) {
        val requiredBytes = writePos + additionalBytes
        if (requiredBytes > writeBuffer.size) {
            // Allocate a new buffer with a capacity of the next power of 2 of `requiredBytes`.
            var newCapacity = Integer.highestOneBit(requiredBytes)
            newCapacity = if (newCapacity < requiredBytes) newCapacity * 2 else newCapacity
            val newValue = ByteArray(newCapacity)
            System.arraycopy(writeBuffer, 0, newValue, 0, writePos)
            this.writeBuffer = newValue
        }
    }

    protected fun getMinIntegerSize(value: Int): Int {
        assert(value >= 0)
        return when {
            value <= VariantUtil.U8_MAX -> VariantUtil.U8_SIZE
            value <= VariantUtil.U16_MAX -> VariantUtil.U16_SIZE
            value <= VariantUtil.U24_MAX -> VariantUtil.U24_SIZE
            else -> VariantUtil.U32_SIZE
        }
    }
}
