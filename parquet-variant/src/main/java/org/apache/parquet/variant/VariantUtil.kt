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
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.*


/**
 * This class defines constants related to the Variant format and provides functions for
 * manipulating Variant binaries.
 *
 * A Variant is made up of 2 binaries: value and metadata. A Variant value consists of a one-byte
 * header and a number of content bytes (can be zero). The header byte is divided into upper 6 bits
 * (called "type info") and lower 2 bits (called "basic type"). The content format is explained in
 * the below constants for all possible basic type and type info values.
 *
 * The Variant metadata includes a version id and a dictionary of distinct strings (case-sensitive).
 * Its binary format is:
 * - Version: 1-byte unsigned integer. The only acceptable value is 1 currently.
 * - Dictionary size: 4-byte little-endian unsigned integer. The number of keys in the
 * dictionary.
 * - Offsets: (size + 1) * 4-byte little-endian unsigned integers. `offsets[i]` represents the
 * starting position of string i, counting starting from the address of `offsets[0]`. Strings
 * must be stored contiguously, so we don’t need to store the string size, instead, we compute it
 * with `offset[i + 1] - offset[i]`.
 * - UTF-8 string data.
 */
internal object VariantUtil {
    const val BASIC_TYPE_BITS: Int = 2
    const val BASIC_TYPE_MASK: Int = 3
    const val PRIMITIVE_TYPE_MASK: Int = 63

    /** The inclusive maximum value of the type info value. It is the size limit of `SHORT_STR`.  */
    const val MAX_SHORT_STR_SIZE: Int = 63

    // The basic types
    /**
     * Primitive value.
     * The type info value must be one of the values in the "Primitive" section below.
     */
    const val PRIMITIVE: Int = 0

    /**
     * Short string value.
     * The type info value is the string size, which must be in `[0, MAX_SHORT_STR_SIZE]`.
     * The string content bytes directly follow the header byte.
     */
    const val SHORT_STR: Int = 1

    /**
     * Object value.
     * The content contains a size, a list of field ids, a list of field offsets, and
     * the actual field values. The list of field ids has `size` ids, while the list of field offsets
     * has `size + 1` offsets, where the last offset represents the total size of the field values
     * data. The list of fields ids must be sorted by the field name in alphabetical order.
     * Duplicate field names within one object are not allowed.
     * 5 bits in the type info are used to specify the integer type of the object header. It is
     * 0_b4_b3b2_b1b0 (most significant bit is 0), where:
     * - b4: the integer type of size. When it is 0/1, `size` is a little-endian 1/4-byte
     * unsigned integer.
     * - b3b2: the integer type of ids. When the 2 bits are 0/1/2, the id list contains
     * 1/2/3-byte little-endian unsigned integers.
     * - b1b0: the integer type of offset. When the 2 bits are 0/1/2, the offset list contains
     * 1/2/3-byte little-endian unsigned integers.
     */
    const val OBJECT: Int = 2

    /**
     * Array value.
     * The content contains a size, a list of field offsets, and the actual element values.
     * It is similar to an object without the id list. The length of the offset list
     * is `size + 1`, where the last offset represent the total size of the element data.
     * Its type info is: 000_b2_b1b0:
     * - b2: the type of size.
     * - b1b0: the integer type of offset.
     */
    const val ARRAY: Int = 3

    // The primitive types
    /** JSON Null value. Empty content.  */
    const val NULL: Int = 0

    /** True value. Empty content.  */
    const val TRUE: Int = 1

    /** False value. Empty content.  */
    const val FALSE: Int = 2

    /** 1-byte little-endian signed integer.  */
    const val INT8: Int = 3

    /** 2-byte little-endian signed integer.  */
    const val INT16: Int = 4

    /** 4-byte little-endian signed integer.  */
    const val INT32: Int = 5

    /** 4-byte little-endian signed integer.  */
    const val INT64: Int = 6

    /** 8-byte IEEE double.  */
    const val DOUBLE: Int = 7

    /** 4-byte decimal. Content is 1-byte scale + 4-byte little-endian signed integer.  */
    const val DECIMAL4: Int = 8

    /** 8-byte decimal. Content is 1-byte scale + 8-byte little-endian signed integer.  */
    const val DECIMAL8: Int = 9

    /** 16-byte decimal. Content is 1-byte scale + 16-byte little-endian signed integer.  */
    const val DECIMAL16: Int = 10

    /**
     * Date value. Content is 4-byte little-endian signed integer that represents the
     * number of days from the Unix epoch.
     */
    const val DATE: Int = 11

    /**
     * Timestamp value. Content is 8-byte little-endian signed integer that represents the number of
     * microseconds elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC. It is displayed to users in
     * their local time zones and may be displayed differently depending on the execution environment.
     */
    const val TIMESTAMP_TZ: Int = 12

    /**
     * Timestamp_ntz value. It has the same content as `TIMESTAMP` but should always be interpreted
     * as if the local time zone is UTC.
     */
    const val TIMESTAMP_NTZ: Int = 13

    /** 4-byte IEEE float.  */
    const val FLOAT: Int = 14

    /**
     * Binary value. The content is (4-byte little-endian unsigned integer representing the binary
     * size) + (size bytes of binary content).
     */
    const val BINARY: Int = 15

    /**
     * Long string value. The content is (4-byte little-endian unsigned integer representing the
     * string size) + (size bytes of string content).
     */
    const val LONG_STR: Int = 16

    /**
     * Time value. Values can be from 00:00:00 to 23:59:59.999999.
     * Content is 8-byte little-endian unsigned integer that represents the number of microseconds
     * since midnight.
     */
    const val TIME: Int = 17

    /**
     * Timestamp nanos value. Similar to `TIMESTAMP_TZ`, but represents the number of nanoseconds
     * elapsed since the Unix epoch, 1970-01-01 00:00:00 UTC.
     */
    const val TIMESTAMP_NANOS_TZ: Int = 18

    /**
     * Timestamp nanos (without timestamp) value. It has the same content as `TIMESTAMP_NANOS_TZ` but
     * should always be interpreted as if the local time zone is UTC.
     */
    const val TIMESTAMP_NANOS_NTZ: Int = 19

    /**
     * UUID value. The content is a 16-byte binary, encoded using big-endian.
     * For example, UUID 00112233-4455-6677-8899-aabbccddeeff is encoded as the bytes
     * 00 11 22 33 44 55 66 77 88 99 aa bb cc dd ee ff.
     */
    const val UUID: Int = 20

    // The metadata version.
    const val VERSION: Int = 1

    // The lower 4 bits of the first metadata byte contain the version.
    const val VERSION_MASK: Int = 0x0F

    // Constants for various unsigned integer sizes.
    const val U8_MAX: Int = 0xFF
    const val U16_MAX: Int = 0xFFFF
    const val U24_MAX: Int = 0xFFFFFF
    const val U8_SIZE: Int = 1
    const val U16_SIZE: Int = 2
    const val U24_SIZE: Int = 3
    const val U32_SIZE: Int = 4

    // Max decimal precision for each decimal type.
    const val MAX_DECIMAL4_PRECISION: Int = 9
    const val MAX_DECIMAL8_PRECISION: Int = 18
    const val MAX_DECIMAL16_PRECISION: Int = 38

    // The size (in bytes) of a UUID.
    const val UUID_SIZE: Int = 16

    // header bytes
    val HEADER_NULL: Byte = primitiveHeader(NULL)
    val HEADER_LONG_STRING: Byte = primitiveHeader(LONG_STR)
    val HEADER_TRUE: Byte = primitiveHeader(TRUE)
    val HEADER_FALSE: Byte = primitiveHeader(FALSE)
    val HEADER_INT8: Byte = primitiveHeader(INT8)
    val HEADER_INT16: Byte = primitiveHeader(INT16)
    val HEADER_INT32: Byte = primitiveHeader(INT32)
    val HEADER_INT64: Byte = primitiveHeader(INT64)
    val HEADER_DOUBLE: Byte = primitiveHeader(DOUBLE)
    val HEADER_DECIMAL4: Byte = primitiveHeader(DECIMAL4)
    val HEADER_DECIMAL8: Byte = primitiveHeader(DECIMAL8)
    val HEADER_DECIMAL16: Byte = primitiveHeader(DECIMAL16)
    val HEADER_DATE: Byte = primitiveHeader(DATE)
    val HEADER_TIMESTAMP_TZ: Byte = primitiveHeader(TIMESTAMP_TZ)
    val HEADER_TIMESTAMP_NTZ: Byte = primitiveHeader(TIMESTAMP_NTZ)
    val HEADER_TIME: Byte = primitiveHeader(TIME)
    val HEADER_TIMESTAMP_NANOS_TZ: Byte = primitiveHeader(TIMESTAMP_NANOS_TZ)
    val HEADER_TIMESTAMP_NANOS_NTZ: Byte = primitiveHeader(TIMESTAMP_NANOS_NTZ)
    val HEADER_FLOAT: Byte = primitiveHeader(FLOAT)
    val HEADER_BINARY: Byte = primitiveHeader(BINARY)
    val HEADER_UUID: Byte = primitiveHeader(UUID)

    fun primitiveHeader(type: Int): Byte {
        return (type shl 2 or PRIMITIVE).toByte()
    }

    fun shortStrHeader(size: Int): Byte {
        return (size shl 2 or SHORT_STR).toByte()
    }

    @JvmStatic
    fun objectHeader(largeSize: Boolean, idSize: Int, offsetSize: Int): Byte {
        return ((((if (largeSize) 1 else 0) shl (BASIC_TYPE_BITS + 4))
                or ((idSize - 1) shl (BASIC_TYPE_BITS + 2))
                or ((offsetSize - 1) shl BASIC_TYPE_BITS)
                or OBJECT)).toByte()
    }

    @JvmStatic
    fun arrayHeader(largeSize: Boolean, offsetSize: Int): Byte {
        return (((if (largeSize) 1 else 0) shl (BASIC_TYPE_BITS + 2)) or ((offsetSize - 1) shl BASIC_TYPE_BITS) or ARRAY).toByte()
    }

    /**
     * Check the validity of an array index `pos`.
     * @param pos The index to check
     * @param length The length of the array
     * @throws IllegalArgumentException if the index is out of bound
     */
    fun checkIndex(pos: Int, length: Int) {
        require(pos >= 0 && pos < length) {
            "Invalid byte-array offset (%d). length: %d".format(pos, length)
        }
    }

    /**
     * Write the least significant `numBytes` bytes in `value` into `bytes[pos, pos + numBytes)` in
     * little endian.
     * @param bytes The byte array to write into
     * @param pos The starting index of the byte array to write into
     * @param value The value to write
     * @param numBytes The number of bytes to write
     */
    fun writeLong(bytes: ByteArray, pos: Int, value: Long, numBytes: Int) {
        for (i in 0..<numBytes) {
            bytes[pos + i] = ((value ushr (8 * i)) and 0xFFL).toByte()
        }
    }

    /**
     * Reads a little-endian signed long value from `buffer[pos, pos + numBytes)`.
     * @param buffer The ByteBuffer to read from
     * @param pos The starting index of the buffer to read from
     * @param numBytes The number of bytes to read
     * @return The long value
     */
    fun readLong(buffer: ByteBuffer, pos: Int, numBytes: Int): Long {
        checkIndex(pos, buffer.limit())
        checkIndex(pos + numBytes - 1, buffer.limit())
        var result: Long = 0
        // All bytes except the most significant byte should be unsigned-extended and shifted
        // (so we need & 0xFF`). The most significant byte should be sign-extended and is handled
        // after the loop.
        for (i in 0..<numBytes - 1) {
            val unsignedByteValue = (buffer[pos + i].toInt() and 0xFF).toLong()
            result = result or (unsignedByteValue shl (8 * i))
        }
        val signedByteValue = buffer[pos + numBytes - 1].toLong()
        return result or (signedByteValue shl (8 * (numBytes - 1)))
    }

    /**
     * Read a little-endian unsigned int value from `bytes[pos, pos + numBytes)`. The value must fit
     * into a non-negative int (`[0, Integer.MAX_VALUE]`).
     */
    fun readUnsigned(bytes: ByteBuffer, pos: Int, numBytes: Int): Int {
        checkIndex(pos, bytes.limit())
        checkIndex(pos + numBytes - 1, bytes.limit())
        var result = 0
        // Similar to the `readLong` loop, but all bytes should be unsigned-extended.
        for (i in 0..<numBytes) {
            val unsignedByteValue = bytes[pos + i].toInt() and 0xFF
            result = result or (unsignedByteValue shl (8 * i))
        }
        require(result >= 0) { "Failed to read unsigned int. numBytes: %d".format(numBytes) }
        return result
    }

    /**
     * Returns the value type of Variant value `value[pos...]`. It is only legal to call `get*` if
     * `getType` returns the corresponding type. For example, it is only legal to call
     * `getLong` if this method returns `Type.Long`.
     * @param value The Variant value to get the type from
     * @return The type of the Variant value
     */
    fun getType(value: ByteBuffer): Variant.Type {
        val pos = value.position()
        checkIndex(pos, value.limit())
        val v = value[pos].toInt()
        val basicType = v and BASIC_TYPE_MASK
        val typeInfo = (v shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        return when (basicType) {
            SHORT_STR -> Variant.Type.STRING
            OBJECT -> Variant.Type.OBJECT
            ARRAY -> Variant.Type.ARRAY
            else -> when (typeInfo) {
                NULL -> Variant.Type.NULL
                TRUE, FALSE -> Variant.Type.BOOLEAN
                INT8 -> Variant.Type.BYTE
                INT16 -> Variant.Type.SHORT
                INT32 -> Variant.Type.INT
                INT64 -> Variant.Type.LONG
                DOUBLE -> Variant.Type.DOUBLE
                DECIMAL4 -> Variant.Type.DECIMAL4
                DECIMAL8 -> Variant.Type.DECIMAL8
                DECIMAL16 -> Variant.Type.DECIMAL16
                DATE -> Variant.Type.DATE
                TIMESTAMP_TZ -> Variant.Type.TIMESTAMP_TZ
                TIMESTAMP_NTZ -> Variant.Type.TIMESTAMP_NTZ
                FLOAT -> Variant.Type.FLOAT
                BINARY -> Variant.Type.BINARY
                LONG_STR -> Variant.Type.STRING
                TIME -> Variant.Type.TIME
                TIMESTAMP_NANOS_TZ -> Variant.Type.TIMESTAMP_NANOS_TZ
                TIMESTAMP_NANOS_NTZ -> Variant.Type.TIMESTAMP_NANOS_NTZ
                UUID -> Variant.Type.UUID
                else -> throw UnsupportedOperationException(
                    "Unknown type in Variant. primitive type: %d".format(typeInfo)
                )
            }
        }
    }

    /**
     * Returns the debug string representation of the type of the Variant value `value[pos...]`.
     * @param value The Variant value to get the type from
     * @return The String representation of the type of the Variant value
     */
    private fun getTypeDebugString(value: ByteBuffer): String? {
        return try {
            getType(value).toString()
        } catch (_: Exception) {
            val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
            val valueHeader = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
            "unknownType(basicType: %d, valueHeader: %d)".format(basicType, valueHeader)
        }
    }

    private fun unexpectedType(type: Variant.Type, actualValue: ByteBuffer): IllegalArgumentException {
        val actualType = getTypeDebugString(actualValue)
        return IllegalArgumentException("Cannot read %s value as %s".format(actualType, type))
    }

    private fun unexpectedType(types: Array<Variant.Type>, actualValue: ByteBuffer): IllegalArgumentException {
        val actualType = getTypeDebugString(actualValue)
        return IllegalArgumentException(
            "Cannot read %s value as one of %s".format(actualType, types.contentToString())
        )
    }

    fun getBoolean(value: ByteBuffer): Boolean {
        val pos = value.position()
        checkIndex(pos, value.limit())
        val v = value[pos].toInt()
        val basicType = v and BASIC_TYPE_MASK
        val typeInfo = (v shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE || (typeInfo != TRUE && typeInfo != FALSE)) {
            throw unexpectedType(Variant.Type.BOOLEAN, value)
        }
        return typeInfo == TRUE
    }

    /**
     * Returns a long value from Variant value `value[pos...]`.
     * It is only legal to call it if `getType` returns one of Type.BYTE, SHORT, INT, LONG,
     * DATE, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ.
     * If the type is `DATE`, the return value is guaranteed to fit into an int and
     * represents the number of days from the Unix epoch.
     * If the type is `TIMESTAMP_TZ/TIMESTAMP_NTZ`, the return value represents the number of
     * microseconds from the Unix epoch.
     * If the type is `TIME`, the return value represents the number of microseconds since midnight.
     * If the type is `TIMESTAMP_NANOS_TZ/TIMESTAMP_NANOS_NTZ`, the return value represents the number
     * of nanoseconds from the Unix epoch.
     * @param value The Variant value
     * @return The long value
     */
    fun getLong(value: ByteBuffer): Long {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE) {
            throw unexpectedType(
                types = arrayOf(
                    Variant.Type.BYTE,
                    Variant.Type.SHORT,
                    Variant.Type.INT,
                    Variant.Type.DATE,
                    Variant.Type.LONG,
                    Variant.Type.TIMESTAMP_TZ,
                    Variant.Type.TIMESTAMP_NTZ,
                    Variant.Type.TIME,
                    Variant.Type.TIMESTAMP_NANOS_TZ,
                    Variant.Type.TIMESTAMP_NANOS_NTZ,
                ),
                actualValue = value,
            )
        }
       return when (typeInfo) {
            INT8 -> readLong(value, value.position() + 1, 1)
            INT16 -> readLong(value, value.position() + 1, 2)
            INT32, DATE -> readLong(value, value.position() + 1, 4)
            INT64, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ -> readLong(
                buffer = value,
                pos = value.position() + 1,
                numBytes = 8,
            )
            else -> throw unexpectedType(
                types = arrayOf(
                    Variant.Type.BYTE,
                    Variant.Type.SHORT,
                    Variant.Type.INT,
                    Variant.Type.DATE,
                    Variant.Type.LONG,
                    Variant.Type.TIMESTAMP_TZ,
                    Variant.Type.TIMESTAMP_NTZ,
                    Variant.Type.TIME,
                    Variant.Type.TIMESTAMP_NANOS_TZ,
                    Variant.Type.TIMESTAMP_NANOS_NTZ,
                ),
                actualValue = value,
            )
        }
    }

    /**
     * Similar to getLong(), but for the types: Type.BYTE, SHORT, INT, DATE.
     * @param value The Variant value
     * @return The int value
     */
    fun getInt(value: ByteBuffer): Int {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE) {
            throw unexpectedType(
                types = arrayOf(Variant.Type.BYTE, Variant.Type.SHORT, Variant.Type.INT, Variant.Type.DATE),
                actualValue = value,
            )
        }
        return when (typeInfo) {
            INT8 -> readLong(value, value.position() + 1, 1).toInt()
            INT16 -> readLong(value, value.position() + 1, 2).toInt()
            INT32, DATE -> readLong(value, value.position() + 1, 4).toInt()
            else -> throw unexpectedType(
                types = arrayOf(Variant.Type.BYTE, Variant.Type.SHORT, Variant.Type.INT, Variant.Type.DATE),
                actualValue = value,
            )
        }
    }

    /**
     * Similar to getLong(), but for the types: Type.BYTE, SHORT.
     * @param value The Variant value
     * @return The short value
     */
    fun getShort(value: ByteBuffer): Short {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE) {
            throw unexpectedType(types = arrayOf(Variant.Type.BYTE, Variant.Type.SHORT), actualValue = value)
        }
        return when (typeInfo) {
            INT8 -> readLong(value, value.position() + 1, 1).toShort()
            INT16 -> readLong(value, value.position() + 1, 2).toShort()
            else -> throw unexpectedType(
                types = arrayOf(Variant.Type.BYTE, Variant.Type.SHORT),
                actualValue = value,
            )
        }
    }

    /**
     * Similar to getLong(), but for the types: Type.BYTE, SHORT.
     * @param value The Variant value
     * @return The short value
     */
    fun getByte(value: ByteBuffer): Byte {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE) {
            throw unexpectedType(Variant.Type.BYTE, value)
        }
        return when (typeInfo) {
            INT8 -> readLong(value, value.position() + 1, 1).toByte()
            else -> throw unexpectedType(Variant.Type.BYTE, value)
        }
    }

    fun getDouble(value: ByteBuffer): Double {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE || typeInfo != DOUBLE) {
            throw unexpectedType(Variant.Type.DOUBLE, value)
        }
        return java.lang.Double.longBitsToDouble(readLong(value, value.position() + 1, 8))
    }

    fun getDecimalWithOriginalScale(value: ByteBuffer): BigDecimal {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE) {
            throw unexpectedType(
                types = arrayOf(Variant.Type.DECIMAL4, Variant.Type.DECIMAL8, Variant.Type.DECIMAL16),
                actualValue = value,
            )
        }
        // Interpret the scale byte as unsigned. If it is a negative byte, the unsigned value must be
        // greater than `MAX_DECIMAL16_PRECISION` and will trigger an error in `checkDecimal`.
        val scale = value[value.position() + 1].toInt() and 0xFF
        val result: BigDecimal
        when (typeInfo) {
            DECIMAL4 -> result = BigDecimal.valueOf(readLong(value, value.position() + 2, 4), scale)
            DECIMAL8 -> result = BigDecimal.valueOf(readLong(value, value.position() + 2, 8), scale)
            DECIMAL16 -> {
                checkIndex(value.position() + 17, value.limit())
                val bytes = ByteArray(16)
                // Copy the bytes reversely because the `BigInteger` constructor expects a big-endian
                // representation.
                var i = 0
                while (i < 16) {
                    bytes[i] = value[value.position() + 17 - i]
                    ++i
                }
                result = BigDecimal(BigInteger(bytes), scale)
            }

            else -> throw unexpectedType(
                types = arrayOf(Variant.Type.DECIMAL4, Variant.Type.DECIMAL8, Variant.Type.DECIMAL16),
                actualValue = value,
            )
        }
        return result
    }

    fun getDecimal(value: ByteBuffer): BigDecimal {
        return getDecimalWithOriginalScale(value)
    }

    fun getFloat(value: ByteBuffer): Float {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE || typeInfo != FLOAT) {
            throw unexpectedType(Variant.Type.FLOAT, value)
        }
        return java.lang.Float.intBitsToFloat(readLong(value, value.position() + 1, 4).toInt())
    }

    fun getBinary(value: ByteBuffer): ByteBuffer {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE || typeInfo != BINARY) {
            throw unexpectedType(Variant.Type.BINARY, value)
        }
        val start = value.position() + 1 + U32_SIZE
        val length = readUnsigned(value, value.position() + 1, U32_SIZE)
        checkIndex(start + length - 1, value.limit())
        return slice(value, start)
    }

    fun getString(value: ByteBuffer): String {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType == SHORT_STR || (basicType == PRIMITIVE && typeInfo == LONG_STR)) {
            val start: Int
            val length: Int
            if (basicType == SHORT_STR) {
                start = value.position() + 1
                length = typeInfo
            } else {
                start = value.position() + 1 + U32_SIZE
                length = readUnsigned(value, value.position() + 1, U32_SIZE)
            }
            checkIndex(start + length - 1, value.limit())
            if (value.hasArray()) {
                // If the buffer is backed by an array, we can use the array directly.
                return String(value.array(), value.arrayOffset() + start, length)
            } else {
                // If the buffer is not backed by an array, we need to copy the bytes into a new array.
                val valueArray = ByteArray(length)
                slice(value, start).get(valueArray)
                return String(valueArray)
            }
        }
        throw unexpectedType(Variant.Type.STRING, value)
    }

    fun getUUID(value: ByteBuffer): UUID {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != PRIMITIVE || typeInfo != UUID) {
            throw unexpectedType(Variant.Type.UUID, value)
        }
        val start = value.position() + 1
        checkIndex(start + UUID_SIZE - 1, value.limit())
        val bb = slice(value, start).order(ByteOrder.BIG_ENDIAN)
        return UUID(bb.getLong(), bb.getLong())
    }

    /**
     * Slices the `value` buffer starting from `start` index.
     * @param value The ByteBuffer to slice
     * @param start The starting index of the slice
     * @return The sliced ByteBuffer
     */
    fun slice(value: ByteBuffer, start: Int): ByteBuffer {
        return value.duplicate().also {
            it.position(start)
        }
    }

    /**
     * Parses the object at `value[pos...]`, and returns the object details.
     */
    fun getObjectInfo(value: ByteBuffer): ObjectInfo {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != OBJECT) {
            throw unexpectedType(Variant.Type.OBJECT, value)
        }
        // Refer to the comment of the `OBJECT` constant for the details of the object header encoding.
        // Suppose `typeInfo` has a bit representation of 0_b4_b3b2_b1b0, the following line extracts
        // b4 to determine whether the object uses a 1/4-byte size.
        val largeSize = ((typeInfo shr 4) and 0x1) != 0
        val sizeBytes = if (largeSize) U32_SIZE else 1
        val numElements = readUnsigned(value, value.position() + 1, sizeBytes)
        // Extracts b3b2 to determine the integer size of the field id list.
        val idSize = ((typeInfo shr 2) and 0x3) + 1
        // Extracts b1b0 to determine the integer size of the offset list.
        val offsetSize = (typeInfo and 0x3) + 1
        val idStartOffset = 1 + sizeBytes
        val offsetStartOffset = idStartOffset + numElements * idSize
        val dataStartOffset = offsetStartOffset + (numElements + 1) * offsetSize
        return ObjectInfo(numElements, idSize, offsetSize, idStartOffset, offsetStartOffset, dataStartOffset)
    }

    /**
     * Parses the array at `value[pos...]`, and returns the array details.
     */
    fun getArrayInfo(value: ByteBuffer): ArrayInfo {
        checkIndex(value.position(), value.limit())
        val basicType = value[value.position()].toInt() and BASIC_TYPE_MASK
        val typeInfo = (value[value.position()].toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
        if (basicType != ARRAY) {
            throw unexpectedType(Variant.Type.ARRAY, value)
        }
        // Refer to the comment of the `ARRAY` constant for the details of the object header encoding.
        // Suppose `typeInfo` has a bit representation of 000_b2_b1b0, the following line extracts
        // b2 to determine whether the object uses a 1/4-byte size.
        val largeSize = ((typeInfo shr 2) and 0x1) != 0
        val sizeBytes = (if (largeSize) U32_SIZE else 1)
        val numElements = readUnsigned(value, value.position() + 1, sizeBytes)
        // Extracts b1b0 to determine the integer size of the offset list.
        val offsetSize = (typeInfo and 0x3) + 1
        val offsetStartOffset = 1 + sizeBytes
        val dataStartOffset = offsetStartOffset + (numElements + 1) * offsetSize
        return ArrayInfo(numElements, offsetSize, offsetStartOffset, dataStartOffset)
    }

    /**
     * Returns a key at `id` in the Variant metadata.
     *
     * @param metadata The Variant metadata
     * @param id The key id
     * @return The key
     * @throws IllegalArgumentException if the id is out of bound
     * @throws IllegalStateException if the encoded metadata is malformed
     */
    fun getMetadataKey(metadata: ByteBuffer, id: Int): String {
        // Extracts the highest 2 bits in the metadata header to determine the integer size of the
        // offset list.
        val offsetSize = ((metadata[metadata.position()].toInt() shr 6) and 0x3) + 1
        val dictSize = readUnsigned(metadata, metadata.position() + 1, offsetSize)
        require(id < dictSize) { "Invalid dictionary id: %d. dictionary size: %d".format(id, dictSize) }
        // The offset list after the header byte, and a `dictSize` with `offsetSize` bytes.
        val offsetListPos = metadata.position() + 1 + offsetSize
        // The data starts after the offset list, and `(dictSize + 1)` offset values.
        val dataPos = offsetListPos + (dictSize + 1) * offsetSize
        val offset = readUnsigned(metadata, offsetListPos + (id) * offsetSize, offsetSize)
        val nextOffset = readUnsigned(metadata, offsetListPos + (id + 1) * offsetSize, offsetSize)
        check(offset <= nextOffset) { "Invalid offset: %d. next offset: %d".format(offset, nextOffset) }
        checkIndex(dataPos + nextOffset - 1, metadata.limit())
        if (metadata.hasArray() && !metadata.isReadOnly) {
            return String(metadata.array(), metadata.arrayOffset() + dataPos + offset, nextOffset - offset)
        } else {
            // ByteBuffer does not have an array, so we need to use the `get` method to read the bytes.
            val metadataArray = ByteArray(nextOffset - offset)
            slice(metadata, dataPos + offset).get(metadataArray)
            return String(metadataArray)
        }
    }

    /**
     * Returns a map from each string to its ID in the Variant metadata.
     * @param metadata The Variant metadata
     * @return A map from metadata key to its position.
     */
    fun getMetadataMap(metadata: ByteBuffer): Map<String, Int> {
        val pos = metadata.position()
        checkIndex(pos, metadata.limit())

        // Extracts the highest 2 bits in the metadata header to determine the integer size of the
        // offset list.
        val offsetSize = ((metadata.get(pos).toInt() shr 6) and 0x3) + 1
        val dictSize = readUnsigned(metadata, pos + 1, offsetSize)
        val result = mutableMapOf<String, Int>()
        var offset = readUnsigned(metadata, pos + 1 + offsetSize, offsetSize)
        for (id in 0..<dictSize) {
            val stringStart = 1 + (dictSize + 2) * offsetSize
            val nextOffset = readUnsigned(metadata, pos + 1 + (id + 2) * offsetSize, offsetSize)
            if (offset > nextOffset) {
                throw java.lang.UnsupportedOperationException("Invalid offset: %d. next offset: %d".format(offset, nextOffset))
            }
            checkIndex(pos + stringStart + nextOffset - 1, metadata.limit())
            val key = if (metadata.hasArray() && !metadata.isReadOnly()) {
                String(
                    metadata.array(),
                    metadata.arrayOffset() + pos + stringStart + offset,
                    nextOffset - offset,
                )
            } else {
                // ByteBuffer does not have an array, so we need to use the `get` method to read the bytes.
                val metadataArray = ByteArray(nextOffset - offset)
                slice(metadata, stringStart + offset).get(metadataArray)
                String(metadataArray)
            }
            result.put(key, id)
            offset = nextOffset
        }
        return result
    }


    /**
     * Computes the actual size (in bytes) of the Variant value.
     * @param value The Variant value binary
     * @return The size (in bytes) of the Variant value, including the header byte
     */
    fun valueSize(value: ByteBuffer): Int {
        val pos = value.position()
        val basicType = value.get(pos).toInt() and BASIC_TYPE_MASK
        return when (basicType) {
            SHORT_STR -> {
                val stringSize = (value.get(pos).toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
                1 + stringSize
            }

            OBJECT -> {
                val info = getObjectInfo(slice(value, pos))
                info.dataStartOffset + readUnsigned(
                    value,
                    pos + info.offsetStartOffset + info.numElements * info.offsetSize,
                    info.offsetSize,
                )
            }

            ARRAY -> {
                val info = getArrayInfo(slice(value, pos))
                info.dataStartOffset + readUnsigned(
                    value,
                    pos + info.offsetStartOffset + info.numElements * info.offsetSize,
                    info.offsetSize
                )
            }

            else -> {
                val typeInfo = (value.get(pos).toInt() shr BASIC_TYPE_BITS) and PRIMITIVE_TYPE_MASK
                when (typeInfo) {
                    NULL, TRUE, FALSE -> 1
                    INT8 -> 2
                    INT16 -> 3
                    INT32, DATE, FLOAT -> 5
                    INT64, DOUBLE, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ -> 9
                    DECIMAL4 -> 6
                    DECIMAL8 -> 10
                    DECIMAL16 -> 18
                    BINARY, LONG_STR -> 1 + U32_SIZE + readUnsigned(value, pos + 1, U32_SIZE)
                    UUID -> 1 + UUID_SIZE
                    else -> throw java.lang.UnsupportedOperationException("Unknown type in Variant. primitive type: $typeInfo")
                }
            }
        }
    }

    /**
     * A helper class representing the details of a Variant object, used for `ObjectHandler`.
     */
    internal class ObjectInfo(
        /** Number of object fields.  */
        val numElements: Int,
        /** The integer size of the field id list.  */
        val idSize: Int,
        /** The integer size of the offset list.  */
        val offsetSize: Int,
        /** The byte offset (from the beginning of the Variant object) of the field id list.  */
        val idStartOffset: Int,
        /** The byte offset (from the beginning of the Variant object) of the offset list.  */
        val offsetStartOffset: Int,
        /** The byte offset (from the beginning of the Variant object) of the field data.  */
        val dataStartOffset: Int,
    )

    /**
     * A helper class representing the details of a Variant array, used for `ArrayHandler`.
     */
    internal class ArrayInfo(
        /** Number of object fields.  */
        val numElements: Int,
        /** The integer size of the offset list.  */
        val offsetSize: Int,
        /** The byte offset (from the beginning of the Variant array) of the offset list.  */
        val offsetStartOffset: Int,
        /** The byte offset (from the beginning of the Variant array) of the field data.  */
        val dataStartOffset: Int,
    )
}

