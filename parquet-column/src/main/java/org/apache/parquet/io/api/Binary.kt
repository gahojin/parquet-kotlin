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
package org.apache.parquet.io.api

import org.apache.parquet.io.ParquetEncodingException
import org.apache.parquet.schema.PrimitiveComparator
import java.io.DataOutput
import java.io.IOException
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.ObjectStreamException
import java.io.OutputStream
import java.io.Serializable
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.CharBuffer
import java.nio.charset.CharsetEncoder
import java.nio.charset.StandardCharsets
import kotlin.concurrent.getOrSet
import kotlin.math.min

// this isn't really something others should extend
abstract class Binary private constructor() : Comparable<Binary>, Serializable {
    /**
     * Signals if backing bytes are owned, and can be modified, by producer of the Binary
     *
     * @return if backing bytes are held on by producer of the Binary
     */
    open var isBackingBytesReused: Boolean = false
        protected set

    abstract val bytes: ByteArray

    /**
     * Variant of getBytes() that avoids copying backing data structure by returning
     * backing byte[] of the Binary. Do not modify backing byte[] unless you know what
     * you are doing.
     *
     * @return backing byte[] of correct size, with an offset of 0, if possible, else returns result of getBytes()
     */
    abstract val bytesUnsafe: ByteArray

    abstract fun toStringUsingUTF8(): String

    abstract fun length(): Int

    @Throws(IOException::class)
    abstract fun writeTo(out: OutputStream)

    @Throws(IOException::class)
    abstract fun writeTo(out: DataOutput)

    abstract fun slice(start: Int, length: Int): Binary

    abstract fun equals(bytes: ByteArray, offset: Int, length: Int): Boolean

    abstract fun equals(bytes: ByteBuffer, offset: Int, length: Int): Boolean

    abstract fun equals(other: Binary): Boolean

    @Deprecated(
        """will be removed in 2.0.0. The comparison logic depends on the related logical type therefore this one
    might not be correct. The {@link java.util.Comparator} implementation for the related type available at
    {@link org.apache.parquet.schema.PrimitiveType#comparator} should be used instead."""
    )
    abstract override fun compareTo(other: Binary): Int

    abstract fun lexicographicCompare(other: Binary): Int

    abstract fun lexicographicCompare(other: ByteArray, otherOffset: Int, otherLength: Int): Int

    abstract fun lexicographicCompare(other: ByteBuffer, otherOffset: Int, otherLength: Int): Int

    abstract fun toByteBuffer(): ByteBuffer

    open fun get2BytesLittleEndian(): Short {
        throw UnsupportedOperationException("Not implemented")
    }

    override fun equals(obj: Any?): Boolean {
        if (obj == null) {
            return false
        }
        if (obj is Binary) {
            return equals(obj)
        }
        return false
    }

    override fun toString(): String {
        return ("Binary{${length()} ${if (isBackingBytesReused) "reused" else "constant"} bytes, ${bytesUnsafe.contentToString()}}")
    }

    fun copy(): Binary {
        return if (isBackingBytesReused) {
            fromConstantByteArray(bytes)
        } else {
            this
        }
    }

    private class ByteArraySliceBackedBinary(
        private val value: ByteArray,
        private val offset: Int,
        private val length: Int,
        override var isBackingBytesReused: Boolean,
    ) : Binary() {
        override val bytes: ByteArray
            get() = value.copyOfRange(offset, offset + length)

        // Backing array is larger than the slice used for this Binary.
        override val bytesUnsafe: ByteArray
            get() = bytes

        override fun toStringUsingUTF8(): String {
            // Charset#decode uses a thread-local decoder cache and is faster than
            // new String(...) which instantiates a new Decoder per invocation
            return StandardCharsets.UTF_8
                .decode(ByteBuffer.wrap(value, offset, length))
                .toString()
        }

        override fun length(): Int {
            return length
        }

        @Throws(IOException::class)
        override fun writeTo(out: OutputStream) {
            out.write(value, offset, length)
        }

        override fun slice(start: Int, length: Int): Binary {
            return if (isBackingBytesReused) {
                fromReusedByteArray(value, offset + start, length)
            } else {
                fromConstantByteArray(value, offset + start, length)
            }
        }

        override fun hashCode(): Int {
            return hashCode(value, offset, length)
        }

        override fun equals(other: Binary): Boolean {
            return other.equals(value, offset, length)
        }

        override fun equals(other: ByteArray, otherOffset: Int, otherLength: Int): Boolean {
            return equals(value, offset, length, other, otherOffset, otherLength)
        }

        override fun equals(bytes: ByteBuffer, otherOffset: Int, otherLength: Int): Boolean {
            return equals(value, offset, length, bytes, otherOffset, otherLength)
        }

        @Deprecated("will be removed in 2.0.0. The comparison logic depends on the related logical type therefore this one\n    might not be correct. The {@link java.util.Comparator} implementation for the related type available at\n    {@link org.apache.parquet.schema.PrimitiveType#comparator} should be used instead.")
        override fun compareTo(other: Binary): Int {
            return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(this, other)
        }

        override fun lexicographicCompare(other: Binary): Int {
            // NOTE: We have to flip the sign, since we swap operands sides
            return -other.lexicographicCompare(value, offset, length)
        }

        override fun lexicographicCompare(other: ByteArray, otherOffset: Int, otherLength: Int): Int {
            return lexicographicCompare(value, offset, length, other, otherOffset, otherLength)
        }

        override fun lexicographicCompare(other: ByteBuffer, otherOffset: Int, otherLength: Int): Int {
            return lexicographicCompare(value, offset, length, other, otherOffset, otherLength)
        }

        override fun toByteBuffer(): ByteBuffer {
            return ByteBuffer.wrap(value, offset, length)
        }

        override fun get2BytesLittleEndian(): Short {
            require(length == 2) { "length must be 2" }

            return (((value[offset + 1].toInt() and 0xff) shl 8) or (value[offset].toInt() and 0xff)).toShort()
        }

        @Throws(IOException::class)
        override fun writeTo(out: DataOutput) {
            out.write(value, offset, length)
        }
    }

    private class FromStringBinary(value: String) : ByteBufferBackedBinary(encodeUTF8(value), false) {
        override fun toString(): String {
            return "Binary{\"${toStringUsingUTF8()}\"}"
        }

        companion object {
            private fun encodeUTF8(value: String): ByteBuffer {
                return ByteBuffer.wrap(value.toByteArray(StandardCharsets.UTF_8))
            }
        }
    }

    private class FromCharSequenceBinary(value: CharSequence) : ByteBufferBackedBinary(encodeUTF8(value), false) {
        override fun toString(): String {
            return "Binary{\"${toStringUsingUTF8()}\"}"
        }

        companion object {
            private val ENCODER = ThreadLocal<CharsetEncoder>()

            private fun encodeUTF8(value: CharSequence): ByteBuffer {
                try {
                    return ENCODER.getOrSet { StandardCharsets.UTF_8.newEncoder() }
                        .encode(CharBuffer.wrap(value))
                } catch (e: CharacterCodingException) {
                    throw ParquetEncodingException("UTF-8 not supported.", e)
                }
            }
        }
    }

    private class ByteArrayBackedBinary(
        private val value: ByteArray,
        override var isBackingBytesReused: Boolean,
    ) : Binary() {
        override val bytes: ByteArray
            get() = value.copyOf()

        override val bytesUnsafe: ByteArray
            get() = value

        override fun toStringUsingUTF8(): String {
            return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(value)).toString()
        }

        override fun length(): Int {
            return value.size
        }

        @Throws(IOException::class)
        override fun writeTo(out: OutputStream) {
            out.write(value)
        }

        override fun slice(start: Int, length: Int): Binary {
            return if (isBackingBytesReused) {
                fromReusedByteArray(value, start, length)
            } else {
                fromConstantByteArray(value, start, length)
            }
        }

        override fun hashCode(): Int {
            return hashCode(value, 0, value.size)
        }

        override fun equals(other: Binary): Boolean {
            return other.equals(value, 0, value.size)
        }

        override fun equals(other: ByteArray, otherOffset: Int, otherLength: Int): Boolean {
            return equals(value, 0, value.size, other, otherOffset, otherLength)
        }

        override fun equals(bytes: ByteBuffer, otherOffset: Int, otherLength: Int): Boolean {
            return equals(value, 0, value.size, bytes, otherOffset, otherLength)
        }

        @Deprecated("will be removed in 2.0.0. The comparison logic depends on the related logical type therefore this one\n    might not be correct. The {@link java.util.Comparator} implementation for the related type available at\n    {@link org.apache.parquet.schema.PrimitiveType#comparator} should be used instead.")
        override fun compareTo(other: Binary): Int {
            return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(this, other)
        }

        override fun lexicographicCompare(other: Binary): Int {
            // NOTE: We have to flip the sign, since we swap operands sides
            return -other.lexicographicCompare(value, 0, value.size)
        }

        override fun lexicographicCompare(other: ByteArray, otherOffset: Int, otherLength: Int): Int {
            return lexicographicCompare(this.value, 0, value.size, other, otherOffset, otherLength)
        }

        override fun lexicographicCompare(other: ByteBuffer, otherOffset: Int, otherLength: Int): Int {
            return lexicographicCompare(this.value, 0, value.size, other, otherOffset, otherLength)
        }

        override fun toByteBuffer(): ByteBuffer {
            return ByteBuffer.wrap(value)
        }

        override fun get2BytesLittleEndian(): Short {
            require(value.size == 2) { "length must be 2" }

            return (((value[1].toInt() and 0xff) shl 8) or (value[0].toInt() and 0xff)).toShort()
        }

        @Throws(IOException::class)
        override fun writeTo(out: DataOutput) {
            out.write(value)
        }
    }

    private open class ByteBufferBackedBinary(
        private var value: ByteBuffer,
        private var offset: Int,
        private var length: Int,
        override var isBackingBytesReused: Boolean,
    ) : Binary() {

        @Transient
        private var cachedBytes: ByteArray? = null

        override val bytes: ByteArray
            get() {
                val bytes = ByteArray(length)

                val limit = value.limit()
                value.limit(offset + length)
                val position = value.position()
                value.position(offset)
                value.get(bytes)
                value.limit(limit)
                value.position(position)
                if (!isBackingBytesReused) { // backing buffer might change
                    cachedBytes = bytes
                }
                return bytes
            }

        override val bytesUnsafe: ByteArray
            get() = cachedBytes ?: bytes

        constructor(value: ByteBuffer, isBackingBytesReused: Boolean) : this(
            value = value,
            offset = value.position(),
            length = value.remaining(),
            isBackingBytesReused = isBackingBytesReused,
        )

        override fun toStringUsingUTF8(): String {
            val ret: String
            if (value.hasArray()) {
                ret = String(value.array(), value.arrayOffset() + offset, length, StandardCharsets.UTF_8)
            } else {
                val limit = value.limit()
                value.limit(offset + length)
                val position = value.position()
                value.position(offset)
                // no corresponding interface to read a subset of a buffer, would have to slice it
                // which creates another ByteBuffer object or do what is done here to adjust the
                // limit/offset and set them back after
                ret = StandardCharsets.UTF_8.decode(value).toString()
                value.limit(limit)
                value.position(position)
            }

            return ret
        }

        override fun length(): Int {
            return length
        }

        @Throws(IOException::class)
        override fun writeTo(out: OutputStream) {
            if (value.hasArray()) {
                out.write(value.array(), value.arrayOffset() + offset, length)
            } else {
                out.write(bytesUnsafe, 0, length)
            }
        }

        override fun slice(start: Int, length: Int): Binary {
            return fromConstantByteArray(bytesUnsafe, start, length)
        }

        override fun hashCode(): Int {
            return if (value.hasArray()) {
                hashCode(value.array(), value.arrayOffset() + offset, length)
            } else {
                hashCode(value, offset, length)
            }
        }

        override fun equals(other: Binary): Boolean {
            return if (value.hasArray()) {
                other.equals(value.array(), value.arrayOffset() + offset, length)
            } else {
                other.equals(value, offset, length)
            }
        }

        override fun equals(other: ByteArray, otherOffset: Int, otherLength: Int): Boolean {
            return if (value.hasArray()) {
                equals(value.array(), value.arrayOffset() + offset, length, other, otherOffset, otherLength)
            } else {
                equals(other, otherOffset, otherLength, value, offset, length)
            }
        }

        override fun equals(otherBytes: ByteBuffer, otherOffset: Int, otherLength: Int): Boolean {
            return equals(value, 0, length, otherBytes, otherOffset, otherLength)
        }

        @Deprecated("will be removed in 2.0.0. The comparison logic depends on the related logical type therefore this one\n    might not be correct. The {@link java.util.Comparator} implementation for the related type available at\n    {@link org.apache.parquet.schema.PrimitiveType#comparator} should be used instead.")
        override fun compareTo(other: Binary): Int {
            return PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR.compare(this, other)
        }

        override fun lexicographicCompare(other: Binary): Int {
            return if (value.hasArray()) {
                // NOTE: We have to flip the sign, since we swap operands sides
                -other.lexicographicCompare(value.array(), value.arrayOffset() + offset, length)
            } else {
                // NOTE: We have to flip the sign, since we swap operands sides
                -other.lexicographicCompare(value, offset, length)
            }
        }

        override fun lexicographicCompare(other: ByteArray, otherOffset: Int, otherLength: Int): Int {
            return if (value.hasArray()) {
                lexicographicCompare(value.array(), value.arrayOffset() + offset, length, other, otherOffset, otherLength)
            } else {
                // NOTE: We have to flip the sign, since we swap operands sides
                -lexicographicCompare(other, otherOffset, otherLength, value, offset, length)
            }
        }

        override fun lexicographicCompare(other: ByteBuffer, otherOffset: Int, otherLength: Int): Int {
            return lexicographicCompare(value, offset, length, other, otherOffset, otherLength)
        }

        override fun toByteBuffer(): ByteBuffer {
            return value.duplicate().also {
                it.position(offset)
                it.limit(offset + length)
            }
        }

        override fun get2BytesLittleEndian(): Short {
            require(length == 2) { "length must be 2" }

            return value.order(ByteOrder.LITTLE_ENDIAN).getShort(offset)
        }

        @Throws(IOException::class)
        override fun writeTo(out: DataOutput) {
            // TODO: should not have to materialize those bytes
            out.write(bytesUnsafe)
        }

        @Throws(IOException::class)
        private fun writeObject(out: ObjectOutputStream) {
            val bytes = bytesUnsafe
            out.writeInt(bytes.size)
            out.write(bytes)
        }

        @Throws(IOException::class, ClassNotFoundException::class)
        private fun readObject(`in`: ObjectInputStream) {
            val length = `in`.readInt()
            val bytes = ByteArray(length)
            `in`.readFully(bytes, 0, length)
            value = ByteBuffer.wrap(bytes)
            offset = 0
            this.length = length
        }

        @Throws(ObjectStreamException::class)
        private fun readObjectNoData() {
            value = ByteBuffer.wrap(ByteArray(0))
        }
    }

    companion object {
        private const val serialVersionUID = 1L

        @JvmField
        val EMPTY: Binary = fromConstantByteArray(ByteArray(0))

        @JvmStatic
        fun fromReusedByteArray(value: ByteArray, offset: Int, length: Int): Binary {
            return ByteArraySliceBackedBinary(value, offset, length, true)
        }

        @JvmStatic
        fun fromConstantByteArray(value: ByteArray, offset: Int, length: Int): Binary {
            return ByteArraySliceBackedBinary(value, offset, length, false)
        }

        @Deprecated("")
        @JvmStatic
        fun fromByteArray(value: ByteArray, offset: Int, length: Int): Binary {
            return fromReusedByteArray(value, offset, length) // Assume producer intends to reuse byte[]
        }

        @JvmStatic
        fun fromReusedByteArray(value: ByteArray): Binary {
            return ByteArrayBackedBinary(value, true)
        }

        @JvmStatic
        fun fromConstantByteArray(value: ByteArray): Binary {
            return ByteArrayBackedBinary(value, false)
        }

        @Deprecated("")
        @JvmStatic
        fun fromByteArray(value: ByteArray): Binary {
            return fromReusedByteArray(value) // Assume producer intends to reuse byte[]
        }

        @JvmStatic
        fun fromReusedByteBuffer(value: ByteBuffer, offset: Int, length: Int): Binary {
            return ByteBufferBackedBinary(value, offset, length, true)
        }

        @JvmStatic
        fun fromConstantByteBuffer(value: ByteBuffer, offset: Int, length: Int): Binary {
            return ByteBufferBackedBinary(value, offset, length, false)
        }

        @JvmStatic
        fun fromReusedByteBuffer(value: ByteBuffer): Binary {
            return ByteBufferBackedBinary(value, true)
        }

        @JvmStatic
        fun fromConstantByteBuffer(value: ByteBuffer): Binary {
            return ByteBufferBackedBinary(value, false)
        }

        @Deprecated("")
        @JvmStatic
        fun fromByteBuffer(value: ByteBuffer): Binary {
            return fromReusedByteBuffer(value) // Assume producer intends to reuse byte[]
        }

        @JvmStatic
        fun fromString(value: String): Binary {
            return FromStringBinary(value)
        }

        @JvmStatic
        fun fromCharSequence(value: CharSequence): Binary {
            return FromCharSequenceBinary(value)
        }

        @JvmStatic
        fun lexicographicCompare(one: Binary, other: Binary): Int {
            return one.lexicographicCompare(other)
        }

        /**
         * @param array
         * @param offset
         * @param length
         * @return
         * @see {@link Arrays.hashCode
         */
        private fun hashCode(array: ByteArray, offset: Int, length: Int): Int {
            var result = 1
            for (i in offset..<offset + length) {
                val b = array[i]
                result = 31 * result + b
            }
            return result
        }

        private fun hashCode(buf: ByteBuffer, offset: Int, length: Int): Int {
            var result = 1
            for (i in offset..<offset + length) {
                val b = buf.get(i)
                result = 31 * result + b
            }
            return result
        }

        private fun equals(
            buf1: ByteBuffer, offset1: Int, length1: Int, buf2: ByteBuffer, offset2: Int, length2: Int,
        ): Boolean {
            if (length1 != length2) return false
            for (i in 0..<length1) {
                if (buf1.get(i + offset1) != buf2.get(i + offset2)) {
                    return false
                }
            }
            return true
        }

        private fun equals(
            array1: ByteArray, offset1: Int, length1: Int, buf: ByteBuffer, offset2: Int, length2: Int
        ): Boolean {
            if (length1 != length2) return false
            for (i in 0..<length1) {
                if (array1[i + offset1] != buf.get(i + offset2)) {
                    return false
                }
            }
            return true
        }

        /**
         * @param array1
         * @param offset1
         * @param length1
         * @param array2
         * @param offset2
         * @param length2
         * @return
         * @see {@link Arrays.equals
         */
        private fun equals(
            array1: ByteArray, offset1: Int, length1: Int, array2: ByteArray, offset2: Int, length2: Int
        ): Boolean {
            if (length1 != length2) return false
            if (array1 == array2 && offset1 == offset2) return true
            for (i in 0..<length1) {
                if (array1[i + offset1] != array2[i + offset2]) {
                    return false
                }
            }
            return true
        }

        private fun lexicographicCompare(
            array1: ByteArray, offset1: Int, length1: Int, array2: ByteArray, offset2: Int, length2: Int
        ): Int {
            val minLen = min(length1.toDouble(), length2.toDouble()).toInt()
            for (i in 0..<minLen) {
                val res: Int = unsignedCompare(array1[i + offset1], array2[i + offset2])
                if (res != 0) {
                    return res
                }
            }

            return length1 - length2
        }

        private fun lexicographicCompare(
            array: ByteArray, offset1: Int, length1: Int, buffer: ByteBuffer, offset2: Int, length2: Int
        ): Int {
            val minLen = min(length1.toDouble(), length2.toDouble()).toInt()
            for (i in 0..<minLen) {
                val res: Int = unsignedCompare(array[i + offset1], buffer.get(i + offset2))
                if (res != 0) {
                    return res
                }
            }

            return length1 - length2
        }

        private fun lexicographicCompare(
            buffer1: ByteBuffer, offset1: Int, length1: Int, buffer2: ByteBuffer, offset2: Int, length2: Int
        ): Int {
            val minLen = min(length1.toDouble(), length2.toDouble()).toInt()
            for (i in 0..<minLen) {
                val res: Int = unsignedCompare(buffer1.get(i + offset1), buffer2.get(i + offset2))
                if (res != 0) {
                    return res
                }
            }

            return length1 - length2
        }

        private fun unsignedCompare(b1: Byte, b2: Byte): Int {
            return toUnsigned(b1) - toUnsigned(b2)
        }

        private fun toUnsigned(b: Byte): Int {
            return b.toInt() and 0xFF
        }
    }
}
