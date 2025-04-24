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
import org.junit.Assert
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.time.LocalDate
import java.util.Arrays

class TestVariantArray {
    private fun checkType(v: Variant?, expectedBasicType: Int, expectedType: Variant.Type?) {
        requireNotNull(v)
        Assert.assertEquals(
            expectedBasicType.toLong(),
            (v.value.get(v.value.position()).toInt() and VariantUtil.BASIC_TYPE_MASK).toLong()
        )
        Assert.assertEquals(expectedType, v.type)
    }

    private fun randomString(len: Int) = buildString(len) {
        for (i in 0..<len) {
            append(RANDOM_CHARS[random.nextInt(RANDOM_CHARS.length)])
        }
    }

    private fun testVariant(v: Variant, consumer: (Variant) -> Unit) {
        consumer(v)
        // Create new Variant with different byte offsets
        val newValue = ByteArray(v.value.capacity() + 50)
        val newMetadata = ByteArray(v.metadata.capacity() + 50)
        Arrays.fill(newValue, 0xFF.toByte())
        Arrays.fill(newMetadata, 0xFF.toByte())
        v.value.position(0)
        v.value.get(newValue, 25, v.value.capacity())
        v.value.position(0)
        v.metadata.position(0)
        v.metadata.get(newMetadata, 25, v.metadata.capacity())
        v.metadata.position(0)
        val v2 = Variant(
            ByteBuffer.wrap(newValue, 25, v.value.capacity()),
            ByteBuffer.wrap(newMetadata, 25, v.metadata.capacity())
        )
        consumer(v2)
    }

    @Test
    fun testEmptyArray() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(3, 0x00)), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(0, v.numArrayElements().toLong())
        }
    }

    @Test
    fun testEmptyLargeArray() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(19, 0x00, 0x00, 0x00, 0x00)), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(0, v.numArrayElements().toLong())
        }
    }

    @Test
    fun testLargeArraySize() {
        val value = Variant(
            ByteBuffer.wrap(byteArrayOf(19, 0xFF.toByte(), 0x01.toByte(), 0x00, 0x00)), EMPTY_METADATA
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(511, v.numArrayElements().toLong())
        }
    }

    @Test
    fun testMixedArray() {
        val nested: ByteArray = constructArray(VALUE_INT, VALUE_NULL, VALUE_SHORT_STRING)
        val value = Variant(
            ByteBuffer.wrap(constructArray(VALUE_DATE, VALUE_BOOL, VALUE_INT, VALUE_STRING, nested)),
            EMPTY_METADATA,
        )

        testVariant(value) { v ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(5, v.numArrayElements().toLong())
            checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.DATE)
            Assert.assertEquals(
                LocalDate.parse("2025-04-17"),
                LocalDate.ofEpochDay(v.getElementAtIndex(0)!!.int.toLong()),
            )
            checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getElementAtIndex(1)!!.boolean)
            checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.getElementAtIndex(2)!!.int.toLong())
            checkType(v.getElementAtIndex(3), VariantUtil.PRIMITIVE, Variant.Type.STRING)
            Assert.assertEquals("variant", v.getElementAtIndex(3)!!.string)
            checkType(v.getElementAtIndex(4), VariantUtil.ARRAY, Variant.Type.ARRAY)

            val nestedV = v.getElementAtIndex(4)
            Assert.assertEquals(3, nestedV!!.numArrayElements().toLong())
            checkType(nestedV.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, nestedV.getElementAtIndex(0)!!.int.toLong())
            checkType(nestedV.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.NULL)
            checkType(nestedV.getElementAtIndex(2), VariantUtil.SHORT_STR, Variant.Type.STRING)
            Assert.assertEquals("c", nestedV.getElementAtIndex(2)!!.string)
        }
    }

    fun testArrayOffsetSize(randomString: String) {
        val value = Variant(
            ByteBuffer.wrap(constructArray(constructString(randomString), VALUE_BOOL, VALUE_INT)), EMPTY_METADATA
        )

        testVariant(value) { v ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(3, v.numArrayElements().toLong())
            checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.STRING)
            Assert.assertEquals(randomString, v.getElementAtIndex(0)!!.string)
            checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getElementAtIndex(1)!!.boolean)
            checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.getElementAtIndex(2)!!.int.toLong())
        }
    }

    @Test
    fun testArrayTwoByteOffset() {
        // a string larger than 255 bytes to push the value offset size above 1 byte
        testArrayOffsetSize(randomString(300))
    }

    @Test
    fun testArrayThreeByteOffset() {
        // a string larger than 65535 bytes to push the value offset size above 2 bytes
        testArrayOffsetSize(randomString(70000))
    }

    @Test
    fun testArrayFourByteOffset() {
        // a string larger than 16777215 bytes to push the value offset size above 3 bytes
        testArrayOffsetSize(randomString(16800000))
    }

    @Test
    fun testInvalidArray() {
        try {
            // An object header
            val value = Variant(ByteBuffer.wrap(byteArrayOf(66)), EMPTY_METADATA)
            value.numArrayElements()
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read OBJECT value as ARRAY", e.message)
        }
    }

    companion object {
        private const val RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

        /** Random number generator for generating random strings  */
        private val random = SecureRandom(byteArrayOf(1, 2, 3, 4, 5))

        private val EMPTY_METADATA: ByteBuffer = ByteBuffer.wrap(byteArrayOf(1))

        private val VALUE_NULL = byteArrayOf(primitiveHeader(0))
        private val VALUE_BOOL = byteArrayOf(primitiveHeader(1))
        private val VALUE_INT = byteArrayOf(primitiveHeader(5), 0xD2.toByte(), 0x02, 0x96.toByte(), 0x49)
        private val VALUE_STRING = byteArrayOf(
            primitiveHeader(16),
            0x07,
            0x00,
            0x00,
            0x00,
            'v'.code.toByte(),
            'a'.code.toByte(),
            'r'.code.toByte(),
            'i'.code.toByte(),
            'a'.code.toByte(),
            'n'.code.toByte(),
            't'.code.toByte(),
        )
        private val VALUE_SHORT_STRING = byteArrayOf(5, 'c'.code.toByte())
        private val VALUE_DATE = byteArrayOf(44, 0xE3.toByte(), 0x4E, 0x00, 0x00)

        private fun primitiveHeader(type: Int): Byte {
            return (type shl 2).toByte()
        }

        private fun getMinIntegerSize(value: Int): Int {
            return if (value <= 0xFF) 1 else if (value <= 0xFFFF) 2 else if (value <= 0xFFFFFF) 3 else 4
        }

        private fun writeVarlenInt(buffer: ByteBuffer, value: Int, valueSize: Int) {
            when (valueSize) {
                1 -> buffer.put(value.toByte())
                2 -> buffer.putShort(value.toShort())
                3 -> {
                    buffer.put((value and 0xFF).toByte())
                    buffer.put(((value shr 8) and 0xFF).toByte())
                    buffer.put(((value shr 16) and 0xFF).toByte())
                }
                else -> buffer.putInt(value)
            }
        }

        private fun constructString(value: String): ByteArray {
            return ByteBuffer.allocate(value.length + 5)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(primitiveHeader(16))
                .putInt(value.length)
                .put(value.toByteArray(StandardCharsets.UTF_8))
                .array()
        }

        private fun constructArray(vararg elements: ByteArray): ByteArray {
            var dataSize = 0
            for (element in elements) {
                dataSize += element.size
            }

            val isLarge = elements.size > 0xFF
            val offsetSize: Int = getMinIntegerSize(dataSize)
            val headerSize = 1 + (if (isLarge) 4 else 1) + (elements.size + 1) * offsetSize

            val output = ByteBuffer.allocate(headerSize + dataSize).order(ByteOrder.LITTLE_ENDIAN)

            output.put(arrayHeader(isLarge, offsetSize))

            if (isLarge) {
                output.putInt(elements.size)
            } else {
                output.put(elements.size.toByte())
            }

            var currOffset = 0
            for (i in elements.indices) {
                writeVarlenInt(output, currOffset, offsetSize)
                currOffset += elements[i].size
            }
            writeVarlenInt(output, currOffset, offsetSize)

            for (i in elements.indices) {
                output.put(elements[i])
            }
            output.flip()
            return output.array()
        }
    }
}
