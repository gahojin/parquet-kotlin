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

import org.apache.parquet.variant.VariantTestUtil.EMPTY_METADATA
import org.apache.parquet.variant.VariantTestUtil.checkType
import org.apache.parquet.variant.VariantTestUtil.constructString
import org.apache.parquet.variant.VariantTestUtil.getMinIntegerSize
import org.apache.parquet.variant.VariantTestUtil.randomString
import org.apache.parquet.variant.VariantTestUtil.testVariant
import org.apache.parquet.variant.VariantTestUtil.writeVarlenInt
import org.apache.parquet.variant.VariantUtil.arrayHeader
import org.junit.Assert
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.LocalDate

class TestVariantArray {
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
        private val VALUE_NULL = byteArrayOf(VariantTestUtil.primitiveHeader(0))
        private val VALUE_BOOL = byteArrayOf(VariantTestUtil.primitiveHeader(1))
        private val VALUE_INT = byteArrayOf(VariantTestUtil.primitiveHeader(5), 0xD2.toByte(), 0x02, 0x96.toByte(), 0x49)
        private val VALUE_STRING = byteArrayOf(
            VariantTestUtil.primitiveHeader(16),
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
