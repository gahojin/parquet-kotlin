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
import org.apache.parquet.variant.VariantTestUtil.metadataHeader
import org.apache.parquet.variant.VariantTestUtil.primitiveHeader
import org.apache.parquet.variant.VariantTestUtil.randomString
import org.apache.parquet.variant.VariantTestUtil.testVariant
import org.apache.parquet.variant.VariantTestUtil.writeVarlenInt
import org.apache.parquet.variant.VariantUtil.objectHeader
import org.junit.Assert
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.util.Arrays

class TestVariantObject {
    @Test
    fun testEmptyObject() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(2, 0x00)), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(0, v.numObjectElements().toLong())
        }
    }

    @Test
    fun testEmptyLargeObject() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(66, 0x00, 0x00, 0x00, 0x00)), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(0, v.numObjectElements().toLong())
        }
    }

    @Test
    fun testUnsortedMetadataObject() {
        val keys = mapOf("a" to 2, "b" to 1, "c" to 0)
        val fields = mapOf("a" to VALUE_INT, "b" to VALUE_BOOL, "c" to VALUE_STRING)

        val value = Variant(
            ByteBuffer.wrap(constructObject(keys, fields, true)),
            constructMetadata(false, listOf("c", "b", "a")),
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(3, v.numObjectElements().toLong())
            checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.getFieldByKey("a")!!.int.toLong())
            checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getFieldByKey("b")!!.boolean)
            checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.STRING)
            Assert.assertEquals("variant", v.getFieldByKey("c")!!.string)
        }
    }

    @Test
    fun testMixedObject() {
        val keys = mapOf("a" to 0, "b" to 1, "c" to 2)
        val nested: ByteArray = constructObject(keys, mapOf("a" to VALUE_DATE, "c" to VALUE_NULL), false)
        val fields = mapOf("a" to VALUE_INT, "b" to VALUE_BOOL, "c" to nested)

        val value = Variant(
            ByteBuffer.wrap(constructObject(keys, fields, true)),
            constructMetadata(true, listOf("a", "b", "c")),
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(3, v.numObjectElements().toLong())
            checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.getFieldByKey("a")!!.int.toLong())
            checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getFieldByKey("b")!!.boolean)
            checkType(v.getFieldByKey("c"), VariantUtil.OBJECT, Variant.Type.OBJECT)

            val nestedV = v.getFieldByKey("c")
            Assert.assertEquals(2, nestedV!!.numObjectElements().toLong())
            checkType(nestedV.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.DATE)
            Assert.assertEquals(
                LocalDate.parse("2025-04-17"),
                LocalDate.ofEpochDay(nestedV.getFieldByKey("a")!!.int.toLong()),
            )
            checkType(nestedV.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.NULL)
        }
    }

    @Test
    fun testUnsortedDataObject() {
        val keys = mapOf("a" to 0, "b" to 1, "c" to 2)
        val fields = mapOf("a" to VALUE_INT, "b" to VALUE_BOOL, "c" to VALUE_STRING)

        val value = Variant(
            ByteBuffer.wrap(constructObject(keys, fields, false)),
            Companion.constructMetadata(true, listOf("a", "b", "c"))
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(3, v.numObjectElements().toLong())
            checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.getFieldByKey("a")!!.int.toLong())
            checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getFieldByKey("b")!!.boolean)
            checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.STRING)
            Assert.assertEquals("variant", v.getFieldByKey("c")!!.string)
        }
    }

    private fun testObjectOffsetSize(randomString: String) {
        val value = Variant(
            ByteBuffer.wrap(
                constructObject(
                    mapOf("a" to 0, "b" to 1, "c" to 2),
                    mapOf("a" to constructString(randomString), "b" to VALUE_BOOL, "c" to VALUE_INT),
                    true,
                )
            ),
            constructMetadata(true, listOf("a", "b", "c"))
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(3, v.numObjectElements().toLong())
            checkType(v.getFieldByKey("a"), VariantUtil.PRIMITIVE, Variant.Type.STRING)
            Assert.assertEquals(randomString, v.getFieldByKey("a")!!.string)
            checkType(v.getFieldByKey("b"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getFieldByKey("b")!!.boolean)
            checkType(v.getFieldByKey("c"), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.getFieldByKey("c")!!.int.toLong())
        }
    }

    @Test
    fun testObjectTwoByteOffset() {
        // a string larger than 255 bytes to push the offset size above 1 byte
        testObjectOffsetSize(randomString(300))
    }

    @Test
    fun testObjectThreeByteOffset() {
        // a string larger than 65535 bytes to push the offset size above 2 bytes
        testObjectOffsetSize(randomString(70000))
    }

    @Test
    fun testObjectFourByteOffset() {
        // a string larger than 16777215 bytes to push the offset size above 3 bytes
        testObjectOffsetSize(randomString(16800000))
    }

    private fun testObjectFieldIdSize(numExtraKeys: Int) {
        val fieldNames = (0..<numExtraKeys).map { "a$it" }.toMutableList()
        fieldNames.add("z1")
        fieldNames.add("z2")

        val value = Variant(
            ByteBuffer.wrap(
                constructObject(
                    mapOf("z1" to numExtraKeys, "z2" to numExtraKeys + 1),
                    mapOf("z1" to VALUE_BOOL, "z2" to VALUE_INT),
                    true,
                )
            ),
            constructMetadata(true, fieldNames)
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(2, v.numObjectElements().toLong())
            checkType(v.getFieldByKey("z1"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getFieldByKey("z1")!!.boolean)
            checkType(v.getFieldByKey("z2"), VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.getFieldByKey("z2")!!.int.toLong())
        }
    }

    @Test
    fun testObjectTwoByteFieldId() {
        // need more than 255 dictionary entries to push field id size above 1 byte
        testObjectFieldIdSize(300)
    }

    @Test
    fun testObjectThreeByteFieldId() {
        // need more than 65535 dictionary entries to push field id size above 2 bytes
        testObjectFieldIdSize(70000)
    }

    @Test
    fun testObjectFourByteFieldId() {
        // need more than 16777215 dictionary entries to push field id size above 3 bytes
        testObjectFieldIdSize(16800000)
    }

    @Test
    fun testLargeObject() {
        val keys = HashMap<String, Int>()
        val fields = HashMap<String, ByteArray>()
        for (i in 0..999) {
            val name = "a%04d".format(i)
            keys.put(name, i)
            fields.put(name, constructString(randomString(5)))
        }

        val sortedKeys = keys.keys.toMutableList()
        sortedKeys.sort()

        val value =
            Variant(ByteBuffer.wrap(constructObject(keys, fields, false)), constructMetadata(true, sortedKeys))
        testVariant(value) { v ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(1000, v.numObjectElements().toLong())
            for (i in 0..999) {
                val name = "a%04d".format(i)
                checkType(v.getFieldByKey(name), VariantUtil.PRIMITIVE, Variant.Type.STRING)
                Assert.assertEquals(
                    String(fields.get(name)!!, 5, fields.get(name)!!.size - 5),
                    v.getFieldByKey(name)!!.string
                )
            }
        }
    }

    @Test
    fun testInvalidObject() {
        try {
            // An array header
            val value = Variant(ByteBuffer.wrap(byteArrayOf(19)), EMPTY_METADATA)
            value.numObjectElements()
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read ARRAY value as OBJECT", e.message)
        }
    }

    companion object {
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
        private val VALUE_DATE = byteArrayOf(44, 0xE3.toByte(), 0x4E, 0x00, 0x00)

        private fun constructObject(
            keys: Map<String, Int>,
            fields: Map<String, ByteArray>,
            orderedData: Boolean,
        ): ByteArray {
            var dataSize = 0
            var maxId = 0
            for (entry in fields.entries) {
                dataSize += entry.value.size
                maxId = maxOf(maxId, keys[entry.key] ?: 0)
            }

            val isLarge = fields.size > 0xFF
            val fieldIdSize: Int = getMinIntegerSize(maxId)
            val offsetSize: Int = getMinIntegerSize(dataSize)
            // The space for header byte, object size, id list, and offset list.
            val headerSize = 1 + (if (isLarge) 4 else 1) + fields.size * fieldIdSize + (fields.size + 1) * offsetSize

            val output = ByteBuffer.allocate(headerSize + dataSize).order(ByteOrder.LITTLE_ENDIAN)

            output.put(objectHeader(isLarge, fieldIdSize, offsetSize))

            if (isLarge) {
                output.putInt(fields.size)
            } else {
                output.put(fields.size.toByte())
            }

            val sortedFieldNames = fields.keys.toTypedArray<String?>()
            Arrays.sort(sortedFieldNames)

            // write field ids
            for (fieldName in sortedFieldNames) {
                val fieldId: Int = keys[fieldName] ?: 0
                writeVarlenInt(output, fieldId, fieldIdSize)
            }

            // write offsets
            var currOffset = 0
            for (fieldName in sortedFieldNames) {
                val offsetToWrite =
                    if (orderedData) currOffset else dataSize - currOffset - fields[fieldName]!!.size
                writeVarlenInt(output, offsetToWrite, offsetSize)
                currOffset += fields[fieldName]?.size ?: 0
            }
            writeVarlenInt(output, if (orderedData) currOffset else 0, offsetSize)

            // write data
            for (i in sortedFieldNames.indices) {
                output.put(fields[sortedFieldNames[if (orderedData) i else sortedFieldNames.size - i - 1]])
            }

            output.flip()
            return output.array()
        }

        private fun constructMetadata(isSorted: Boolean, fieldNames: List<String>): ByteBuffer {
            if (fieldNames.isEmpty()) {
                return EMPTY_METADATA
            }

            var dataSize = 0
            for (fieldName in fieldNames) {
                dataSize += fieldName.length
            }

            val offsetSize: Int = getMinIntegerSize(dataSize)
            val offsetListStart = 1 + offsetSize
            val stringStart = offsetListStart + (fieldNames.size + 1) * offsetSize
            val metadataSize = stringStart + dataSize

            val output = ByteBuffer.allocate(metadataSize).order(ByteOrder.LITTLE_ENDIAN)

            output.put(metadataHeader(isSorted, offsetSize))
            writeVarlenInt(output, fieldNames.size, offsetSize)

            // write offsets
            var currentOffset = 0
            for (fieldName in fieldNames) {
                writeVarlenInt(output, currentOffset, offsetSize)
                currentOffset += fieldName.length
            }
            writeVarlenInt(output, currentOffset, offsetSize)

            // write strings
            for (fieldName in fieldNames) {
                output.put(fieldName.toByteArray(StandardCharsets.UTF_8))
            }

            output.flip()
            return output
        }
    }
}
