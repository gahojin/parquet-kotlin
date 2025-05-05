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

import org.apache.parquet.variant.VariantTestUtil.checkType
import org.apache.parquet.variant.VariantTestUtil.randomString
import org.apache.parquet.variant.VariantTestUtil.testVariant
import org.junit.Assert
import org.junit.Test

class TestVariantArrayBuilder {
    @Test
    fun testEmptyArrayBuilder() {
        val b = VariantBuilder()
        val a = b.startArray()
        b.endArray()
        testVariant(b.build()) { v: Variant? ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(0, v!!.numArrayElements().toLong())
        }
    }

    @Test
    fun testLargeArraySizeBuilder() {
        val b: VariantBuilder = VariantBuilder()
        val a: VariantArrayBuilder = b.startArray()
        for (i in 0..510) {
            a.appendInt(i)
        }
        b.endArray()
        testVariant(b.build()) { v: Variant? ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(511, v!!.numArrayElements().toLong())
            for (i in 0..510) {
                checkType(v.getElementAtIndex(i), VariantUtil.PRIMITIVE, Variant.Type.INT)
                Assert.assertEquals(i.toLong(), v.getElementAtIndex(i)!!.int.toLong())
            }
        }
    }

    @Test
    fun testMixedArrayBuilder() {
        val b: VariantBuilder = VariantBuilder()
        val arrBuilder: VariantArrayBuilder = b.startArray()
        arrBuilder.appendBoolean(true)
        val obj: VariantObjectBuilder = arrBuilder.startObject()
        obj.appendKey("key")
        obj.appendInt(321)
        arrBuilder.endObject()
        arrBuilder.appendLong(1234567890)
        run {
            // build a nested array
            val nestedBuilder: VariantArrayBuilder = arrBuilder.startArray()
            run {
                // build a nested empty array
                nestedBuilder.startArray()
                nestedBuilder.endArray()
            }
            nestedBuilder.appendString("variant")
            nestedBuilder.startObject()
            nestedBuilder.endObject()
            arrBuilder.endArray()
        }
        b.endArray()

        testVariant(b.build()) { v: Variant? ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(4, v!!.numArrayElements().toLong())
            checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getElementAtIndex(0)!!.boolean)

            checkType(v.getElementAtIndex(1), VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(1, v.getElementAtIndex(1)!!.numObjectElements().toLong())
            checkType(
                v.getElementAtIndex(1)!!.getFieldByKey("key"), VariantUtil.PRIMITIVE, Variant.Type.INT
            )
            Assert.assertEquals(321, v.getElementAtIndex(1)!!.getFieldByKey("key")!!.int.toLong())

            checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.LONG)
            Assert.assertEquals(1234567890, v.getElementAtIndex(2)!!.long)

            checkType(v.getElementAtIndex(3), VariantUtil.ARRAY, Variant.Type.ARRAY)
            val nested = v.getElementAtIndex(3)
            Assert.assertEquals(3, nested!!.numArrayElements().toLong())
            checkType(nested.getElementAtIndex(0), VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(0, nested.getElementAtIndex(0)!!.numArrayElements().toLong())
            checkType(nested.getElementAtIndex(1), VariantUtil.SHORT_STR, Variant.Type.STRING)
            Assert.assertEquals("variant", nested.getElementAtIndex(1)!!.string)
            checkType(nested.getElementAtIndex(2), VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(0, nested.getElementAtIndex(2)!!.numObjectElements().toLong())
        }
    }

    private fun buildNested(i: Int, obj: VariantArrayBuilder) {
        if (i > 0) {
            obj.appendString("str" + i)
            buildNested(i - 1, obj.startArray())
            obj.endArray()
        }
    }

    @Test
    fun testNestedBuilder() {
        val b: VariantBuilder = VariantBuilder()
        buildNested(1000, b.startArray())
        b.endArray()

        testVariant(b.build()) { v: Variant? ->
            var curr = v
            for (i in 1000 downTo 0) {
                checkType(curr, VariantUtil.ARRAY, Variant.Type.ARRAY)
                if (i == 0) {
                    Assert.assertEquals(0, curr!!.numArrayElements().toLong())
                } else {
                    Assert.assertEquals(2, curr!!.numArrayElements().toLong())
                    checkType(curr.getElementAtIndex(0), VariantUtil.SHORT_STR, Variant.Type.STRING)
                    Assert.assertEquals("str$i", curr.getElementAtIndex(0)!!.string)
                    curr = curr.getElementAtIndex(1)
                }
            }
        }
    }

    private fun testArrayOffsetSizeBuilder(randomString: String) {
        val b: VariantBuilder = VariantBuilder()
        val arrBuilder: VariantArrayBuilder = b.startArray()
        arrBuilder.appendString(randomString)
        arrBuilder.appendBoolean(true)
        arrBuilder.appendLong(1234567890)
        b.endArray()

        testVariant(b.build()) { v: Variant? ->
            checkType(v, VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(3, v!!.numArrayElements().toLong())
            checkType(v.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.STRING)
            Assert.assertEquals(randomString, v.getElementAtIndex(0)!!.string)
            checkType(v.getElementAtIndex(1), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getElementAtIndex(1)!!.boolean)
            checkType(v.getElementAtIndex(2), VariantUtil.PRIMITIVE, Variant.Type.LONG)
            Assert.assertEquals(1234567890, v.getElementAtIndex(2)!!.long)
        }
    }

    @Test
    fun testArrayTwoByteOffsetBuilder() {
        // a string larger than 255 bytes to push the value offset size above 1 byte
        testArrayOffsetSizeBuilder(randomString(300))
    }

    @Test
    fun testArrayThreeByteOffsetBuilder() {
        // a string larger than 65535 bytes to push the value offset size above 2 bytes
        testArrayOffsetSizeBuilder(randomString(70000))
    }

    @Test
    fun testArrayFourByteOffsetBuilder() {
        // a string larger than 16777215 bytes to push the value offset size above 3 bytes
        testArrayOffsetSizeBuilder(randomString(16800000))
    }

    @Test
    fun testMissingEndArray() {
        val b = VariantBuilder()
        b.startArray()
        try {
            b.build()
            Assert.fail("Expected Exception when calling build() without endArray()")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testMissingStartArray() {
        val b = VariantBuilder()
        try {
            b.endArray()
            Assert.fail("Expected Exception when calling endArray() without startArray()")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testInvalidAppendDuringArray() {
        val b = VariantBuilder()
        b.startArray()
        try {
            b.appendInt(1)
            Assert.fail("Expected Exception when calling append() before endArray()")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testStartArrayEndObject() {
        val b = VariantBuilder()
        val obj = b.startArray()
        try {
            obj.endObject()
            Assert.fail("Expected Exception when calling endObject() while building array")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedObject() {
        val b = VariantBuilder()
        val arr = b.startArray()
        arr.startObject()
        try {
            b.endArray()
            Assert.fail("Expected Exception when calling endArray() with an open nested object")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedObjectWithKey() {
        val b = VariantBuilder()
        val arr = b.startArray()
        val nested = arr.startObject()
        nested.appendKey("nested")
        try {
            b.endArray()
            Assert.fail("Expected Exception when calling endArray() with an open nested object")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedObjectWithKeyValue() {
        val b = VariantBuilder()
        val arr = b.startArray()
        val nested = arr.startObject()
        nested.appendKey("nested")
        nested.appendInt(1)
        try {
            b.endArray()
            Assert.fail("Expected Exception when calling endArray() with an open nested object")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedArray() {
        val b = VariantBuilder()
        val arr = b.startArray()
        arr.startArray()
        try {
            b.endArray()
            Assert.fail("Expected Exception when calling endArray() with an open nested array")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedArrayWithElement() {
        val b = VariantBuilder()
        val arr: VariantArrayBuilder = b.startArray()
        val nested: VariantArrayBuilder = arr.startArray()
        nested.appendInt(1)
        try {
            b.endArray()
            Assert.fail("Expected Exception when calling endArray() with an open nested array")
        } catch (_: Exception) {
            // expected
        }
    }
}
