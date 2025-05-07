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
import org.junit.Ignore
import org.junit.Test

class TestVariantObjectBuilder {
    @Test
    fun testEmptyObjectBuilder() {
        val b = VariantBuilder()
        b.startObject()
        b.endObject()
        testVariant(b.build()) { v: Variant? ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(0, v!!.numObjectElements().toLong())
        }
    }

    @Test
    fun testLargeObjectBuilder() {
        val b = VariantBuilder()
        val o = b.startObject()
        for (i in 0..1233) {
            o.appendKey("a$i")
            o.appendLong(i.toLong())
        }
        b.endObject()
        testVariant(b.build()) { v: Variant? ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(1234, v!!.numObjectElements().toLong())
            for (i in 0..1233) {
                checkType(v.getFieldByKey("a$i"), VariantUtil.PRIMITIVE, Variant.Type.LONG)
                Assert.assertEquals(i.toLong(), v.getFieldByKey("a$i")!!.long)
            }
        }
    }

    @Test
    fun testMixedObjectBuilder() {
        val b = VariantBuilder()
        val objBuilder = b.startObject()
        objBuilder.appendKey("outer 2")
        objBuilder.appendLong(1234567890)
        objBuilder.appendKey("outer 1")
        objBuilder.appendBoolean(true)
        objBuilder.appendKey("outer 4")
        objBuilder.startArray()
        objBuilder.endArray()
        objBuilder.appendKey("outer 3")
        run {
            // build a nested obj
            val nestedBuilder = objBuilder.startObject()
            nestedBuilder.appendKey("nested 3")
            val arr = nestedBuilder.startArray()
            arr.appendInt(321)
            nestedBuilder.endArray()
            nestedBuilder.appendKey("nested 1")
            run {
                // build a nested empty obj
                nestedBuilder.startObject()
                nestedBuilder.endObject()
            }
            nestedBuilder.appendKey("nested 2")
            nestedBuilder.appendString("variant")
            objBuilder.endObject()
        }
        b.endObject()

        testVariant(b.build()) { v: Variant? ->
            checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(4, v!!.numObjectElements().toLong())
            checkType(v.getFieldByKey("outer 1"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.getFieldByKey("outer 1")!!.boolean)
            checkType(v.getFieldByKey("outer 2"), VariantUtil.PRIMITIVE, Variant.Type.LONG)
            Assert.assertEquals(1234567890, v.getFieldByKey("outer 2")!!.long)
            checkType(v.getFieldByKey("outer 3"), VariantUtil.OBJECT, Variant.Type.OBJECT)

            val nested = v.getFieldByKey("outer 3")
            Assert.assertEquals(3, nested!!.numObjectElements().toLong())
            checkType(nested.getFieldByKey("nested 1"), VariantUtil.OBJECT, Variant.Type.OBJECT)
            Assert.assertEquals(0, nested.getFieldByKey("nested 1")!!.numObjectElements().toLong())
            checkType(nested.getFieldByKey("nested 2"), VariantUtil.SHORT_STR, Variant.Type.STRING)
            Assert.assertEquals("variant", nested.getFieldByKey("nested 2")!!.string)
            checkType(nested.getFieldByKey("nested 3"), VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(1, nested.getFieldByKey("nested 3")!!.numArrayElements().toLong())
            checkType(
                nested.getFieldByKey("nested 3")!!.getElementAtIndex(0), VariantUtil.PRIMITIVE, Variant.Type.INT
            )
            Assert.assertEquals(
                321, nested.getFieldByKey("nested 3")!!.getElementAtIndex(0)!!.int.toLong()
            )

            checkType(v.getFieldByKey("outer 4"), VariantUtil.ARRAY, Variant.Type.ARRAY)
            Assert.assertEquals(0, v.getFieldByKey("outer 4")!!.numArrayElements().toLong())
        }
    }

    private fun buildNested(i: Int, obj: VariantObjectBuilder) {
        if (i > 0) {
            obj.appendKey("key$i")
            obj.appendString("str$i")
            obj.appendKey("duplicate")
            buildNested(i - 1, obj.startObject())
            obj.endObject()
        }
    }

    @Test
    fun testNestedBuilder() {
        val b = VariantBuilder()
        buildNested(1000, b.startObject())
        b.endObject()

        testVariant(b.build()) { v: Variant? ->
            var curr = v
            for (i in 1000 downTo 0) {
                checkType(curr, VariantUtil.OBJECT, Variant.Type.OBJECT)
                if (i == 0) {
                    Assert.assertEquals(0, curr!!.numObjectElements().toLong())
                } else {
                    Assert.assertEquals(2, curr!!.numObjectElements().toLong())
                    checkType(
                        curr.getFieldByKey("key$i"), VariantUtil.SHORT_STR, Variant.Type.STRING
                    )
                    Assert.assertEquals("str$i", curr.getFieldByKey("key$i")!!.string)
                    curr = curr.getFieldByKey("duplicate")
                }
            }
        }
    }

    private fun testObjectOffsetSizeBuilder(randomString: String) {
        val b = VariantBuilder()
        val objBuilder = b.startObject()
        objBuilder.appendKey("key1")
        objBuilder.appendString(randomString)
        objBuilder.appendKey("key2")
        objBuilder.appendBoolean(true)
        objBuilder.appendKey("key3")
        objBuilder.appendLong(1234567890)
        b.endObject()

        val v: Variant = b.build()
        checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
        Assert.assertEquals(3, v.numObjectElements().toLong())
        checkType(v.getFieldByKey("key1"), VariantUtil.PRIMITIVE, Variant.Type.STRING)
        Assert.assertEquals(randomString, v.getFieldByKey("key1")!!.string)
        checkType(v.getFieldByKey("key2"), VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
        Assert.assertTrue(v.getFieldByKey("key2")!!.boolean)
        checkType(v.getFieldByKey("key3"), VariantUtil.PRIMITIVE, Variant.Type.LONG)
        Assert.assertEquals(1234567890, v.getFieldByKey("key3")!!.long)
    }

    @Test
    fun testObjectTwoByteOffsetBuilder() {
        // a string larger than 255 bytes to push the offset size above 1 byte
        testObjectOffsetSizeBuilder(randomString(300))
    }

    @Test
    fun testObjectThreeByteOffsetBuilder() {
        // a string larger than 65535 bytes to push the offset size above 2 bytes
        testObjectOffsetSizeBuilder(randomString(70000))
    }

    @Test
    fun testObjectFourByteOffsetBuilder() {
        // a string larger than 16777215 bytes to push the offset size above 3 bytes
        testObjectOffsetSizeBuilder(randomString(16800000))
    }

    private fun testObjectFieldIdSizeBuilder(numKeys: Int) {
        val b = VariantBuilder()
        val objBuilder = b.startObject()
        for (i in 0..<numKeys) {
            objBuilder.appendKey("k$i")
            objBuilder.appendLong(i.toLong())
        }
        b.endObject()

        val v: Variant = b.build()
        checkType(v, VariantUtil.OBJECT, Variant.Type.OBJECT)
        Assert.assertEquals(numKeys.toLong(), v.numObjectElements().toLong())
        // Only check a few keys, to avoid slowing down the test
        checkType(v.getFieldByKey("k" + 0), VariantUtil.PRIMITIVE, Variant.Type.LONG)
        Assert.assertEquals(0, v.getFieldByKey("k" + 0)!!.long)
        checkType(v.getFieldByKey("k" + (numKeys - 1)), VariantUtil.PRIMITIVE, Variant.Type.LONG)
        Assert.assertEquals((numKeys - 1).toLong(), v.getFieldByKey("k" + (numKeys - 1))!!.long)
    }

    @Test
    fun testObjectTwoByteFieldIdBuilder() {
        // need more than 255 dictionary entries to push field id size above 1 byte
        testObjectFieldIdSizeBuilder(300)
    }

    @Test
    fun testObjectThreeByteFieldIdBuilder() {
        // need more than 65535 dictionary entries to push field id size above 2 bytes
        testObjectFieldIdSizeBuilder(70000)
    }

    @Test
    @Ignore("Test uses too much memory")
    fun testObjectFourByteFieldIdBuilder() {
        // need more than 16777215 dictionary entries to push field id size above 3 bytes
        testObjectFieldIdSizeBuilder(16800000)
    }

    @Test
    fun testDuplicateKeys() {
        val b = VariantBuilder()
        val objBuilder = b.startObject()
        objBuilder.appendKey("duplicate")
        objBuilder.appendLong(0)
        objBuilder.appendKey("duplicate")
        objBuilder.appendLong(1)
        b.endObject()
        val v: Variant = b.build()
        Assert.assertEquals(1, v.numObjectElements().toLong())
        checkType(v.getFieldByKey("duplicate"), VariantUtil.PRIMITIVE, Variant.Type.LONG)
        Assert.assertEquals(1, v.getFieldByKey("duplicate")!!.long)
    }

    @Test
    fun testSortingKeys() {
        val b = VariantBuilder()
        val objBuilder = b.startObject()
        objBuilder.appendKey("1")
        objBuilder.appendString("1")
        objBuilder.appendKey("0")
        objBuilder.appendString("")
        objBuilder.appendKey("3")
        objBuilder.appendString("333")
        objBuilder.appendKey("2")
        objBuilder.appendString("22")
        b.endObject()
        val v: Variant = b.build()
        Assert.assertEquals(4, v.numObjectElements().toLong())
        checkType(v.getFieldByKey("0"), VariantUtil.SHORT_STR, Variant.Type.STRING)
        Assert.assertEquals("", v.getFieldByKey("0")!!.string)
        checkType(v.getFieldByKey("1"), VariantUtil.SHORT_STR, Variant.Type.STRING)
        Assert.assertEquals("1", v.getFieldByKey("1")!!.string)
        checkType(v.getFieldByKey("2"), VariantUtil.SHORT_STR, Variant.Type.STRING)
        Assert.assertEquals("22", v.getFieldByKey("2")!!.string)
        checkType(v.getFieldByKey("3"), VariantUtil.SHORT_STR, Variant.Type.STRING)
        Assert.assertEquals("333", v.getFieldByKey("3")!!.string)
    }

    @Test
    fun testMissingEndObject() {
        val b = VariantBuilder()
        b.startObject()
        try {
            b.build()
            Assert.fail("Expected Exception when calling build() without endObject()")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testMissingStartObject() {
        val b = VariantBuilder()
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() without startObject()")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testMissingValue() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("a")
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() with mismatched keys and values")
        } catch (_: Exception) {
            // expected
        }

        obj.appendInt(1)
        obj.appendKey("b")
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() with mismatched keys and values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testInvalidAppendDuringObjectAppend() {
        val b = VariantBuilder()
        b.startObject()
        try {
            b.appendInt(1)
            Assert.fail("Expected Exception when calling append() before endObject()")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testMultipleAppendKey() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("a")
        try {
            obj.appendKey("a")
            Assert.fail("Expected Exception when calling appendKey() multiple times without appending a value")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testNoAppendKey() {
        val b = VariantBuilder()
        val obj = b.startObject()
        try {
            obj.appendInt(1)
            Assert.fail("Expected Exception when appending a value, before appending a key")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testMultipleAppendValue() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("a")
        obj.appendInt(1)
        try {
            obj.appendInt(1)
            Assert.fail("Expected Exception when appending a value, before appending a key")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testStartObjectEndArray() {
        val b = VariantBuilder()
        val obj = b.startObject()
        try {
            obj.endArray()
            Assert.fail("Expected Exception when calling endArray() while building object")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedObject() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("outer")
        obj.startObject()
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() with an open nested object")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedObjectWithKey() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("outer")
        val nested: VariantObjectBuilder = obj.startObject()
        nested.appendKey("nested")
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() with an open nested object")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedObjectWithKeyValue() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("outer")
        val nested: VariantObjectBuilder = obj.startObject()
        nested.appendKey("nested")
        nested.appendInt(1)
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() with an open nested object")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedArray() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("outer")
        obj.startArray()
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() with an open nested array")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testOpenNestedArrayWithElement() {
        val b = VariantBuilder()
        val obj = b.startObject()
        obj.appendKey("outer")
        val nested: VariantArrayBuilder = obj.startArray()
        nested.appendInt(1)
        try {
            b.endObject()
            Assert.fail("Expected Exception when calling endObject() with an open nested array")
        } catch (_: Exception) {
            // expected
        }
    }
}
