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
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.time.LocalDate
import java.time.LocalTime
import java.util.UUID

class TestVariantScalarBuilder {
    @Test
    fun testNullBuilder() {
        val vb = VariantBuilder()
        vb.appendNull()
        testVariant(vb.build()) { v: Variant? -> checkType(v, VariantUtil.NULL, Variant.Type.NULL) }

        try {
            vb.appendNull()
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testBooleanBuilder() {
        listOf(true, false).forEach { b ->
            val vb = VariantBuilder()
            vb.appendBoolean(b)
            testVariant(vb.build()) { v: Variant? ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
                Assert.assertEquals(b, v!!.boolean)
            }
            try {
                vb.appendBoolean(true)
                Assert.fail("Expected Exception when appending multiple values")
            } catch (_: Exception) {
                // expected
            }
        }
    }

    @Test
    fun testLongBuilder() {
        arrayOf(
            0L,
            Byte.Companion.MIN_VALUE.toLong(),
            Byte.Companion.MAX_VALUE.toLong(),
            Short.Companion.MIN_VALUE.toLong(),
            Short.Companion.MAX_VALUE.toLong(),
            Int.Companion.MIN_VALUE.toLong(),
            Int.Companion.MAX_VALUE.toLong(),
            Long.Companion.MIN_VALUE,
            Long.Companion.MAX_VALUE,
        ).forEach { l ->
            val vb = VariantBuilder()
            vb.appendLong(l)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG)
                Assert.assertEquals(l, v.long)
            }
            try {
                vb.appendLong(1L)
                Assert.fail("Expected Exception when appending multiple values")
            } catch (_: Exception) {
                // expected
            }
        }
    }

    @Test
    fun testIntBuilder() {
        arrayOf(
            0,
            Byte.Companion.MIN_VALUE.toInt(),
            Byte.Companion.MAX_VALUE.toInt(),
            Short.Companion.MIN_VALUE.toInt(),
            Short.Companion.MAX_VALUE.toInt(),
            Int.Companion.MIN_VALUE,
            Int.Companion.MAX_VALUE,
        ).forEach { i ->
            val vb: VariantBuilder = VariantBuilder()
            vb.appendInt(i)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT)
                Assert.assertEquals(i, v.int)
            }
            try {
                vb.appendInt(1)
                Assert.fail("Expected Exception when appending multiple values")
            } catch (_: Exception) {
                // expected
            }
        }
    }

    @Test
    fun testShortBuilder() {
        arrayOf(
            0.toShort(),
            Byte.Companion.MIN_VALUE.toShort(),
            Byte.Companion.MAX_VALUE.toShort(),
            Short.Companion.MIN_VALUE,
            Short.Companion.MAX_VALUE,
        ).forEach { s ->
            val vb = VariantBuilder()
            vb.appendShort(s)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT)
                Assert.assertEquals(s, v.short)
            }
            try {
                vb.appendShort(1.toShort())
                Assert.fail("Expected Exception when appending multiple values")
            } catch (_: Exception) {
                // expected
            }
        }
    }

    @Test
    fun testByteBuilder() {
        arrayOf(0.toByte(), Byte.Companion.MIN_VALUE, Byte.Companion.MAX_VALUE).forEach { b ->
            val vb = VariantBuilder()
            vb.appendByte(b)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE)
                Assert.assertEquals(b, v.byte)
            }
            try {
                vb.appendByte(1.toByte())
                Assert.fail("Expected Exception when appending multiple values")
            } catch (_: Exception) {
                // expected
            }
        }
    }

    @Test
    fun testFloatBuilder() {
        arrayOf(Float.Companion.MIN_VALUE, 0f, -0f, Float.Companion.MAX_VALUE).forEach { f ->
            val vb = VariantBuilder()
            vb.appendFloat(f)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT)
                Assert.assertEquals(f, v.float, 0f)
            }
            try {
                vb.appendFloat(1.2f)
                Assert.fail("Expected Exception when appending multiple values")
            } catch (_: Exception) {
                // expected
            }
        }
    }

    @Test
    fun testDoubleBuilder() {
        arrayOf(Double.Companion.MIN_VALUE, 0.0, -0.0, Double.Companion.MAX_VALUE).forEach { d ->
            val vb = VariantBuilder()
            vb.appendDouble(d)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE)
                Assert.assertEquals(d, v.double, 0.0)
            }
            try {
                vb.appendDouble(1.2)
                Assert.fail("Expected Exception when appending multiple values")
            } catch (_: Exception) {
                // expected
            }
        }
    }

    @Test
    fun testDecimalBuilder() {
        // decimal4
        arrayOf(BigDecimal("123.456"), BigDecimal("-987.654")).forEach { d ->
            val vb = VariantBuilder()
            vb.appendDecimal(d)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4)
                Assert.assertEquals(d, v.decimal)
            }
        }

        // decimal8
        arrayOf(BigDecimal("10.2147483647"), BigDecimal("-1021474836.47")).forEach { d ->
            val vb = VariantBuilder()
            vb.appendDecimal(d)
            testVariant(vb.build()) { v ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8)
                Assert.assertEquals(d, v.decimal)
            }
        }

        // decimal16
        arrayOf(BigDecimal("109223372036854775.807"), BigDecimal("-109.223372036854775807"))
            .forEach { d ->
                val vb = VariantBuilder()
                vb.appendDecimal(d)
                testVariant(vb.build()) { v: Variant? ->
                    checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16)
                    Assert.assertEquals(d, v!!.decimal)
                }
            }

        val vb = VariantBuilder()
        vb.appendDecimal(BigDecimal("10.2147483647"))
        try {
            vb.appendDecimal(BigDecimal("10.2147483647"))
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testDateBuilder() {
        val vb = VariantBuilder()
        val days = Math.toIntExact(LocalDate.of(2024, 12, 16).toEpochDay())
        vb.appendDate(days)
        testVariant(vb.build()) { v: Variant? ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE)
            Assert.assertEquals(days.toLong(), v!!.int.toLong())
        }

        try {
            vb.appendDate(123)
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testTimestampTzBuilder() {
        val vb = VariantBuilder()
        vb.appendTimestampTz(1734373425321456L)
        testVariant(vb.build()) { v: Variant? ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ)
            Assert.assertEquals(1734373425321456L, v!!.long)
        }

        try {
            vb.appendTimestampTz(1734373425321456L)
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testTimestampNtzBuilder() {
        val vb = VariantBuilder()
        vb.appendTimestampNtz(1734373425321456L)
        testVariant(vb.build()) { v: Variant? ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ)
            Assert.assertEquals(1734373425321456L, v!!.long)
        }

        try {
            vb.appendTimestampNtz(1734373425321456L)
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testBinaryBuilder() {
        val vb = VariantBuilder()
        vb.appendBinary(ByteBuffer.wrap(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
        testVariant(vb.build()) { v: Variant? ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BINARY)
            Assert.assertEquals(ByteBuffer.wrap(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)), v!!.binary)
        }

        try {
            vb.appendBinary(ByteBuffer.wrap(byteArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testStringBuilder() {
        arrayOf(VariantUtil.MAX_SHORT_STR_SIZE - 3, VariantUtil.MAX_SHORT_STR_SIZE + 3)
            .forEach { len ->
                val vb = VariantBuilder()
                val s = randomString(len)
                vb.appendString(s)
                testVariant(vb.build()) { v: Variant? ->
                    if (len <= VariantUtil.MAX_SHORT_STR_SIZE) {
                        checkType(v, VariantUtil.SHORT_STR, Variant.Type.STRING)
                    } else {
                        checkType(v, VariantUtil.PRIMITIVE, Variant.Type.STRING)
                    }
                    Assert.assertEquals(s, v!!.string)
                }
            }

        val vb: VariantBuilder = VariantBuilder()
        vb.appendString(randomString(10))
        try {
            vb.appendString(randomString(10))
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testTimeBuilder() {
        arrayOf("00:00:00.000000", "00:00:00.000120", "12:00:00.000000", "12:00:00.002300", "23:59:59.999999").forEach { timeStr ->
            val vb = VariantBuilder()
            val micros = LocalTime.parse(timeStr).toNanoOfDay() / 1000
            vb.appendTime(micros)
            testVariant(vb.build()) { v: Variant? ->
                checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIME)
                Assert.assertEquals(micros, v!!.long)
            }
        }

        // test negative time
        try {
            val vb = VariantBuilder()
            vb.appendTime(-1)
            Assert.fail("Expected Exception when adding a negative time value")
        } catch (_: IllegalArgumentException) {
            // expected
        }

        val vb: VariantBuilder = VariantBuilder()
        vb.appendTime(123456)
        try {
            vb.appendTime(123456)
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testTimestampNanosBuilder() {
        val vb = VariantBuilder()
        vb.appendTimestampNanosTz(1734373425321456987L)
        testVariant(vb.build()) { v: Variant? ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ)
            Assert.assertEquals(1734373425321456987L, v!!.long)
        }

        try {
            vb.appendTimestampNanosTz(1734373425321456987L)
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testTimestampNanosNtzBuilder() {
        val vb = VariantBuilder()
        vb.appendTimestampNanosNtz(1734373425321456987L)
        testVariant(vb.build()) { v: Variant? ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ)
            Assert.assertEquals(1734373425321456987L, v!!.long)
        }

        try {
            vb.appendTimestampNanosNtz(1734373425321456987L)
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }

    @Test
    fun testUUIDBuilder() {
        val vb = VariantBuilder()
        val uuid = byteArrayOf(0, 17, 34, 51, 68, 85, 102, 119, -120, -103, -86, -69, -52, -35, -18, -1)
        val msb = ByteBuffer.wrap(uuid, 0, 8).order(ByteOrder.BIG_ENDIAN).getLong()
        val lsb = ByteBuffer.wrap(uuid, 8, 8).order(ByteOrder.BIG_ENDIAN).getLong()
        val expected = UUID(msb, lsb)

        vb.appendUUID(expected)
        testVariant(vb.build()) { v: Variant? ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.UUID)
            Assert.assertEquals(expected, v!!.uUID)
        }

        try {
            vb.appendUUID(expected)
            Assert.fail("Expected Exception when appending multiple values")
        } catch (_: Exception) {
            // expected
        }
    }
}
