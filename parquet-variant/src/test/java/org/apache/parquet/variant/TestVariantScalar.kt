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
import org.apache.parquet.variant.VariantTestUtil.primitiveHeader
import org.apache.parquet.variant.VariantTestUtil.testVariant
import org.junit.Assert
import org.junit.Test
import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.temporal.ChronoUnit
import java.util.UUID

class TestVariantScalar {
    @Test
    fun testNull() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(0))), EMPTY_METADATA)
        testVariant(value) { v -> checkType(v, VariantUtil.NULL, Variant.Type.NULL) }
    }

    @Test
    fun testTrue() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(1))), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertTrue(v.boolean)
        }
    }

    @Test
    fun testFalse() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(2))), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BOOLEAN)
            Assert.assertFalse(v.boolean)
        }
    }

    @Test
    fun testLong() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(6), 0xB1.toByte(), 0x1C, 0x6C, 0xB1.toByte(), 0xF4.toByte(), 0x10, 0x22, 0x11,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG)
            Assert.assertEquals(1234567890987654321L, v.long)
        }
    }

    @Test
    fun testNegativeLong() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(6),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.LONG)
            Assert.assertEquals(-1L, v.long)
        }
    }

    @Test
    fun testInt() {
        val value = Variant(
            ByteBuffer.wrap(byteArrayOf(primitiveHeader(5), 0xD2.toByte(), 0x02, 0x96.toByte(), 0x49)), EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(1234567890, v.int.toLong())
        }
    }

    @Test
    fun testNegativeInt() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(5),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.INT)
            Assert.assertEquals(-1, v.int.toLong())
        }
    }

    @Test
    fun testShort() {
        val value =
            Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(4), 0xD2.toByte(), 0x04)), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT)
            Assert.assertEquals(1234.toShort().toLong(), v.short.toLong())
        }
    }

    @Test
    fun testNegativeShort() {
        val value =
            Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(4), 0xFF.toByte(), 0xFF.toByte())), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.SHORT)
            Assert.assertEquals(-1.toShort().toLong(), v.short.toLong())
        }
    }

    @Test
    fun testByte() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(3), 34)), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE)
            Assert.assertEquals(34.toByte().toLong(), v.byte.toLong())
        }
    }

    @Test
    fun testNegativeByte() {
        val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(3), 0xFF.toByte())), EMPTY_METADATA)
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BYTE)
            Assert.assertEquals(-1.toByte().toLong(), v.byte.toLong())
        }
    }

    @Test
    fun testFloat() {
        val value = Variant(
            ByteBuffer.wrap(byteArrayOf(primitiveHeader(14), 0xD2.toByte(), 0x02, 0x96.toByte(), 0x49)),
            EMPTY_METADATA
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT)
            Assert.assertEquals(java.lang.Float.intBitsToFloat(1234567890), v.float, 0f)
        }
    }

    @Test
    fun testNegativeFloat() {
        val value = Variant(
            ByteBuffer.wrap(byteArrayOf(primitiveHeader(14), 0x00, 0x00, 0x00, 0x80.toByte())), EMPTY_METADATA
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.FLOAT)
            Assert.assertEquals(-0.0f, v.float, 0f)
        }
    }

    @Test
    fun testDouble() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(7), 0xB1.toByte(), 0x1C, 0x6C, 0xB1.toByte(), 0xF4.toByte(), 0x10, 0x22, 0x11,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE)
            Assert.assertEquals(java.lang.Double.longBitsToDouble(1234567890987654321L), v.double, 0.0)
        }
    }

    @Test
    fun testNegativeDouble() {
        val value = Variant(
            ByteBuffer.wrap(byteArrayOf(primitiveHeader(7), 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80.toByte())),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DOUBLE)
            Assert.assertEquals(-0.0, v.double, 0.0)
        }
    }

    @Test
    fun testDecimal4() {
        val value = Variant(
            ByteBuffer.wrap(byteArrayOf(primitiveHeader(8), 0x04, 0xD2.toByte(), 0x02, 0x96.toByte(), 0x49)),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4)
            Assert.assertEquals(BigDecimal("123456.7890"), v.decimal)
        }
    }

    @Test
    fun testNegativeDecimal4() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(primitiveHeader(8), 0x04, 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte()),
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL4)
            Assert.assertEquals(BigDecimal("-0.0001"), v.decimal)
        }
    }

    @Test
    fun testDecimal8() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(9), 0x09, 0xB1.toByte(), 0x1C, 0x6C, 0xB1.toByte(), 0xF4.toByte(), 0x10, 0x22, 0x11,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8)
            Assert.assertEquals(BigDecimal("1234567890.987654321"), v.decimal)
        }
    }

    @Test
    fun testNegativeDecimal8() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(9),
                    0x09,
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL8)
            Assert.assertEquals(BigDecimal("-0.000000001"), v.decimal)
        }
    }

    @Test
    fun testDecimal16() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(10),
                    0x09,
                    0x15,
                    0x71,
                    0x34,
                    0xB0.toByte(),
                    0xB8.toByte(),
                    0x87.toByte(),
                    0x10,
                    0x89.toByte(),
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                    0x00,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16)
            Assert.assertEquals(BigDecimal("9876543210.123456789"), v.decimal)
        }
    }

    @Test
    fun testNegativeDecimal16() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(10),
                    0x09,
                    0xEB.toByte(),
                    0x8E.toByte(),
                    0xCB.toByte(),
                    0x4F,
                    0x47,
                    0x78,
                    0xEF.toByte(),
                    0x76,
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DECIMAL16)
            Assert.assertEquals(BigDecimal("-9876543210.123456789"), v.decimal)
        }
    }

    @Test
    fun testDate() {
        val value = Variant(
            ByteBuffer.wrap(byteArrayOf(primitiveHeader(11), 0xE3.toByte(), 0x4E, 0x00, 0x00)), EMPTY_METADATA
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE)
            Assert.assertEquals(LocalDate.parse("2025-04-17"), LocalDate.ofEpochDay(v.int.toLong()))
        }
    }

    @Test
    fun testNegativeDate() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(11),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.DATE)
            Assert.assertEquals(LocalDate.parse("1969-12-31"), LocalDate.ofEpochDay(v.int.toLong()))
        }
    }

    @Test
    fun testTimestamp() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(12),
                    0xC0.toByte(),
                    0x77,
                    0xA1.toByte(),
                    0xEA.toByte(),
                    0xF4.toByte(),
                    0x32,
                    0x06,
                    0x00,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ)
            Assert.assertEquals(
                Instant.parse("2025-04-17T08:09:10.123456Z"), Instant.EPOCH.plus(v.long, ChronoUnit.MICROS)
            )
        }
    }

    @Test
    fun testNegativeTimestamp() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(12),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte()
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_TZ)
            Assert.assertEquals(
                Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH.plus(v.long, ChronoUnit.MICROS)
            )
        }
    }

    @Test
    fun testTimestampNtz() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(13),
                    0xC0.toByte(),
                    0x77,
                    0xA1.toByte(),
                    0xEA.toByte(),
                    0xF4.toByte(),
                    0x32,
                    0x06,
                    0x00,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ)
            Assert.assertEquals(
                Instant.parse("2025-04-17T08:09:10.123456Z"), Instant.EPOCH.plus(v.long, ChronoUnit.MICROS)
            )
        }
    }

    @Test
    fun testNegativeTimestampNtz() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(13),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NTZ)
            Assert.assertEquals(
                Instant.parse("1969-12-31T23:59:59.999999Z"), Instant.EPOCH.plus(v.long, ChronoUnit.MICROS)
            )
        }
    }

    @Test
    fun testBinary() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(15),
                    0x05,
                    0x00,
                    0x00,
                    0x00,
                    'a'.code.toByte(),
                    'b'.code.toByte(),
                    'c'.code.toByte(),
                    'd'.code.toByte(),
                    'e'.code.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.BINARY)
            Assert.assertEquals(
                ByteBuffer.wrap(
                    byteArrayOf(
                        'a'.code.toByte(),
                        'b'.code.toByte(),
                        'c'.code.toByte(),
                        'd'.code.toByte(),
                        'e'.code.toByte(),
                    )
                ), v.binary,
            )
        }
    }

    @Test
    fun testString() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
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
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.STRING)
            Assert.assertEquals("variant", v.string)
        }
    }

    @Test
    fun testShortString() {
        val value =
            Variant(
                ByteBuffer.wrap(
                    byteArrayOf(
                        29,
                        'v'.code.toByte(),
                        'a'.code.toByte(),
                        'r'.code.toByte(),
                        'i'.code.toByte(),
                        'a'.code.toByte(),
                        'n'.code.toByte(),
                        't'.code.toByte(),
                    )
                ), EMPTY_METADATA,
            )
        testVariant(value) { v ->
            checkType(v, VariantUtil.SHORT_STR, Variant.Type.STRING)
            Assert.assertEquals("variant", v.string)
        }
    }

    @Test
    fun testTime() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(17),
                    0x00.toByte(),
                    0x00.toByte(),
                    0xCA.toByte(),
                    0x1D.toByte(),
                    0x14.toByte(),
                    0x00.toByte(),
                    0x00.toByte(),
                    0x00.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIME)
            Assert.assertEquals(LocalTime.parse("23:59:59.123456"), LocalTime.ofNanoOfDay(v.long * 1000))
        }
    }

    @Test
    fun testTimestampNanos() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(18),
                    0x15,
                    0xC9.toByte(),
                    0xBB.toByte(),
                    0x86.toByte(),
                    0xB4.toByte(),
                    0x0C,
                    0x37,
                    0x18,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ)
            Assert.assertEquals(
                Instant.parse("2025-04-17T08:09:10.123456789Z"), Instant.EPOCH.plus(v.long, ChronoUnit.NANOS)
            )
        }
    }

    @Test
    fun testNegativeTimestampNanos() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(18),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_TZ)
            Assert.assertEquals(
                Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.EPOCH.plus(v.long, ChronoUnit.NANOS)
            )
        }
    }

    @Test
    fun testTimestampNanosNtz() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(19),
                    0x15,
                    0xC9.toByte(),
                    0xBB.toByte(),
                    0x86.toByte(),
                    0xB4.toByte(),
                    0x0C,
                    0x37,
                    0x18,
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ)
            Assert.assertEquals(
                Instant.parse("2025-04-17T08:09:10.123456789Z"), Instant.EPOCH.plus(v.long, ChronoUnit.NANOS)
            )
        }
    }

    @Test
    fun testNegativeTimestampNanosNtz() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(19),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.TIMESTAMP_NANOS_NTZ)
            Assert.assertEquals(
                Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.EPOCH.plus(v.long, ChronoUnit.NANOS)
            )
        }
    }

    @Test
    fun testUUID() {
        val value = Variant(
            ByteBuffer.wrap(
                byteArrayOf(
                    primitiveHeader(20),
                    0x00,
                    0x11,
                    0x22,
                    0x33,
                    0x44,
                    0x55,
                    0x66,
                    0x77,
                    0x88.toByte(),
                    0x99.toByte(),
                    0xAA.toByte(),
                    0xBB.toByte(),
                    0xCC.toByte(),
                    0xDD.toByte(),
                    0xEE.toByte(),
                    0xFF.toByte(),
                )
            ),
            EMPTY_METADATA,
        )
        testVariant(value) { v ->
            checkType(v, VariantUtil.PRIMITIVE, Variant.Type.UUID)
            Assert.assertEquals(UUID.fromString("00112233-4455-6677-8899-aabbccddeeff"), v.uUID)
        }
    }

    @Test
    fun testInvalidType() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(0xFC.toByte())), EMPTY_METADATA)
            value.boolean
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals(
                "Cannot read unknownType(basicType: 0, valueHeader: 63) value as BOOLEAN", e.message
            )
        }
    }

    @Test
    fun testInvalidBoolean() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.boolean
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as BOOLEAN", e.message)
        }
    }

    @Test
    fun testInvalidLong() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(16))), EMPTY_METADATA)
            value.long
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals(
                "Cannot read STRING value as one of [BYTE, SHORT, INT, DATE, LONG, TIMESTAMP_TZ, TIMESTAMP_NTZ, TIME, TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ]",
                e.message,
            )
        }
    }

    @Test
    fun testInvalidInt() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.int
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as one of [BYTE, SHORT, INT, DATE]", e.message)
        }
    }

    @Test
    fun testInvalidShort() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.short
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as one of [BYTE, SHORT]", e.message)
        }
    }

    @Test
    fun testInvalidByte() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.byte
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as BYTE", e.message)
        }
    }

    @Test
    fun testInvalidFloat() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.float
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as FLOAT", e.message)
        }
    }

    @Test
    fun testInvalidDouble() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.double
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as DOUBLE", e.message)
        }
    }

    @Test
    fun testInvalidDecimal() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6), 0)), EMPTY_METADATA)
            value.decimal
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as one of [DECIMAL4, DECIMAL8, DECIMAL16]", e.message)
        }
    }

    @Test
    fun testInvalidUUID() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.uUID
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as UUID", e.message)
        }
    }

    @Test
    fun testInvalidString() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.string
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as STRING", e.message)
        }
    }

    @Test
    fun testInvalidBinary() {
        try {
            val value = Variant(ByteBuffer.wrap(byteArrayOf(primitiveHeader(6))), EMPTY_METADATA)
            value.binary
            Assert.fail("Expected exception not thrown")
        } catch (e: Exception) {
            Assert.assertEquals("Cannot read LONG value as BINARY", e.message)
        }
    }
}
