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
package org.apache.parquet.column.values.bitpacking

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

private val LOG: Logger = LoggerFactory.getLogger(BitPackingTest::class.java)

class BitPackingTest : StringSpec({
    "zero" {
        val values = intArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        validateEncodeDecode(0, values, "")
    }

    "one_0" {
        val values = intArrayOf(0)
        validateEncodeDecode(1, values, "00000000")
    }

    "one_1" {
        val values = intArrayOf(1)
        validateEncodeDecode(1, values, "10000000")
    }

    "one_0_0" {
        val values = intArrayOf(0, 0)
        validateEncodeDecode(1, values, "00000000")
    }

    "one_1_1" {
        val values = intArrayOf(1, 1)
        validateEncodeDecode(1, values, "11000000")
    }

    "one_9_1s" {
        val values = intArrayOf(1, 1 ,1, 1, 1, 1, 1, 1, 1)
        validateEncodeDecode(1, values, "11111111 10000000")
    }

    "one_9_0s" {
        val values = intArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0)
        validateEncodeDecode(1, values, "00000000 00000000")
    }

    "one_7_0s_1_1" {
        val values = intArrayOf(0, 0, 0, 0, 0, 0, 0, 1)
        validateEncodeDecode(1, values, "00000001")
    }

    "one_9_0s_1_1" {
        val values = intArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 1)
        validateEncodeDecode(1, values, "00000000 01000000")
    }

    "one" {
        val values = intArrayOf(0, 1, 0, 0, 1, 1, 1, 0, 0, 1)
        validateEncodeDecode(1, values, "01001110 01000000")
    }

    "two" {
        val values = intArrayOf(0, 1, 2, 3, 3, 3, 2, 1, 1, 0, 0, 0, 1)
        validateEncodeDecode(2, values, "00011011 11111001 01000000 01000000")
    }

    "three" {
        val values = intArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 1)
        validateEncodeDecode(3, values, "00000101 00111001 01110111 00100000")
    }

    "four" {
        val values = intArrayOf(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1)
        validateEncodeDecode(4, values, "00000001 00100011 01000101 01100111 10001001 10101011 11001101 11101111 00010000")
    }

    "five" {
        val values = intArrayOf(
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
            29, 30, 31, 1,
        )
        validateEncodeDecode(5, values, """
            00000000 01000100 00110010 00010100 11000111 01000010 01010100 10110110 00110101 11001111 10000100 01100101 00111010 01010110 11010111 11000110 01110101 10111110 01110111 11011111 00001000
        """.trimIndent())
    }

    "six" {
        val values = intArrayOf(0, 28, 34, 35, 63, 1)
        validateEncodeDecode(6, values, "00000001 11001000 10100011 11111100 00010000")
    }

    "seven" {
        val values = intArrayOf(0, 28, 34, 35, 63, 1, 125, 1, 1)
        validateEncodeDecode(7, values, "00000000 01110001 00010010 00110111 11100000 01111110 10000001 00000010")
    }
})

@OptIn(ExperimentalStdlibApi::class)
private fun validateEncodeDecode(bitLength: Int, values: IntArray, expected: String) {
    val baos = ByteArrayOutputStream()
    val w = BitPacking.getBitPackingWriter(bitLength, baos)
    values.forEach { w.write(it) }
    w.finish()

    val bytes = baos.toByteArray()
    LOG.debug("vals ($bitLength): ${values.joinToString(" ")}")
    val actual = bytes.joinToString(" ") { Integer.toBinaryString(it.toInt() and 0xff).padStart(8, '0') }
    LOG.debug("bytes: $actual")

    actual shouldBe expected

    val bais = ByteArrayInputStream(bytes)
    val r = BitPacking.createBitPackingReader(bitLength, bais, values.size.toLong())
    val result = IntArray(values.size) { r.read() }
    LOG.debug("result: ${result.joinToString(" ")}")
    result shouldBe values
}
