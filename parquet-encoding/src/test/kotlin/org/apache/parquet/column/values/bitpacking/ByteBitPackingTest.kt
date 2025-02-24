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
import java.nio.ByteBuffer
import kotlin.math.pow
import kotlin.random.Random

private val LOG: Logger = LoggerFactory.getLogger(ByteBitPackingTest::class.java)

class ByteBitPackingTest : StringSpec({
    "packUnPack" {
        LOG.debug("")
        LOG.debug("testPackUnPack")
        for (i in 1..32) {
            LOG.debug("Width: $i")
            val unpacked = IntArray(32)
            val values = generateValues(i)
            packUnpack(Packer.BIG_ENDIAN.newBytePacker(i), values, unpacked)
            LOG.debug("Output: ${unpacked.joinToString(" ")}")
            values shouldBe unpacked
        }
    }

    "packUnPackLong" {
        LOG.debug("")
        LOG.debug("testPackUnPackLOng")
        for (i in 1..<64) {
            LOG.debug("Width: $i")
            val unpacked32 = LongArray(32)
            val unpacked8 = LongArray(32)
            val values = generateValuesLong(i)

            packUnpack32(Packer.BIG_ENDIAN.newBytePackerForLong(i), values, unpacked32)
            LOG.debug("Output 32: ${unpacked32.joinToString(" ")}")
            values shouldBe unpacked32

            packUnpack8(Packer.BIG_ENDIAN.newBytePackerForLong(i), values, unpacked8)
            LOG.debug("Output 8: ${unpacked8.joinToString(" ")}")
            values shouldBe unpacked8
        }
    }
})

fun packUnpack(packer: BytePacker, values: IntArray, unpacked: IntArray) {
    val packed = ByteArray(packer.bitWidth * 4)
    packer.pack32Values(values, 0, packed, 0)
    LOG.debug("packed: ${packed.joinToString(" ")}")
    packer.unpack32Values(ByteBuffer.wrap(packed), 0, unpacked, 0)
}

fun packUnpack32(packer: BytePackerForLong, values: LongArray, unpacked: LongArray) {
    val packed = ByteArray(packer.bitWidth * 4)
    packer.pack32Values(values, 0, packed, 0)
    LOG.debug("packed: ${packed.joinToString(" ")}")
    packer.unpack32Values(ByteBuffer.wrap(packed), 0, unpacked, 0)
}

fun packUnpack8(packer: BytePackerForLong, values: LongArray, unpacked: LongArray) {
    val packed = ByteArray(packer.bitWidth * 4)
    for (i in 0..<4) {
        packer.pack8Values(values, i * 8, packed, packer.bitWidth * i)
    }
    LOG.debug("packed: ${packed.joinToString(" ")}")
    val wrapPacked = ByteBuffer.wrap(packed)
    for (i in 0..<4) {
        packer.unpack8Values(wrapPacked, packer.bitWidth * i, unpacked, i * 8)
    }
}

fun generateValues(bitWidth: Int): IntArray {
    val random = Random(0)
    val values = IntArray(32) { (random.nextDouble() * 100000 % 2.0.pow(bitWidth.toDouble())).toInt() }
    LOG.debug("Input: ${values.joinToString(" ")}")
    return values
}

fun generateValuesLong(bitWidth: Int): LongArray {
    val random = Random(0)
    val values = LongArray(32) { random.nextLong() and ((1L shl bitWidth) - 1L) }
    LOG.debug("Input: ${values.joinToString(" ")}")
    return values
}
