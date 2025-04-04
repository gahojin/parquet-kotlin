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

private val LOG: Logger = LoggerFactory.getLogger(LemireBitPackingTest::class.java)

class LemireBitPackingTest : StringSpec({
    "packUnPack" {
        for (packer in Packer.entries) {
            LOG.debug("")
            LOG.debug("testPackUnPack")
            for (i in 1..<32) {
                LOG.debug("Width: $i")
                val values = generateValues(i)
                val unpacked = IntArray(32)

                packUnpack(packer.newIntPacker(i), values, unpacked)
                LOG.debug("int based Output ${packer::class.simpleName}: ${unpacked.joinToString(" ")}")
                values shouldBe unpacked

                packUnpack(packer.newBytePacker(i), values, unpacked)
                LOG.debug("byte based Output ${packer::class.simpleName}: ${unpacked.joinToString(" ")}")
                values shouldBe unpacked
            }
        }
    }
})

fun packUnpack(packer: IntPacker, values: IntArray, unpacked: IntArray) {
    val packed = IntArray(packer.bitWidth)
    packer.pack32Values(values, 0, packed, 0)
    LOG.debug("packed: ${packed.joinToString(" ")}")
    packer.unpack32Values(packed, 0, unpacked, 0)
}
