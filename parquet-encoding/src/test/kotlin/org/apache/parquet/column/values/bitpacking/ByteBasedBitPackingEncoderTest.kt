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
import org.apache.parquet.bytes.BytesUtils

class ByteBasedBitPackingEncoderTest : StringSpec({
    "slabBoundary" {
        for (i in 0..32) {
            val encoder = ByteBasedBitPackingEncoder(i, Packer.BIG_ENDIAN)

            // make sure to write through the progression of slabs
            val totalValues = 191 * 1024 * 8 + 10

            for (j in 0..<totalValues) {
                try {
                    encoder.writeInt(j)
                } catch (e: Exception) {
                    throw RuntimeException("$i: error writing $j", e)
                }
            }
            encoder.bufferSize shouldBe BytesUtils.paddedByteCountFromBits(totalValues * i)
            encoder.numSlabs shouldBe if (i == 0) 0L else 8
        }
    }
})
