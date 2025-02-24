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

import okio.IOException
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.concat
import org.apache.parquet.bytes.BytesUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * @param bitWidth the number of bits used to encode an int
 * @param packer factory for bit packing implementations
 */
class ByteBasedBitPackingEncoder(
    private val bitWidth: Int,
    packer: Packer,
) {
    private val packer = packer.newBytePacker(bitWidth)
    private val input = IntArray(VALUES_WRITTEN_AT_A_TIME)
    private var slabSize: Int = if (bitWidth == 0) 1 else bitWidth * INITIAL_SLAB_SIZE_MULT
    private var totalFullSlabSize: Long = 0L
    private var inputSize: Int = 0
    private var packed: ByteArray = ByteArray(slabSize)
    private var packedPosition: Int = 0
    private val slabs = ArrayList<BytesInput>()
    private var totalValues: Int = 0

    /** size of the data as it would be written */
    val bufferSize: Int
        get() = BytesUtils.paddedByteCountFromBits((totalValues + inputSize) * bitWidth)

    /** total memory allocated */
    val allocatedSize: Long
        get() = totalFullSlabSize + packed.size + input.size * 4

    /** number of full slabs along with the current slab (debug aid) */
    var numSlabs: Int = 0

    /**
     * writes an int using the requested number of bits.
     * accepts only values less than 2^bitWidth
     *
     * @param value the value to write
     * @throws IOException if there is an exception while writing
     */
    @Throws(IOException::class)
    fun writeInt(value: Int) {
        input[inputSize++] = value
        if (inputSize == VALUES_WRITTEN_AT_A_TIME) {
            pack()
            if (packedPosition == slabSize) {
                slabs.add(BytesInput.from(packed))
                numSlabs++
                totalFullSlabSize += slabSize
                if (slabSize < bitWidth * MAX_SLAB_SIZE_MULT) {
                    slabSize *= 2
                }
                initPackedSlab()
            }
        }
    }

    private fun pack() {
        packer.pack8Values(input, 0, packed, packedPosition)
        packedPosition += bitWidth
        totalValues += inputSize
        inputSize = 0
    }

    private fun initPackedSlab() {
        packed = ByteArray(slabSize)
        packedPosition = 0
    }

    /**
     * @return the bytes representing the packed values
     * @throws IOException if there is an exception while creating the BytesInput
     */
    @Throws(IOException::class)
    fun toBytes(): BytesInput {
        val packedByteLength = packedPosition + BytesUtils.paddedByteCountFromBits(inputSize * bitWidth)

        LOG.debug("writing {} bytes", totalFullSlabSize + packedByteLength)

        if (inputSize > 0) {
            for (i in inputSize..<input.size) {
                input[i] = 0
            }
            pack()
        }
        return concat(concat(slabs), BytesInput.from(packed, 0, packedByteLength))
    }

    fun memUsageString(prefix: String): String {
        return "$prefix ByteBitPacking ${slabs.size} slabs, $allocatedSize bytes"
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(ByteBasedBitPackingEncoder::class.java)
        private const val VALUES_WRITTEN_AT_A_TIME: Int = 8
        private const val MAX_SLAB_SIZE_MULT: Int = 64 * 1024
        private const val INITIAL_SLAB_SIZE_MULT: Int = 1024
    }
}
