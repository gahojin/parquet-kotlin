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
package org.apache.parquet.column.values.rle

import org.apache.parquet.bytes.BytesUtils.readIntLittleEndianPaddedOnBitWidth
import org.apache.parquet.bytes.BytesUtils.readUnsignedVarInt
import org.apache.parquet.column.values.bitpacking.BytePacker
import org.apache.parquet.column.values.bitpacking.Packer
import org.apache.parquet.io.ParquetDecodingException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.DataInputStream
import java.io.IOException
import java.io.InputStream
import kotlin.math.ceil

/**
 * Decodes values written in the grammar described in [RunLengthBitPackingHybridEncoder]
 */
open class RunLengthBitPackingHybridDecoder(
    private val bitWidth: Int,
    private val `in`: InputStream,
) {
    private enum class MODE {
        RLE,
        PACKED,
    }

    private val packer: BytePacker

    private var mode: MODE? = null
    private var currentCount = 0
    private var currentValue = 0
    private lateinit var currentBuffer: IntArray

    init {
        LOG.debug("decoding bitWidth {}", bitWidth)

        require(bitWidth >= 0 && bitWidth <= 32) { "bitWidth must be >= 0 and <= 32" }
        packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth)
    }

    @Throws(IOException::class)
    open fun readInt(): Int {
        if (currentCount == 0) {
            readNext()
        }
        --currentCount
        return when (mode) {
            MODE.RLE -> currentValue
            MODE.PACKED -> currentBuffer[currentBuffer.size - 1 - currentCount]
            else -> throw ParquetDecodingException("not a valid mode $mode")
        }
    }

    @Throws(IOException::class)
    private fun readNext() {
        require(`in`.available() > 0) { "Reading past RLE/BitPacking stream." }
        val header = readUnsignedVarInt(`in`)
        mode = if ((header and 1) == 0) MODE.RLE else MODE.PACKED
        when (mode) {
            MODE.RLE -> {
                currentCount = header ushr 1
                LOG.debug("reading {} values RLE", currentCount)
                currentValue = readIntLittleEndianPaddedOnBitWidth(`in`, bitWidth)
            }

            MODE.PACKED -> {
                val numGroups = header ushr 1
                currentCount = numGroups * 8
                LOG.debug("reading {} values BIT PACKED", currentCount)
                currentBuffer = IntArray(currentCount) // TODO: reuse a buffer
                val bytes = ByteArray(numGroups * bitWidth)
                // At the end of the file RLE data though, there might not be that many bytes left.
                var bytesToRead = ceil(currentCount * bitWidth / 8.0).toInt()
                bytesToRead = minOf(bytesToRead, `in`.available())
                DataInputStream(`in`).readFully(bytes, 0, bytesToRead)
                var valueIndex = 0
                var byteIndex = 0
                while (valueIndex < currentCount) {
                    packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex)
                    valueIndex += 8
                    byteIndex += bitWidth
                }
            }

            else -> throw ParquetDecodingException("not a valid mode $mode")
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(RunLengthBitPackingHybridDecoder::class.java)
    }
}
