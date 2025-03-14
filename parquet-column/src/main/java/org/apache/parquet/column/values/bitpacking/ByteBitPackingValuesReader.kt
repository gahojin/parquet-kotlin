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
import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt
import org.apache.parquet.bytes.BytesUtils.paddedByteCountFromBits
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.io.ParquetDecodingException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class ByteBitPackingValuesReader(bound: Int, packer: Packer) : ValuesReader() {
    private val bitWidth = getWidthFromMaxInt(bound)
    private val packer = packer.newBytePacker(bitWidth)
    private val decoded = IntArray(VALUES_AT_A_TIME)
    private var decodedPosition = VALUES_AT_A_TIME - 1
    private lateinit var `in`: ByteBufferInputStream
    // Create and retain byte array to avoid object creation in the critical path
    private val tempEncode: ByteArray = ByteArray(bitWidth)

    private fun readMore() {
        try {
            val avail = `in`.available()
            if (avail < bitWidth) {
                `in`.read(tempEncode, 0, avail)
                // Clear the portion of the array we didn't read into
                for (i in avail..<bitWidth) tempEncode[i] = 0
            } else {
                `in`.read(tempEncode, 0, bitWidth)
            }

            // The "deprecated" unpacker is faster than using the one that takes ByteBuffer
            packer.unpack8Values(tempEncode, 0, decoded, 0)
        } catch (e: IOException) {
            throw ParquetDecodingException("Failed to read packed values", e)
        }
        decodedPosition = 0
    }

    override fun readInteger(): Int {
        ++decodedPosition
        if (decodedPosition == decoded.size) {
            readMore()
        }
        return decoded[decodedPosition]
    }

    @Throws(IOException::class)
    override fun initFromPage(valueCount: Int, stream: ByteBufferInputStream) {
        val effectiveBitLength = valueCount * bitWidth
        var length = paddedByteCountFromBits(effectiveBitLength) // ceil
        LOG.debug("reading {} bytes for {} values of size {} bits.", length, valueCount, bitWidth)
        // work-around for null values. this will not happen for repetition or
        // definition levels (never null), but will happen when valueCount has not
        // been adjusted for null values in the data.
        length = minOf(length, stream.available())
        `in` = stream.sliceStream(length.toLong())
        decodedPosition = VALUES_AT_A_TIME - 1
        updateNextOffset(length)
    }

    override fun skip() {
        readInteger()
    }

    companion object {
        private const val VALUES_AT_A_TIME = 8 // because we're using unpack8Values()

        private val LOG: Logger = LoggerFactory.getLogger(ByteBitPackingValuesReader::class.java)
    }
}
