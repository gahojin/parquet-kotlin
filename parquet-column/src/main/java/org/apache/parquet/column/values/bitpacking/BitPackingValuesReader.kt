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
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingReader
import org.apache.parquet.column.values.bitpacking.BitPacking.createBitPackingReader
import org.apache.parquet.io.ParquetDecodingException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * a column reader that packs the ints in the number of bits required based on the maximum size.
 */
class BitPackingValuesReader(bound: Int) : ValuesReader() {
    private lateinit var `in`: ByteBufferInputStream
    private lateinit var bitPackingReader: BitPackingReader
    private val bitsPerValue = getWidthFromMaxInt(bound)

    override fun readInteger(): Int {
        try {
            return bitPackingReader.read()
        } catch (e: IOException) {
            throw ParquetDecodingException(e)
        }
    }

    @Throws(IOException::class)
    override fun initFromPage(valueCount: Int, stream: ByteBufferInputStream) {
        val effectiveBitLength = valueCount * bitsPerValue
        val length = paddedByteCountFromBits(effectiveBitLength)
        LOG.debug("reading {} bytes for {} values of size {} bits.", length, valueCount, bitsPerValue)

        `in` = stream.sliceStream(length.toLong())
        bitPackingReader = createBitPackingReader(bitsPerValue, `in`, valueCount.toLong())
        updateNextOffset(length)
    }

    override fun skip() {
        readInteger()
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(BitPackingValuesReader::class.java)
    }
}
