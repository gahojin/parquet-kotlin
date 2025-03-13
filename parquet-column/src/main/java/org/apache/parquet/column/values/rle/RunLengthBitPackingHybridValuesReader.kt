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

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.bytes.BytesUtils.readIntLittleEndian
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.io.ParquetDecodingException
import java.io.IOException

/**
 * This ValuesReader does all the reading in [.initFromPage]
 * and stores the values in an in memory buffer, which is less than ideal.
 */
class RunLengthBitPackingHybridValuesReader(
    private val bitWidth: Int,
) : ValuesReader() {
    private lateinit var decoder: RunLengthBitPackingHybridDecoder

    @Throws(IOException::class)
    override fun initFromPage(valueCountL: Int, stream: ByteBufferInputStream) {
        val length = readIntLittleEndian(stream)
        decoder = RunLengthBitPackingHybridDecoder(bitWidth, stream.sliceStream(length.toLong()))

        // 4 is for the length which is stored as 4 bytes little endian
        updateNextOffset(length + 4)
    }

    override fun readInteger(): Int {
        try {
            return decoder.readInt()
        } catch (e: IOException) {
            throw ParquetDecodingException(e)
        }
    }

    override fun readBoolean(): Boolean {
        return readInteger() != 0
    }

    override fun skip() {
        readInteger()
    }
}
