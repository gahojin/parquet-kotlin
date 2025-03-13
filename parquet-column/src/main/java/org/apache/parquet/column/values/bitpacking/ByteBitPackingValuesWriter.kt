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
import org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.io.ParquetEncodingException

class ByteBitPackingValuesWriter(bound: Int, private val packer: Packer) : ValuesWriter() {
    private val bitWidth = getWidthFromMaxInt(bound)
    private var encoder = ByteBasedBitPackingEncoder(bitWidth, packer)

    override val encoding: Encoding = Encoding.BIT_PACKED

    override val bytes: BytesInput
        get() {
            try {
                return encoder.toBytes()
            } catch (e: IOException) {
                throw ParquetEncodingException(e)
            }
        }

    override val bufferedSize: Long
        get() = encoder.bufferSize.toLong()

    override val allocatedSize: Long
        get() = encoder.allocatedSize

    override fun writeInteger(v: Int) {
        try {
            encoder.writeInt(v)
        } catch (e: IOException) {
            throw ParquetEncodingException(e)
        }
    }

    override fun reset() {
        encoder = ByteBasedBitPackingEncoder(bitWidth, packer)
    }

    override fun memUsageString(prefix: String): String {
        return encoder.memUsageString(prefix)
    }
}
