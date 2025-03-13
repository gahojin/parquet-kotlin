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
import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.Companion.from
import org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt
import org.apache.parquet.bytes.CapacityByteArrayOutputStream
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingWriter
import org.apache.parquet.column.values.bitpacking.BitPacking.getBitPackingWriter
import org.apache.parquet.io.ParquetEncodingException

/**
 * a column writer that packs the ints in the number of bits required based on the maximum size.
 *
 * @param bound           the maximum value stored by this column
 * @param initialCapacity initial capacity for the writer
 * @param pageSize        the page size
 * @param allocator       a buffer allocator
 */
class BitPackingValuesWriter(
    bound: Int,
    initialCapacity: Int,
    pageSize: Int,
    allocator: ByteBufferAllocator,
) : ValuesWriter() {
    private val out = CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator)
    private val bitsPerValue = getWidthFromMaxInt(bound)
    private var bitPackingWriter: BitPackingWriter = getBitPackingWriter(bitsPerValue, out)

    override val allocatedSize: Long
        get() = out.capacity.toLong()

    override val encoding: Encoding = Encoding.BIT_PACKED

    override val bufferedSize: Long
        get() = out.size()

    override val bytes: BytesInput
        get() {
            try {
                bitPackingWriter.finish()
                return from(out)
            } catch (e: IOException) {
                throw ParquetEncodingException(e)
            }
        }

    private fun init() {
        bitPackingWriter = getBitPackingWriter(bitsPerValue, out)
    }

    override fun writeInteger(v: Int) {
        try {
            bitPackingWriter.write(v)
        } catch (e: IOException) {
            throw ParquetEncodingException(e)
        }
    }

    override fun reset() {
        out.reset()
        init()
    }

    override fun close() {
        out.close()
    }

    override fun memUsageString(prefix: String): String {
        return out.memUsageString(prefix)
    }
}
