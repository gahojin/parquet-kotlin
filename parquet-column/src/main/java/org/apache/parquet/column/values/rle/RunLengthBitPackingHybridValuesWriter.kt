/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.rle

import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.Companion.concat
import org.apache.parquet.bytes.BytesInput.Companion.fromInt
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.io.ParquetEncodingException
import java.io.IOException

open class RunLengthBitPackingHybridValuesWriter protected constructor(
    @JvmField protected val encoder: RunLengthBitPackingHybridEncoder,
) : ValuesWriter() {
    override val bufferedSize: Long
        get() = encoder.bufferedSize

    override val allocatedSize: Long
        get() = encoder.allocatedSize

    override val bytes: BytesInput
        get() {
            try {
                // prepend the length of the column
                val rle = encoder.toBytes()
                return concat(fromInt(Math.toIntExact(rle.size())), rle)
            } catch (e: IOException) {
                throw ParquetEncodingException(e)
            }
        }

    override val encoding = Encoding.RLE

    constructor(bitWidth: Int, initialCapacity: Int, pageSize: Int, allocator: ByteBufferAllocator) : this(
        encoder = RunLengthBitPackingHybridEncoder(bitWidth, initialCapacity, pageSize, allocator),
    )

    override fun writeInteger(v: Int) {
        try {
            encoder.writeInt(v)
        } catch (e: IOException) {
            throw ParquetEncodingException(e)
        }
    }

    override fun writeBoolean(v: Boolean) {
        writeInteger(if (v) 1 else 0)
    }

    override fun reset() {
        encoder.reset()
    }

    override fun close() {
        encoder.close()
    }

    override fun memUsageString(prefix: String): String {
        return "%s RunLengthBitPackingHybrid %d bytes".format(prefix, allocatedSize)
    }
}
