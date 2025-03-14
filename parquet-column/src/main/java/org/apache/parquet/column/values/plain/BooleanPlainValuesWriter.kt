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
package org.apache.parquet.column.values.plain

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesWriter
import org.apache.parquet.column.values.bitpacking.Packer

/**
 * An implementation of the PLAIN encoding
 */
class BooleanPlainValuesWriter : ValuesWriter() {
    private val bitPackingWriter = ByteBitPackingValuesWriter(1, Packer.LITTLE_ENDIAN)

    override val bufferedSize: Long
        get() = bitPackingWriter.bufferedSize

    override val bytes: BytesInput
        get() = bitPackingWriter.bytes

    override val allocatedSize: Long
        get() = bitPackingWriter.allocatedSize

    override val encoding: Encoding
        get() = Encoding.PLAIN

    override fun writeBoolean(v: Boolean) {
        bitPackingWriter.writeInteger(if (v) 1 else 0)
    }

    override fun reset() {
        bitPackingWriter.reset()
    }

    override fun close() {
        bitPackingWriter.close()
    }

    override fun memUsageString(prefix: String): String {
        return bitPackingWriter.memUsageString(prefix)
    }
}
