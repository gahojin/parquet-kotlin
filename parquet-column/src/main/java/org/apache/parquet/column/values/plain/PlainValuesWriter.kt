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

import okio.IOException
import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.Companion.from
import org.apache.parquet.bytes.CapacityByteArrayOutputStream
import org.apache.parquet.bytes.LittleEndianDataOutputStream
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.io.ParquetEncodingException
import org.apache.parquet.io.api.Binary
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Plain encoding except for booleans
 */
class PlainValuesWriter(initialSize: Int, pageSize: Int, allocator: ByteBufferAllocator) : ValuesWriter() {
    private val arrayOut = CapacityByteArrayOutputStream(initialSize, pageSize, allocator)
    private val out = LittleEndianDataOutputStream(arrayOut)

    override val bufferedSize: Long
        get() = arrayOut.size()

    override val bytes: BytesInput
        get() {
            try {
                out.flush()
            } catch (e: IOException) {
                throw ParquetEncodingException("could not write page", e)
            }
            if (LOG.isDebugEnabled) LOG.debug(
                "writing a buffer of size {}",
                arrayOut.size()
            )
            return from(arrayOut)
        }

    override val allocatedSize: Long
        get() = arrayOut.capacity.toLong()

    override val encoding: Encoding = Encoding.PLAIN

    override fun writeBytes(v: Binary) {
        try {
            out.writeInt(v.length())
            v.writeTo(out)
        } catch (e: IOException) {
            throw ParquetEncodingException("could not write bytes", e)
        }
    }

    override fun writeInteger(v: Int) {
        try {
            out.writeInt(v)
        } catch (e: IOException) {
            throw ParquetEncodingException("could not write int", e)
        }
    }

    override fun writeLong(v: Long) {
        try {
            out.writeLong(v)
        } catch (e: IOException) {
            throw ParquetEncodingException("could not write long", e)
        }
    }

    override fun writeFloat(v: Float) {
        try {
            out.writeFloat(v)
        } catch (e: IOException) {
            throw ParquetEncodingException("could not write float", e)
        }
    }

    override fun writeDouble(v: Double) {
        try {
            out.writeDouble(v)
        } catch (e: IOException) {
            throw ParquetEncodingException("could not write double", e)
        }
    }

    override fun writeByte(value: Int) {
        try {
            out.write(value)
        } catch (e: IOException) {
            throw ParquetEncodingException("could not write byte", e)
        }
    }

    override fun reset() {
        arrayOut.reset()
    }

    override fun close() {
        arrayOut.close()
        out.close()
    }

    override fun memUsageString(prefix: String): String {
        return arrayOut.memUsageString("$prefix PLAIN")
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(PlainValuesWriter::class.java)
    }
}
