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
package org.apache.parquet.column.values.bytestreamsplit

import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.Companion.from
import org.apache.parquet.bytes.BytesUtils.intToBytes
import org.apache.parquet.bytes.BytesUtils.longToBytes
import org.apache.parquet.bytes.CapacityByteArrayOutputStream
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.io.ParquetEncodingException
import org.apache.parquet.io.api.Binary

abstract class ByteStreamSplitValuesWriter(
    elementSizeInBytes: Int,
    initialCapacity: Int,
    pageSize: Int,
    allocator: ByteBufferAllocator
) : ValuesWriter() {
    protected val numStreams: Int = elementSizeInBytes
    protected val elementSizeInBytes: Int = elementSizeInBytes
    private val byteStreams: Array<CapacityByteArrayOutputStream>

    override val bufferedSize: Long
        get() {
            var totalSize: Long = 0
            for (stream in this.byteStreams) {
                totalSize += stream.size()
            }
            return totalSize
        }

    override val bytes: BytesInput
        get() {
            val allInputs = byteStreams.map { from(it) }.toTypedArray()
            return BytesInput.concat(*allInputs)
        }

    override val encoding: Encoding
        get() = Encoding.BYTE_STREAM_SPLIT

    override val allocatedSize: Long
        get() = byteStreams.sumOf { it.capacity.toLong() }

    init {
        if (elementSizeInBytes <= 0) {
            throw ParquetEncodingException("Element byte size is invalid: $elementSizeInBytes")
        }

        // Round-up the capacity hint.
        val capacityPerStream = (pageSize + elementSizeInBytes - 1) / elementSizeInBytes
        val initialCapacityPerStream = (initialCapacity + elementSizeInBytes - 1) / elementSizeInBytes
        byteStreams = (0..elementSizeInBytes).map {
            CapacityByteArrayOutputStream(initialCapacityPerStream, capacityPerStream, allocator)
        }.toTypedArray()
    }

    override fun reset() {
        for (stream in byteStreams) {
            stream.reset()
        }
    }

    override fun close() {
        for (stream in byteStreams) {
            stream.close()
        }
    }

    protected fun scatterBytes(bytes: ByteArray) {
        if (bytes.size != this.numStreams) {
            throw ParquetEncodingException("Number of bytes doesn't match the number of streams. Num butes: ${bytes.size}, Num streams: $numStreams")
        }
        for (i in bytes.indices) {
            byteStreams[i].write(bytes[i].toInt())
        }
    }

    class FloatByteStreamSplitValuesWriter(
        initialCapacity: Int,
        pageSize: Int,
        allocator: ByteBufferAllocator,
    ) : ByteStreamSplitValuesWriter(java.lang.Float.BYTES, initialCapacity, pageSize, allocator) {
        override fun writeFloat(v: Float) {
            super.scatterBytes(intToBytes(java.lang.Float.floatToIntBits(v)))
        }

        override fun memUsageString(prefix: String): String {
            return "$prefix FloatByteStreamSplitWriter $allocatedSize bytes"
        }
    }

    class DoubleByteStreamSplitValuesWriter(
        initialCapacity: Int,
        pageSize: Int,
        allocator: ByteBufferAllocator,
    ) : ByteStreamSplitValuesWriter(java.lang.Double.BYTES, initialCapacity, pageSize, allocator) {
        override fun writeDouble(v: Double) {
            super.scatterBytes(longToBytes(java.lang.Double.doubleToLongBits(v)))
        }

        override fun memUsageString(prefix: String): String {
            return "$prefix DoubleByteStreamSplitWriter $allocatedSize bytes"
        }
    }

    class IntegerByteStreamSplitValuesWriter(
        initialCapacity: Int,
        pageSize: Int,
        allocator: ByteBufferAllocator,
    ) : ByteStreamSplitValuesWriter(4, initialCapacity, pageSize, allocator) {
        override fun writeInteger(v: Int) {
            super.scatterBytes(intToBytes(v))
        }

        override fun memUsageString(prefix: String): String {
            return "$prefix IntegerByteStreamSplitWriter %d bytes"
        }
    }

    class LongByteStreamSplitValuesWriter(
        initialCapacity: Int,
        pageSize: Int,
        allocator: ByteBufferAllocator,
    ) : ByteStreamSplitValuesWriter(8, initialCapacity, pageSize, allocator) {
        override fun writeLong(v: Long) {
            super.scatterBytes(longToBytes(v))
        }

        override fun memUsageString(prefix: String): String {
            return "%s LongByteStreamSplitWriter %d bytes".format(prefix, allocatedSize)
        }
    }

    class FixedLenByteArrayByteStreamSplitValuesWriter(
        private val length: Int,
        initialCapacity: Int,
        pageSize: Int,
        allocator: ByteBufferAllocator
    ) : ByteStreamSplitValuesWriter(
        length, initialCapacity, pageSize, allocator
    ) {
        override fun writeBytes(v: Binary) {
            assert(v.length() == length) { ("Fixed Binary size " + v.length() + " does not match field type length " + length) }
            super.scatterBytes(v.bytesUnsafe)
        }

        override fun memUsageString(prefix: String): String {
            return "%s FixedLenByteArrayByteStreamSplitValuesWriter %d bytes".format(prefix, allocatedSize)
        }
    }
}
