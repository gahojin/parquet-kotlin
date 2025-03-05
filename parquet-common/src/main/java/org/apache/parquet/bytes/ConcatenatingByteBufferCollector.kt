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
package org.apache.parquet.bytes

import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels

/**
 * Alternative to [ConcatenatingByteArrayCollector] but using [java.nio.ByteBuffer]s allocated by its
 * [ByteBufferAllocator].
 *
 * @param allocator to be used for allocating the required [ByteBuffer] instances
 */
class ConcatenatingByteBufferCollector(
    private val allocator: ByteBufferAllocator,
) : BytesInput(), AutoCloseable {
    private val slabs = arrayListOf<ByteBuffer>()
    private var size: Long = 0L

    /**
     * Collects the content of the specified input. It allocates a new [ByteBuffer] instance that can contain all
     * the content.
     *
     * @param bytesInput the input which content is to be collected
     */
    fun collect(bytesInput: BytesInput) {
        val inputSize = Math.toIntExact(bytesInput.size())
        val slab = allocator.allocate(inputSize)
        bytesInput.writeInto(slab)
        slab!!.flip()
        slabs.add(slab)
        size += inputSize.toLong()
    }

    override fun close() {
        for (slab in slabs) {
            allocator.release(slab)
        }
        slabs.clear()
    }

    @Throws(IOException::class)
    override fun writeAllTo(out: OutputStream) {
        val channel = Channels.newChannel(out)
        for (buffer in slabs) {
            channel.write(buffer.duplicate())
        }
    }

    override fun writeInto(buffer: ByteBuffer) {
        for (slab in slabs) {
            buffer.put(slab.duplicate())
        }
    }

    override val internalByteBuffer: ByteBuffer?
        get() = if (slabs.size == 1) slabs[0].duplicate() else null

    override fun size(): Long {
        return size
    }

    /**
     * @param prefix a prefix to be used for every new line in the string
     * @return a text representation of the memory usage of this structure
     */
    fun memUsageString(prefix: String?): String {
        return String.format("%s %s %d slabs, %,d bytes", prefix, javaClass.simpleName, slabs.size, size)
    }
}
