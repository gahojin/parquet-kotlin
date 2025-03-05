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

/**
 * Used for collecting the content of [BytesInput] objects.
 */
@Deprecated("Use {@link ConcatenatingByteBufferCollector} instead.", replaceWith = ReplaceWith("ConcatenatingByteArrayCollector"))
class ConcatenatingByteArrayCollector : BytesInput() {
    private val slabs: MutableList<ByteArray> = ArrayList()
    private var size: Long = 0L

    @Throws(IOException::class)
    fun collect(bytesInput: BytesInput) {
        val bytes = bytesInput.toByteArray()
        slabs.add(bytes)
        size += bytes.size.toLong()
    }

    fun reset() {
        size = 0
        slabs.clear()
    }

    @Throws(IOException::class)
    override fun writeAllTo(out: OutputStream) {
        for (slab in slabs) {
            out.write(slab)
        }
    }

    override fun writeInto(buffer: ByteBuffer) {
        for (slab in slabs) {
            buffer.put(slab)
        }
    }

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
