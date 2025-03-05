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

import java.nio.ByteBuffer

/**
 * Convenient class for releasing [java.nio.ByteBuffer] objects with the corresponding allocator;
 *
 * @param allocator the allocator to be used for releasing the buffers
 * @see .releaseLater
 * @see .close
 */
class ByteBufferReleaser(
    @JvmField val allocator: ByteBufferAllocator,
) : AutoCloseable {
    private val toRelease = arrayListOf<ByteBuffer>()

    /**
     * Adds a [ByteBuffer] object to the list of buffers to be released at [.close]. The specified buffer
     * shall be one that was allocated by the [ByteBufferAllocator] of this object.
     *
     * @param buffer the buffer to be released
     */
    fun releaseLater(buffer: ByteBuffer) {
        toRelease.add(buffer)
    }

    override fun close() {
        for (buf in toRelease) {
            allocator.release(buf)
        }
        toRelease.clear()
    }
}
