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
 * A special [ByteBufferAllocator] implementation that keeps one [ByteBuffer] object and reuses it at the
 * next [.allocate] call. The [.close] shall be called when this allocator is not needed anymore to
 * really release the one buffer.
 */
abstract class ReusingByteBufferAllocator private constructor(
    private val allocator: ByteBufferAllocator,
) : ByteBufferAllocator, AutoCloseable {
    /**
     * A convenience method to get a [ByteBufferReleaser] instance already created for this allocator.
     *
     * @return a releaser for this allocator
     */
    val releaser: ByteBufferReleaser = ByteBufferReleaser(this)

    private var buffer: ByteBuffer? = null
    private var bufferOut: ByteBuffer? = null

    override val isDirect = allocator.isDirect

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if strict and the one buffer was not released yet
     * @see .strict
     * @see .unsafe
     */
    override fun allocate(size: Int): ByteBuffer {
        allocateCheck(bufferOut)

        val newBuf = buffer?.let { buf ->
            if (buf.capacity() < size) {
                allocator.release(buf)
                allocator.allocate(size).also {
                    buffer = it
                }
            } else {
                buf.clear()
                buf.limit(size)
                buf.slice()
            }
        } ?: run {
            allocator.allocate(size).also {
                buffer = it
            }
        }
        bufferOut = newBuf
        return newBuf
    }

    abstract fun allocateCheck(bufferOut: ByteBuffer?)

    override fun release(b: ByteBuffer) {
        checkNotNull(bufferOut) { "The single buffer has already been released or never allocated" }
        require(b === bufferOut) { "The buffer to be released is not the one allocated by this allocator" }
        bufferOut = null
    }

    override fun close() {
        buffer?.also {
            allocator.release(it)
        }
        buffer = null
        bufferOut = null
    }

    companion object {
        /**
         * Creates a new strict [ReusingByteBufferAllocator] object with the specified "parent" allocator to be used for
         * allocating/releasing the one buffer.
         *
         * Strict means it is enforced that [.release] is invoked before a new [.allocate] can be called.
         *
         * @param allocator the allocator to be used for allocating/releasing the one buffer
         * @return a new strict [ReusingByteBufferAllocator] object
         */
        @JvmStatic
        fun strict(allocator: ByteBufferAllocator): ReusingByteBufferAllocator {
            return object : ReusingByteBufferAllocator(allocator) {
                override fun allocateCheck(bufferOut: ByteBuffer?) {
                    check(bufferOut == null) { "The single buffer is not yet released" }
                }
            }
        }

        /**
         * Creates a new unsafe [ReusingByteBufferAllocator] object with the specified "parent" allocator to be used for
         * allocating/releasing the one buffer.
         *
         * Unsafe means it is not enforced that [.release] is invoked before a new [.allocate]
         * can be called, i.e. no exceptions will be thrown at [.allocate].
         *
         * @param allocator the allocator to be used for allocating/releasing the one buffer
         * @return a new unsafe [ReusingByteBufferAllocator] object
         */
        @JvmStatic
        fun unsafe(allocator: ByteBufferAllocator): ReusingByteBufferAllocator {
            return object : ReusingByteBufferAllocator(allocator) {
                override fun allocateCheck(bufferOut: ByteBuffer?) {
                    // no-op
                }
            }
        }
    }
}
