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
import java.util.*

/**
 * A wrapper [ByteBufferAllocator] implementation that tracks whether all allocated buffers are released. It
 * throws the related exception at [.close] if any buffer remains un-released. It also clears the buffers at
 * release so if they continued being used it'll generate errors.
 *
 * To be used for testing purposes.
 */
class TrackingByteBufferAllocator private constructor(
    private val allocator: ByteBufferAllocator,
) : ByteBufferAllocator, AutoCloseable {
    private class Key(private val buffer: ByteBuffer) {
        private val hashCode = System.identityHashCode(buffer)

        override fun equals(other: Any?): Boolean {
            if (this === other) {
                return true
            }
            return buffer === (other as? Key)?.buffer
        }

        override fun hashCode() = hashCode
    }

    open class LeakDetectorHeapByteBufferAllocatorException : RuntimeException {
        protected constructor(msg: String) : super(msg)

        protected constructor(msg: String, cause: Throwable?) : super(msg, cause)

        protected constructor(
            message: String,
            cause: Throwable?,
            enableSuppression: Boolean,
            writableStackTrace: Boolean,
        ) : super(message, cause, enableSuppression, writableStackTrace)
    }

    class ByteBufferAllocationStacktraceException : LeakDetectorHeapByteBufferAllocatorException {
        private constructor() : super("Allocation stacktrace of the first ByteBuffer:")

        @Suppress("UNUSED_PARAMETER")
        private constructor(unused: Boolean) : super(
            "Set org.apache.parquet.bytes.TrackingByteBufferAllocator.DEBUG = true for more info",
            null,
            false,
            false
        )

        companion object {
            private val WITHOUT_STACKTRACE = ByteBufferAllocationStacktraceException(false)

            fun create(): ByteBufferAllocationStacktraceException {
                return if (DEBUG) ByteBufferAllocationStacktraceException() else WITHOUT_STACKTRACE
            }
        }
    }

    class ReleasingUnallocatedByteBufferException : LeakDetectorHeapByteBufferAllocatorException(
        "Releasing a ByteBuffer instance that is not allocated by this allocator or already been released"
    )

    class LeakedByteBufferException(
        count: Int,
        e: ByteBufferAllocationStacktraceException?,
    ) : LeakDetectorHeapByteBufferAllocatorException(
        "$count ByteBuffer object(s) is/are remained unreleased after closing this allocator.",
        e,
    )

    private val allocated: MutableMap<Key, ByteBufferAllocationStacktraceException?> = HashMap()

    override val isDirect = allocator.isDirect

    override fun allocate(size: Int): ByteBuffer {
        val buffer = allocator.allocate(size)
        allocated[Key(buffer)] = ByteBufferAllocationStacktraceException.create()
        return buffer
    }

    @Throws(ReleasingUnallocatedByteBufferException::class)
    override fun release(b: ByteBuffer) {
        Objects.requireNonNull(b)
        if (allocated.remove(Key(b)) == null) {
            throw ReleasingUnallocatedByteBufferException()
        }
        allocator.release(b)
        // Clearing the buffer so subsequent access would probably generate errors
        b.clear()
    }

    @Throws(LeakedByteBufferException::class)
    override fun close() {
        if (allocated.isNotEmpty()) {
            val ex = LeakedByteBufferException(
                allocated.size, allocated.values.iterator().next()
            )
            allocated.clear() // Drop the references to the ByteBuffers, so they can be gc'd
            throw ex
        }
    }

    companion object {
        /**
         * The stacktraces of the allocation are not stored by default because it significantly decreases the unit test
         * execution performance
         *
         * @see ByteBufferAllocationStacktraceException
         */
        private const val DEBUG = false

        @JvmStatic
        fun wrap(allocator: ByteBufferAllocator): TrackingByteBufferAllocator {
            return TrackingByteBufferAllocator(allocator)
        }
    }
}
