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

import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer

/**
 * This ByteBufferInputStream does not consume the ByteBuffer being passed in,
 * but will create a slice of the current buffer.
 */
internal class SingleBufferInputStream(
    buffer: ByteBuffer,
) : ByteBufferInputStream() {
    // duplicate the buffer because its state will be modified
    private val buffer: ByteBuffer = buffer.duplicate()
    private val startPosition: Long = buffer.position().toLong()
    private var mark = -1

    override fun position(): Long {
        // position is relative to the start of the stream, not the buffer
        return buffer.position() - startPosition
    }

    @Throws(IOException::class)
    override fun read(): Int {
        if (!buffer.hasRemaining()) {
            throw EOFException()
        }
        return buffer.get().toInt() and 0xFF // as unsigned
    }

    @Throws(IOException::class)
    override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
        if (length == 0) {
            return 0
        }

        val remaining = buffer.remaining()
        if (remaining <= 0) {
            return -1
        }

        val bytesToRead = minOf(buffer.remaining(), length)
        buffer.get(bytes, offset, bytesToRead)

        return bytesToRead
    }

    override fun skip(n: Long): Long {
        if (n == 0L) {
            return 0
        }

        if (buffer.remaining() <= 0) {
            return -1
        }

        // buffer.remaining is an int, so this will always fit in an int
        val bytesToSkip = minOf(buffer.remaining(), n.toInt())
        buffer.position(buffer.position() + bytesToSkip)

        return bytesToSkip.toLong()
    }

    override fun read(out: ByteBuffer): Int {
        val bytesToCopy: Int
        val copyBuffer: ByteBuffer
        if (buffer.remaining() <= out.remaining()) {
            // copy all of the buffer
            bytesToCopy = buffer.remaining()
            copyBuffer = buffer
        } else {
            // copy a slice of the current buffer
            bytesToCopy = out.remaining()
            copyBuffer = buffer.duplicate()
            copyBuffer.limit(buffer.position() + bytesToCopy)
            buffer.position(buffer.position() + bytesToCopy)
        }

        out.put(copyBuffer)
        out.flip()

        return bytesToCopy
    }

    @Throws(EOFException::class)
    override fun slice(length: Int): ByteBuffer {
        if (buffer.remaining() < length) {
            throw EOFException()
        }

        // length is less than remaining, so it must fit in an int
        val copy = buffer.duplicate()
        copy.limit(copy.position() + length)
        buffer.position(buffer.position() + length)

        return copy
    }

    @Throws(EOFException::class)
    override fun sliceBuffers(length: Long): List<ByteBuffer> {
        if (length == 0L) {
            return emptyList()
        }

        if (length > buffer.remaining()) {
            throw EOFException()
        }

        // length is less than remaining, so it must fit in an int
        return listOf(slice(length.toInt()))
    }

    override fun remainingBuffers(): List<ByteBuffer> {
        if (buffer.remaining() <= 0) {
            return emptyList()
        }

        val remaining = buffer.duplicate()
        buffer.position(buffer.limit())

        return listOf(remaining)
    }

    override fun mark(readlimit: Int) {
        mark = buffer.position()
    }

    @Throws(IOException::class)
    override fun reset() {
        if (mark >= 0) {
            buffer.position(mark)
            this.mark = -1
        } else {
            throw IOException("No mark defined")
        }
    }

    override fun markSupported() = true

    override fun available(): Int {
        return buffer.remaining()
    }
}
