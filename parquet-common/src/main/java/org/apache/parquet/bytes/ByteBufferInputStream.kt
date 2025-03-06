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

import org.apache.parquet.ShouldNeverHappenException
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

open class ByteBufferInputStream(
    // Used to maintain the deprecated behavior of instantiating ByteBufferInputStream directly
    private val delegate: ByteBufferInputStream?,
) : InputStream() {
    internal constructor() : this(null)

    /**
     * @param buffer the buffer to be wrapped in this input stream
     */
    @Deprecated("Will be removed in 2.0.0; Use {@link #wrap(ByteBuffer...)} instead")
    constructor(buffer: ByteBuffer) : this(wrap(buffer))

    /**
     * @param buffer the buffer to be wrapped in this input stream
     * @param offset the offset of the data in the buffer
     * @param count  the number of bytes to be read from the buffer
     */
    @Deprecated("Will be removed in 2.0.0; Use {@link #wrap(ByteBuffer...)} instead")
    constructor(buffer: ByteBuffer, offset: Int, count: Int) : this(buffer.let {
        val temp = it.duplicate()
        temp.position(offset)
        val byteBuf = temp.slice()
        byteBuf.limit(count)
        wrap(byteBuf)
    })

    /**
     * @return the slice of the byte buffer inside this stream
     */
    @Deprecated("Will be removed in 2.0.0; Use {@link #slice(int)} instead")
    fun toByteBuffer(): ByteBuffer {
        try {
            return slice(available())
        } catch (e: EOFException) {
            throw ShouldNeverHappenException(e)
        }
    }

    open fun position(): Long {
        return delegate!!.position()
    }

    @Throws(IOException::class)
    fun skipFully(n: Long) {
        val skipped = skip(n)
        if (skipped < n) {
            throw EOFException("Not enough bytes to skip: $skipped < $n")
        }
    }

    open fun read(out: ByteBuffer): Int {
        return delegate!!.read(out)
    }

    @Throws(EOFException::class)
    open fun slice(length: Int): ByteBuffer {
        return delegate?.slice(length) ?: throw EOFException()
    }

    @Throws(EOFException::class)
    open fun sliceBuffers(length: Long): List<ByteBuffer> {
        return delegate?.sliceBuffers(length) ?: throw EOFException()
    }

    @Throws(EOFException::class)
    fun sliceStream(length: Long): ByteBufferInputStream {
        return wrap(sliceBuffers(length))
    }

    open fun remainingBuffers(): List<ByteBuffer> {
        return delegate?.remainingBuffers() ?: throw EOFException()
    }

    fun remainingStream(): ByteBufferInputStream {
        return wrap(remainingBuffers())
    }

    @Throws(IOException::class)
    override fun read(): Int {
        return delegate?.read() ?: throw EOFException()
    }

    @Throws(IOException::class)
    override fun read(b: ByteArray, off: Int, len: Int): Int {
        return delegate?.read(b, off, len) ?: throw EOFException()
    }

    override fun skip(n: Long): Long {
        return delegate?.skip(n) ?: throw EOFException()
    }

    override fun available(): Int {
        return delegate?.available() ?: throw EOFException()
    }

    override fun mark(readlimit: Int) {
        delegate?.mark(readlimit)
    }

    @Throws(IOException::class)
    override fun reset() {
        delegate?.reset()
    }

    override fun markSupported(): Boolean {
        return delegate?.markSupported() ?: throw EOFException()
    }

    companion object {
        @JvmStatic
        fun wrap(vararg buffers: ByteBuffer): ByteBufferInputStream {
            return if (buffers.size == 1) {
                SingleBufferInputStream(buffers[0])
            } else {
                MultiBufferInputStream(buffers.toList())
            }
        }

        @JvmStatic
        fun wrap(buffers: List<ByteBuffer>): ByteBufferInputStream {
            return if (buffers.size == 1) {
                SingleBufferInputStream(buffers[0])
            } else {
                MultiBufferInputStream(buffers)
            }
        }
    }
}
