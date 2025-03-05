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
package org.apache.parquet.io

import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

/**
 * Implements read methods required by [SeekableInputStream] for generic input streams.
 *
 * Implementations must implement [.getPos] and [.seek] and may optionally
 * implement other read methods to improve performance.
 */
abstract class DelegatingSeekableInputStream(val stream: InputStream) : SeekableInputStream() {
    private val temp = ByteArray(COPY_BUFFER_SIZE)

    @get:Throws(IOException::class)
    abstract override val pos: Long

    @Throws(IOException::class)
    override fun close() {
        stream.close()
    }

    @Throws(IOException::class)
    abstract override fun seek(newPos: Long)

    @Throws(IOException::class)
    override fun read(): Int {
        return stream.read()
    }

    @Throws(IOException::class)
    override fun read(b: ByteArray, off: Int, len: Int): Int {
        return stream.read(b, off, len)
    }

    @Throws(IOException::class)
    override fun readFully(bytes: ByteArray) {
        readFully(stream, bytes, 0, bytes.size)
    }

    @Throws(IOException::class)
    override fun readFully(bytes: ByteArray, start: Int, len: Int) {
        readFully(stream, bytes, start, len)
    }

    @Throws(IOException::class)
    override fun read(buf: ByteBuffer): Int {
        return if (buf.hasArray()) {
            readHeapBuffer(stream, buf)
        } else {
            readDirectBuffer(stream, buf, temp)
        }
    }

    @Throws(IOException::class)
    override fun readFully(buf: ByteBuffer) {
        if (buf.hasArray()) {
            readFullyHeapBuffer(stream, buf)
        } else {
            readFullyDirectBuffer(stream, buf, temp)
        }
    }

    companion object {
        private const val COPY_BUFFER_SIZE = 8192

        // Visible for testing
        @JvmStatic
        @Throws(IOException::class)
        fun readFully(f: InputStream, bytes: ByteArray, start: Int, len: Int) {
            var offset = start
            var remaining = len
            while (remaining > 0) {
                val bytesRead = f.read(bytes, offset, remaining)
                if (bytesRead < 0) {
                    throw EOFException("Reached the end of stream with $remaining bytes left to read")
                }

                remaining -= bytesRead
                offset += bytesRead
            }
        }

        // Visible for testing
        @JvmStatic
        @Throws(IOException::class)
        fun readHeapBuffer(f: InputStream, buf: ByteBuffer): Int {
            val bytesRead = f.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining())
            if (bytesRead < 0) {
                // if this resulted in EOF, don't update position
                return bytesRead
            } else {
                buf.position(buf.position() + bytesRead)
                return bytesRead
            }
        }

        // Visible for testing
        @JvmStatic
        @Throws(IOException::class)
        fun readFullyHeapBuffer(f: InputStream, buf: ByteBuffer) {
            readFully(f, buf.array(), buf.arrayOffset() + buf.position(), buf.remaining())
            buf.position(buf.limit())
        }

        // Visible for testing
        @JvmStatic
        @Throws(IOException::class)
        fun readDirectBuffer(f: InputStream, buf: ByteBuffer, temp: ByteArray): Int {
            // copy all the bytes that return immediately, stopping at the first
            // read that doesn't return a full buffer.
            var nextReadLength = minOf(buf.remaining(), temp.size)
            var totalBytesRead = 0
            var bytesRead: Int

            while ((f.read(temp, 0, nextReadLength).also { bytesRead = it }) == temp.size) {
                buf.put(temp)
                totalBytesRead += bytesRead
                nextReadLength = minOf(buf.remaining(), temp.size)
            }

            if (bytesRead < 0) {
                // return -1 if nothing was read
                return if (totalBytesRead == 0) -1 else totalBytesRead
            } else {
                // copy the last partial buffer
                buf.put(temp, 0, bytesRead)
                totalBytesRead += bytesRead
                return totalBytesRead
            }
        }

        // Visible for testing
        @JvmStatic
        @Throws(IOException::class)
        fun readFullyDirectBuffer(f: InputStream, buf: ByteBuffer, temp: ByteArray) {
            var nextReadLength = minOf(buf.remaining(), temp.size)
            var bytesRead = 0

            while (nextReadLength > 0 && (f.read(temp, 0, nextReadLength).also { bytesRead = it }) >= 0) {
                buf.put(temp, 0, bytesRead)
                nextReadLength = minOf(buf.remaining(), temp.size)
            }

            if (bytesRead < 0 && buf.remaining() > 0) {
                throw EOFException("Reached the end of stream with ${buf.remaining()} bytes left to read")
            }
        }
    }
}
