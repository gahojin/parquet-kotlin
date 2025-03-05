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

import org.apache.parquet.bytes.ByteBufferAllocator
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

/**
 * `SeekableInputStream` is an interface with the methods needed by
 * Parquet to read data from a file or Hadoop data stream.
 */
abstract class SeekableInputStream : InputStream() {
    /** current position in bytes from the start of the stream */
    @get:Throws(IOException::class)
    abstract val pos: Long

    /**
     * Seek to a new position in the InputStream.
     *
     * @param newPos the new position to seek to
     * @throws IOException If the underlying stream throws IOException
     */
    @Throws(IOException::class)
    abstract fun seek(newPos: Long)

    /**
     * Read a byte array of data, from position 0 to the end of the array.
     *
     * This method is equivalent to `read(bytes, 0, bytes.length)`.
     *
     * This method will block until len bytes are available to copy into the
     * array, or will throw [EOFException] if the stream ends before the
     * array is full.
     *
     * @param bytes a byte array to fill with data from the stream
     * @throws IOException  If the underlying stream throws IOException
     * @throws EOFException If the stream has fewer bytes left than are needed to
     * fill the array, `bytes.length`
     */
    @Throws(IOException::class)
    abstract fun readFully(bytes: ByteArray)

    /**
     * Read `len` bytes of data into an array, at position `start`.
     *
     * This method will block until len bytes are available to copy into the
     * array, or will throw [EOFException] if the stream ends before the
     * array is full.
     *
     * @param bytes a byte array to fill with data from the stream
     * @param start the starting position in the byte array for data
     * @param len   the length of bytes to read into the byte array
     * @throws IOException  If the underlying stream throws IOException
     * @throws EOFException If the stream has fewer than `len` bytes left
     */
    @Throws(IOException::class)
    abstract fun readFully(bytes: ByteArray, start: Int, len: Int)

    /**
     * Read `buf.remaining()` bytes of data into a [ByteBuffer].
     *
     * This method will copy available bytes into the buffer, reading at most
     * `buf.remaining()` bytes. The number of bytes actually copied is
     * returned by the method, or -1 is returned to signal that the end of the
     * underlying stream has been reached.
     *
     * @param buf a byte buffer to fill with data from the stream
     * @return the number of bytes read or -1 if the stream ended
     * @throws IOException If the underlying stream throws IOException
     */
    @Throws(IOException::class)
    abstract fun read(buf: ByteBuffer): Int

    /**
     * Read `buf.remaining()` bytes of data into a [ByteBuffer].
     *
     * This method will block until `buf.remaining()` bytes are available
     * to copy into the buffer, or will throw [EOFException] if the stream
     * ends before the buffer is full.
     *
     * @param buf a byte buffer to fill with data from the stream
     * @throws IOException  If the underlying stream throws IOException
     * @throws EOFException If the stream has fewer bytes left than are needed to
     * fill the buffer, `buf.remaining()`
     */
    @Throws(IOException::class)
    abstract fun readFully(buf: ByteBuffer)

    /**
     * Read a set of disjoint file ranges in a vectored manner.
     *
     * @param ranges a list of non-overlapping file ranges to read
     * @param allocator the allocator to use for allocating ByteBuffers
     * @throws UnsupportedOperationException if not available in this class/runtime (default)
     * @throws EOFException if a range is past the known end of the file.
     * @throws IOException any IO problem initiating the read operations.
     * @throws IllegalArgumentException if there are overlapping ranges or
     * a range element is invalid
     */
    @Throws(IOException::class)
    open fun readVectored(
        ranges: List<ParquetFileRange>,
        allocator: ByteBufferAllocator,
    ) {
        throw UnsupportedOperationException("Vectored IO is not supported for $this")
    }

    /**
     * Is the [.readVectored] method available?
     * @param allocator the allocator to use for allocating ByteBuffers
     * @return True if the operation is considered available for this allocator in the hadoop runtime.
     */
    open fun readVectoredAvailable(allocator: ByteBufferAllocator): Boolean {
        return false
    }
}
