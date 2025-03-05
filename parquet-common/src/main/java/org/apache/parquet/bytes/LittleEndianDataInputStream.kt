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
import java.io.InputStream

/**
 * Based on DataInputStream but little endian and without the String/char methods
 *
 * @param delegated the specified input stream
 */
class LittleEndianDataInputStream(
    private val delegated: InputStream,
) : InputStream() {
    private val readBuffer = ByteArray(8)

    /**
     * See the general contract of the `readFully`
     * method of `DataInput`.
     *
     * Bytes for this operation are read from the contained input stream.
     *
     * @param b the buffer into which the data is read.
     * @throws EOFException if this input stream reaches the end before reading all the bytes.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @JvmOverloads
    @Throws(IOException::class)
    fun readFully(b: ByteArray, off: Int = 0, len: Int = b.size) {
        if (len < 0) throw IndexOutOfBoundsException()
        var n = 0
        while (n < len) {
            val count = delegated.read(b, off + n, len - n)
            if (count < 0) throw EOFException()
            n += count
        }
    }

    /**
     * See the general contract of the `skipBytes`
     * method of `DataInput`.
     *
     * Bytes for this operation are read from the contained input stream.
     *
     * @param n the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     * @throws IOException if the contained input stream does not support
     * seek, or the stream has been closed and
     * the contained input stream does not support
     * reading after close, or another I/O error occurs.
     */
    @Throws(IOException::class)
    fun skipBytes(n: Int): Int {
        var total = 0
        var cur = 0

        while ((total < n) && ((delegated.skip((n - total).toLong()).toInt().also { cur = it }) > 0)) {
            total += cur
        }

        return total
    }

    /**
     * @return the next byte in the stream as an int
     * @throws IOException if there is an exception while reading
     * @see java.io.InputStream.read
     */
    @Throws(IOException::class)
    override fun read(): Int {
        return delegated.read()
    }

    /**
     * @return the hash code of the wrapped input stream
     * @see java.lang.Object.hashCode
     */
    override fun hashCode(): Int {
        return delegated.hashCode()
    }

    /**
     * @param b a byte array
     * @return the number of bytes read
     * @throws IOException if there was an exception while reading
     * @see java.io.InputStream.read
     */
    @Throws(IOException::class)
    override fun read(b: ByteArray): Int {
        return delegated.read(b)
    }

    /**
     * @param other another object
     * @return true if this is equal to the object
     * @see java.lang.Object.equals
     */
    override fun equals(other: Any?): Boolean {
        return delegated == other
    }

    /**
     * @param b   a byte array
     * @param off an offset into the byte array
     * @param len the length to read
     * @return the number of bytes read
     * @throws IOException if there was an exception while reading
     * @see java.io.InputStream.read
     */
    @Throws(IOException::class)
    override fun read(b: ByteArray, off: Int, len: Int): Int {
        return delegated.read(b, off, len)
    }

    /**
     * @param n the number of bytes to skip
     * @return the number of bytes skipped
     * @throws IOException if there was an exception while reading
     * @see java.io.InputStream.skip
     */
    @Throws(IOException::class)
    override fun skip(n: Long): Long {
        return delegated.skip(n)
    }

    /**
     * @return the number of bytes available in the wrapped stream
     * @throws IOException if there was an exception while getting the number of available bytes
     * @see java.io.InputStream.available
     */
    @Throws(IOException::class)
    override fun available(): Int {
        return delegated.available()
    }

    /**
     * @throws IOException if there was an exception while closing the underlying stream
     * @see java.io.InputStream.close
     */
    @Throws(IOException::class)
    override fun close() {
        delegated.close()
    }

    /**
     * @param readlimit the number of bytes the mark will be valid for
     * @see java.io.InputStream.mark
     */
    override fun mark(readlimit: Int) {
        delegated.mark(readlimit)
    }

    /**
     * @throws IOException if there is an exception while resetting the underlying stream
     * @see java.io.InputStream.reset
     */
    @Throws(IOException::class)
    override fun reset() {
        delegated.reset()
    }

    /**
     * @return true if mark is supported
     * @see java.io.InputStream.markSupported
     */
    override fun markSupported(): Boolean {
        return delegated.markSupported()
    }

    /**
     * See the general contract of the `readBoolean` method of `DataInput`.
     *
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the `boolean` value read.
     * @throws EOFException if this input stream has reached the end.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @Throws(IOException::class)
    fun readBoolean(): Boolean {
        val ch = delegated.read()
        if (ch < 0) throw EOFException()
        return (ch != 0)
    }

    /**
     * See the general contract of the `readByte` method of `DataInput`.
     *
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream as a signed 8-bit `byte`.
     * @throws EOFException if this input stream has reached the end.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @Throws(IOException::class)
    fun readByte(): Byte {
        val ch = delegated.read()
        if (ch < 0) throw EOFException()
        return (ch).toByte()
    }

    /**
     * See the general contract of the `readUnsignedByte`
     * method of `DataInput`.
     *
     *
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next byte of this input stream, interpreted as an unsigned 8-bit number.
     * @throws EOFException if this input stream has reached the end.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @Throws(IOException::class)
    fun readUnsignedByte(): Int {
        val ch = delegated.read()
        if (ch < 0) throw EOFException()
        return ch
    }

    /**
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as a
     * signed 16-bit number.
     * @throws EOFException if this input stream reaches the end before
     * reading two bytes.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @Throws(IOException::class)
    fun readShort(): Short {
        val ch2 = delegated.read()
        val ch1 = delegated.read()
        if ((ch1 or ch2) < 0) throw EOFException()
        return ((ch1 shl 8) + (ch2 shl 0)).toShort()
    }

    /**
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next two bytes of this input stream, interpreted as an unsigned 16-bit integer.
     * @throws EOFException if this input stream reaches the end before reading two bytes.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @Throws(IOException::class)
    fun readUnsignedShort(): Int {
        val ch2 = delegated.read()
        val ch1 = delegated.read()
        if ((ch1 or ch2) < 0) throw EOFException()
        return (ch1 shl 8) + (ch2 shl 0)
    }

    /**
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next four bytes of this input stream, interpreted as an `int`.
     * @throws EOFException if this input stream reaches the end before reading four bytes.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @Throws(IOException::class)
    fun readInt(): Int {
        // TODO: has this been benchmarked against two alternate implementations?
        // 1) Integer.reverseBytes(in.readInt())
        // 2) keep a member byte[4], wrapped by an IntBuffer with appropriate endianness set,
        //    and call IntBuffer.get()
        // Both seem like they might be faster.
        val ch4 = delegated.read()
        val ch3 = delegated.read()
        val ch2 = delegated.read()
        val ch1 = delegated.read()
        if ((ch1 or ch2 or ch3 or ch4) < 0) throw EOFException()
        return ((ch1 shl 24) + (ch2 shl 16) + (ch3 shl 8) + (ch4 shl 0))
    }

    /**
     * Bytes for this operation are read from the contained input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a `long`.
     * @throws EOFException if this input stream reaches the end before reading eight bytes.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.io.FilterInputStream.in
     */
    @Throws(IOException::class)
    fun readLong(): Long {
        // TODO: see perf question above in readInt
        readFully(readBuffer, 0, 8)
        return ((readBuffer[7].toLong() shl 56) or
                ((readBuffer[6].toInt() and 255).toLong() shl 48) or
                ((readBuffer[5].toInt() and 255).toLong() shl 40) or
                ((readBuffer[4].toInt() and 255).toLong() shl 32) or
                ((readBuffer[3].toInt() and 255).toLong() shl 24) or
                ((readBuffer[2].toInt() and 255).toLong() shl 16) or
                ((readBuffer[1].toInt() and 255).toLong() shl 8) or
                ((readBuffer[0].toInt() and 255).toLong()))
    }

    /**
     * Bytes
     * for this operation are read from the contained
     * input stream.
     *
     * @return the next four bytes of this input stream, interpreted as a `float`.
     * @throws EOFException if this input stream reaches the end before reading four bytes.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.lang.Float.intBitsToFloat
     */
    @Throws(IOException::class)
    fun readFloat(): Float {
        return java.lang.Float.intBitsToFloat(readInt())
    }

    /**
     * Bytes
     * for this operation are read from the contained
     * input stream.
     *
     * @return the next eight bytes of this input stream, interpreted as a
     * `double`.
     * @throws EOFException if this input stream reaches the end before
     * reading eight bytes.
     * @throws IOException  the stream has been closed and the contained
     * input stream does not support reading after close, or
     * another I/O error occurs.
     * @see java.lang.Double.longBitsToDouble
     */
    @Throws(IOException::class)
    fun readDouble(): Double {
        return java.lang.Double.longBitsToDouble(readLong())
    }
}
