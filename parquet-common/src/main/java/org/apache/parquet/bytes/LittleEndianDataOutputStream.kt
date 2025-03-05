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

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.OutputStream

/**
 * Based on DataOutputStream but in little endian and without the String/char methods
 *
 * Creates a new data output stream to write data to the specified
 * underlying output stream. The counter `written` is
 * set to zero.
 *
 * @param out the underlying output stream, to be saved for later use.
 * @see java.io.FilterOutputStream.out
 */
class LittleEndianDataOutputStream(
    private val out: OutputStream,
) : OutputStream() {
    private val writeBuffer = ByteArray(8)

    /**
     * Writes the specified byte (the low eight bits of the argument
     * `b`) to the underlying output stream. If no exception
     * is thrown, the counter `written` is incremented by `1`.
     *
     * Implements the `write` method of `OutputStream`.
     *
     * @param b the `byte` to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     */
    @Throws(IOException::class)
    override fun write(b: Int) {
        out.write(b)
    }

    /**
     * Writes `len` bytes from the specified byte array
     * starting at offset `off` to the underlying output stream.
     * If no exception is thrown, the counter `written` is
     * incremented by `len`.
     *
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     */
    @Throws(IOException::class)
    override fun write(b: ByteArray, off: Int, len: Int) {
        out.write(b, off, len)
    }

    /**
     * Flushes this data output stream. This forces any buffered output
     * bytes to be written out to the stream.
     *
     * The `flush` method of `DataOutputStream`
     * calls the `flush` method of its underlying output stream.
     *
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     *
     * @see OutputStream.flush
     */
    @Throws(IOException::class)
    override fun flush() {
        out.flush()
    }

    /**
     * Writes a `boolean` to the underlying output stream as
     * a 1-byte value. The value `true` is written out as the
     * value `(byte)1`; the value `false` is
     * written out as the value `(byte)0`. If no exception is
     * thrown, the counter `written` is incremented by `1`.
     *
     * @param v a `boolean` value to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     */
    @Throws(IOException::class)
    fun writeBoolean(v: Boolean) {
        out.write(if (v) 1 else 0)
    }

    /**
     * Writes out a `byte` to the underlying output stream as
     * a 1-byte value. If no exception is thrown, the counter
     * `written` is incremented by `1`.
     *
     * @param v a `byte` value to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     */
    @Throws(IOException::class)
    fun writeByte(v: Int) {
        out.write(v)
    }

    /**
     * Writes a `short` to the underlying output stream as two
     * bytes, low byte first. If no exception is thrown, the counter
     * `written` is incremented by `2`.
     *
     * @param v a `short` to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     */
    @Throws(IOException::class)
    fun writeShort(v: Int) {
        out.write((v ushr 0) and 0xFF)
        out.write((v ushr 8) and 0xFF)
    }

    /**
     * Writes an `int` to the underlying output stream as four
     * bytes, low byte first. If no exception is thrown, the counter
     * `written` is incremented by `4`.
     *
     * @param v an `int` to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     */
    @Throws(IOException::class)
    fun writeInt(v: Int) {
        // TODO: see note in LittleEndianDataInputStream: maybe faster
        // to use Integer.reverseBytes() and then writeInt, or a ByteBuffer
        // approach
        out.write((v ushr 0) and 0xFF)
        out.write((v ushr 8) and 0xFF)
        out.write((v ushr 16) and 0xFF)
        out.write((v ushr 24) and 0xFF)
    }

    /**
     * Writes a `long` to the underlying output stream as eight
     * bytes, low byte first. In no exception is thrown, the counter
     * `written` is incremented by `8`.
     *
     * @param v a `long` to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     */
    @Throws(IOException::class)
    fun writeLong(v: Long) {
        writeBuffer[7] = (v ushr 56).toByte()
        writeBuffer[6] = (v ushr 48).toByte()
        writeBuffer[5] = (v ushr 40).toByte()
        writeBuffer[4] = (v ushr 32).toByte()
        writeBuffer[3] = (v ushr 24).toByte()
        writeBuffer[2] = (v ushr 16).toByte()
        writeBuffer[1] = (v ushr 8).toByte()
        writeBuffer[0] = (v ushr 0).toByte()
        out.write(writeBuffer, 0, 8)
    }

    /**
     * Converts the float argument to an `int` using the
     * `floatToIntBits` method in class `Float`,
     * and then writes that `int` value to the underlying
     * output stream as a 4-byte quantity, low byte first. If no
     * exception is thrown, the counter `written` is
     * incremented by `4`.
     *
     * @param v a `float` value to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     *
     * @see java.lang.Float.floatToIntBits
     */
    @Throws(IOException::class)
    fun writeFloat(v: Float) {
        writeInt(java.lang.Float.floatToIntBits(v))
    }

    /**
     * Converts the double argument to a `long` using the
     * `doubleToLongBits` method in class `Double`,
     * and then writes that `long` value to the underlying
     * output stream as an 8-byte quantity, low byte first. If no
     * exception is thrown, the counter `written` is
     * incremented by `8`.
     *
     * @param v a `double` value to be written.
     * @throws IOException if an I/O error occurs.
     * @see java.io.FilterOutputStream.out
     *
     * @see java.lang.Double.doubleToLongBits
     */
    @Throws(IOException::class)
    fun writeDouble(v: Double) {
        writeLong(java.lang.Double.doubleToLongBits(v))
    }

    override fun close() {
        try {
            this.out.use { os ->
                os.flush()
            }
        } catch (e: Exception) {
            if (LOG.isDebugEnabled) LOG.debug("Exception in flushing arrayOut before close", e)
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(LittleEndianDataOutputStream::class.java)
    }
}
