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
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer

/**
 * utility methods to deal with bytes
 */
object BytesUtils {
    private val LOG: Logger = LoggerFactory.getLogger(BytesUtils::class.java)

    /**
     * give the number of bits needed to encode an int given the max value
     *
     * @param bound max int that we want to encode
     * @return the number of bits required
     */
    @JvmStatic
    fun getWidthFromMaxInt(bound: Int): Int {
        return 32 - Integer.numberOfLeadingZeros(bound)
    }

    /**
     * reads an int in little endian at the given position
     *
     * @param in     a byte buffer
     * @param offset an offset into the byte buffer
     * @return the integer at position offset read using little endian byte order
     * @throws IOException if there is an exception reading from the byte buffer
     */
    @JvmStatic
    @Throws(IOException::class)
    fun readIntLittleEndian(`in`: ByteBuffer, offset: Int): Int {
        val ch4 = `in`[offset].toInt() and 0xff
        val ch3 = `in`[offset + 1].toInt() and 0xff
        val ch2 = `in`[offset + 2].toInt() and 0xff
        val ch1 = `in`[offset + 3].toInt() and 0xff
        return ((ch1 shl 24) + (ch2 shl 16) + (ch3 shl 8) + ch4)
    }

    /**
     * reads an int in little endian at the given position
     *
     * @param in     a byte array
     * @param offset an offset into the byte array
     * @return the integer at position offset read using little endian byte order
     * @throws IOException if there is an exception reading from the byte array
     */
    @JvmStatic
    @Throws(IOException::class)
    fun readIntLittleEndian(`in`: ByteArray, offset: Int): Int {
        val ch4 = `in`[offset].toInt() and 0xff
        val ch3 = `in`[offset + 1].toInt() and 0xff
        val ch2 = `in`[offset + 2].toInt() and 0xff
        val ch1 = `in`[offset + 3].toInt() and 0xff
        return ((ch1 shl 24) + (ch2 shl 16) + (ch3 shl 8) + ch4)
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readIntLittleEndian(`in`: InputStream): Int {
        // TODO: this is duplicated code in LittleEndianDataInputStream
        val ch1 = `in`.read()
        val ch2 = `in`.read()
        val ch3 = `in`.read()
        val ch4 = `in`.read()
        if ((ch1 or ch2 or ch3 or ch4) < 0) {
            throw EOFException()
        }
        return ((ch4 shl 24) + (ch3 shl 16) + (ch2 shl 8) + ch1)
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readIntLittleEndianOnOneByte(`in`: InputStream): Int {
        val ch1 = `in`.read()
        if (ch1 < 0) {
            throw EOFException()
        }
        return ch1
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readIntLittleEndianOnTwoBytes(`in`: InputStream): Int {
        val ch1 = `in`.read()
        val ch2 = `in`.read()
        if ((ch1 or ch2) < 0) {
            throw EOFException()
        }
        return ((ch2 shl 8) + ch1)
    }

    @Throws(IOException::class)
    fun readIntLittleEndianOnThreeBytes(`in`: InputStream): Int {
        val ch1 = `in`.read()
        val ch2 = `in`.read()
        val ch3 = `in`.read()
        if ((ch1 or ch2 or ch3) < 0) {
            throw EOFException()
        }
        return ((ch3 shl 16) + (ch2 shl 8) + ch1)
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readIntLittleEndianPaddedOnBitWidth(`in`: InputStream, bitWidth: Int): Int {
        val bytesWidth = paddedByteCountFromBits(bitWidth)
        return when (bytesWidth) {
            0 -> 0
            1 -> readIntLittleEndianOnOneByte(`in`)
            2 -> readIntLittleEndianOnTwoBytes(`in`)
            3 -> readIntLittleEndianOnThreeBytes(`in`)
            4 -> readIntLittleEndian(`in`)
            else -> throw IOException(
                String.format("Encountered bitWidth (%d) that requires more than 4 bytes", bitWidth)
            )
        }
    }

    @JvmStatic
    @Throws(IOException::class)
    fun writeIntLittleEndianOnOneByte(out: OutputStream, v: Int) {
        out.write(v and 0xFF)
    }

    @Throws(IOException::class)
    fun writeIntLittleEndianOnTwoBytes(out: OutputStream, v: Int) {
        out.write(v and 0xFF)
        out.write((v ushr 8) and 0xFF)
    }

    @Throws(IOException::class)
    fun writeIntLittleEndianOnThreeBytes(out: OutputStream, v: Int) {
        out.write(v and 0xFF)
        out.write((v ushr 8) and 0xFF)
        out.write((v ushr 16) and 0xFF)
    }

    @JvmStatic
    @Throws(IOException::class)
    fun writeIntLittleEndian(out: OutputStream, v: Int) {
        // TODO: this is duplicated code in LittleEndianDataOutputStream
        out.write(v and 0xFF)
        out.write((v ushr 8) and 0xFF)
        out.write((v ushr 16) and 0xFF)
        out.write((v ushr 24) and 0xFF)
        if (LOG.isDebugEnabled) LOG.debug(
            ("write le int: " + v + " => " + (v and 0xFF) + " " + ((v ushr 8) and 0xFF) + " "
                    + ((v ushr 16) and 0xFF) + " " + ((v ushr 24) and 0xFF))
        )
    }

    /**
     * Write a little endian int to out, using the the number of bytes required by
     * bit width
     *
     * @param out      an output stream
     * @param v        an int value
     * @param bitWidth bit width for padding
     * @throws IOException if there is an exception while writing
     */
    @JvmStatic
    @Throws(IOException::class)
    fun writeIntLittleEndianPaddedOnBitWidth(out: OutputStream, v: Int, bitWidth: Int) {
        val bytesWidth = paddedByteCountFromBits(bitWidth)
        when (bytesWidth) {
            0 -> {}
            1 -> writeIntLittleEndianOnOneByte(out, v)
            2 -> writeIntLittleEndianOnTwoBytes(out, v)
            3 -> writeIntLittleEndianOnThreeBytes(out, v)
            4 -> writeIntLittleEndian(out, v)
            else -> throw IOException(String.format("Encountered value (%d) that requires more than 4 bytes", v))
        }
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readUnsignedVarInt(`in`: InputStream): Int {
        var value = 0
        var i = 0
        var b: Int
        while (((`in`.read().also { b = it }) and 0x80) != 0) {
            value = value or ((b and 0x7F) shl i)
            i += 7
        }
        return value or (b shl i)
    }

    /**
     * uses a trick mentioned in https://developers.google.com/protocol-buffers/docs/encoding to read zigZag encoded data
     *
     * @param in an input stream
     * @return the value of a zig-zag varint read from the current position in the stream
     * @throws IOException if there is an exception while reading
     */
    @JvmStatic
    @Throws(IOException::class)
    fun readZigZagVarInt(`in`: InputStream): Int {
        val raw = readUnsignedVarInt(`in`)
        val temp = (((raw shl 31) shr 31) xor raw) shr 1
        return temp xor (raw and (1 shl 31))
    }

    @JvmStatic
    @Throws(IOException::class)
    fun writeUnsignedVarInt(value: Int, out: OutputStream) {
        var value = value
        while ((value and -0x80).toLong() != 0L) {
            out.write((value and 0x7F) or 0x80)
            value = value ushr 7
        }
        out.write(value and 0x7F)
    }

    @JvmStatic
    @Throws(IOException::class)
    fun writeUnsignedVarInt(value: Int, dest: ByteBuffer) {
        var value = value
        while ((value and -0x80).toLong() != 0L) {
            dest.put(((value and 0x7F) or 0x80).toByte())
            value = value ushr 7
        }
        dest.put((value and 0x7F).toByte())
    }

    @JvmStatic
    @Throws(IOException::class)
    fun writeZigZagVarInt(intValue: Int, out: OutputStream) {
        writeUnsignedVarInt((intValue shl 1) xor (intValue shr 31), out)
    }

    /**
     * uses a trick mentioned in https://developers.google.com/protocol-buffers/docs/encoding to read zigZag encoded data
     * TODO: the implementation is compatible with readZigZagVarInt. Is there a need for different functions?
     *
     * @param in an input stream
     * @return the value of a zig-zag var-long read from the current position in the stream
     * @throws IOException if there is an exception while reading
     */
    @JvmStatic
    @Throws(IOException::class)
    fun readZigZagVarLong(`in`: InputStream): Long {
        val raw = readUnsignedVarLong(`in`)
        val temp = (((raw shl 63) shr 63) xor raw) shr 1
        return temp xor (raw and (1L shl 63))
    }

    @JvmStatic
    @Throws(IOException::class)
    fun readUnsignedVarLong(`in`: InputStream): Long {
        var value: Long = 0
        var i = 0
        var b: Long
        while (((`in`.read().also { b = it.toLong() }) and 0x80) != 0) {
            value = value or ((b and 0x7FL) shl i)
            i += 7
        }
        return value or (b shl i)
    }

    @JvmStatic
    @Throws(IOException::class)
    fun writeUnsignedVarLong(value: Long, out: OutputStream) {
        var value = value
        while ((value and -0x80L) != 0L) {
            out.write(((value and 0x7FL) or 0x80L).toInt())
            value = value ushr 7
        }
        out.write((value and 0x7FL).toInt())
    }

    @JvmStatic
    fun writeUnsignedVarLong(value: Long, out: ByteBuffer) {
        var value = value
        while ((value and -0x80L) != 0L) {
            out.put(((value and 0x7FL) or 0x80L).toByte())
            value = value ushr 7
        }
        out.put((value and 0x7FL).toByte())
    }

    @JvmStatic
    @Throws(IOException::class)
    fun writeZigZagVarLong(longValue: Long, out: OutputStream) {
        writeUnsignedVarLong((longValue shl 1) xor (longValue shr 63), out)
    }

    /**
     * @param bitLength a count of bits
     * @return the corresponding byte count padded to the next byte
     */
    @JvmStatic
    fun paddedByteCountFromBits(bitLength: Int): Int {
        return (bitLength + 7) / 8
    }

    @JvmStatic
    fun intToBytes(value: Int): ByteArray {
        return byteArrayOf(
            value.toByte(),
            (value ushr 8).toByte(),
            (value ushr 16).toByte(),
            (value ushr 24).toByte(),
        )
    }

    @JvmStatic
    fun bytesToInt(bytes: ByteArray): Int {
        return (((bytes[3].toInt() and 255) shl 24)
                + ((bytes[2].toInt() and 255) shl 16)
                + ((bytes[1].toInt() and 255) shl 8)
                + (bytes[0].toInt() and 255))
    }

    @JvmStatic
    fun longToBytes(value: Long): ByteArray {
        return byteArrayOf(
            value.toByte(),
            (value ushr 8).toByte(),
            (value ushr 16).toByte(),
            (value ushr 24).toByte(),
            (value ushr 32).toByte(),
            (value ushr 40).toByte(),
            (value ushr 48).toByte(),
            (value ushr 56).toByte(),
        )
    }

    @JvmStatic
    fun bytesToLong(bytes: ByteArray): Long {
        return (((bytes[7].toLong() shl 56) or
                ((bytes[6].toInt() and 255).toLong() shl 48) or
                ((bytes[5].toInt() and 255).toLong() shl 40) or
                ((bytes[4].toInt() and 255).toLong() shl 32) or
                ((bytes[3].toInt() and 255).toLong() shl 24) or
                ((bytes[2].toInt() and 255).toLong() shl 16) or
                ((bytes[1].toInt() and 255).toLong() shl 8) or
                ((bytes[0].toInt() and 255).toLong())))
    }

    @JvmStatic
    fun booleanToBytes(value: Boolean): ByteArray {
        return byteArrayOf(
            if (value) 1 else 0
        )
    }

    @JvmStatic
    fun bytesToBool(bytes: ByteArray): Boolean {
        return ((bytes[0].toInt() and 255) != 0)
    }
}
