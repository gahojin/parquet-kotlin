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
package org.apache.parquet.column.values.bitpacking

import okio.IOException
import org.apache.parquet.bytes.BytesUtils
import java.io.InputStream
import java.io.OutputStream

/**
 * provides the correct implementation of a bitpacking based on the width in bits
 */
object BitPacking {
    /**
     * to writes ints to a stream packed to only the needed bits.
     * there is no guarantee of corecteness if ints larger than the max size are written
     */
    interface BitPackingWriter {
        /**
         * will write the bits to the underlying stream aligned on the buffer size
         *
         * @param value the value to encode
         * @throws IOException if there is an exception while writing
         */
        @Throws(IOException::class)
        fun write(value: Int)

        /**
         * will flush the buffer to the underlying stream (and pad with 0s)
         *
         * @throws IOException if there is an exception while finishing
         */
        @Throws(IOException::class)
        fun finish()
    }

    /**
     * to read back what has been written with the corresponding  writer
     */
    interface BitPackingReader {
        /**
         * @return and int decoded from the underlying stream
         * @throws IOException if there is an exception while reading
         */
        @Throws(IOException::class)
        fun read(): Int
    }

    /**
     * @param bitLength the width in bits of the integers to write
     * @param out the stream to write the bytes to
     * @return the correct implementation for the width
     */
    @JvmStatic
    fun getBitPackingWriter(bitLength: Int, out: OutputStream): BitPackingWriter {
        return when (bitLength) {
            0 -> ZeroBitPackingWriter
            1 -> OneBitPackingWriter(out)
            2 -> TwoBitPackingWriter(out)
            3 -> ThreeBitPackingWriter(out)
            4 -> FourBitPackingWriter(out)
            5 -> FiveBitPackingWriter(out)
            6 -> SixBitPackingWriter(out)
            7 -> SevenBitPackingWriter(out)
            8 -> EightBitPackingWriter(out)
            else -> throw UnsupportedOperationException("only support up to 8 for now")
        }
    }

    /**
     * @param bitLength the width in bits of the integers to read
     * @param input the stream to read the bytes from
     * @param valueCount not sure
     * @return the correct implementation for the width
     */
    @JvmStatic
    fun createBitPackingReader(bitLength: Int, input: InputStream, valueCount: Long): BitPackingReader {
        return when (bitLength) {
            0 -> ZeroBitPackingReader
            1 -> OneBitPackingReader(input)
            2 -> TwoBitPackingReader(input)
            3 -> ThreeBitPackingReader(input, valueCount)
            4 -> FourBitPackingReader(input)
            5 -> FiveBitPackingReader(input, valueCount)
            6 -> SixBitPackingReader(input, valueCount)
            7 -> SevenBitPackingReader(input, valueCount)
            8 -> EightBitPackingReader(input)
            else -> throw UnsupportedOperationException("only support up to 8 for now")
        }
    }
}

abstract class BaseBitPackingWriter : BitPacking.BitPackingWriter {
    fun finish(numberOfBits: Int, buffer: Int, out: OutputStream) {
        finish(numberOfBits, buffer.toLong(), out)
    }

    fun finish(numberOfBits: Int, buffer: Long, out: OutputStream) {
        val padding = if (numberOfBits % 8 == 0) 0 else 8 - (numberOfBits % 8)
        val tmp = buffer shl padding
        val numberOfBytes = (numberOfBits + padding) / 8
        var i = (numberOfBytes - 1) * 8
        while (i >= 0) {
            out.write(((tmp ushr i) and 0xFF).toInt())
            i -= 8
        }
    }
}

abstract class BaseBitPackingReader : BitPacking.BitPackingReader {
    fun alignToBytes(bitsCount: Int): Int {
        return BytesUtils.paddedByteCountFromBits(bitsCount)
    }
}

object ZeroBitPackingWriter : BitPacking.BitPackingWriter {
    override fun write(value: Int) = Unit
    override fun finish() = Unit
}

object ZeroBitPackingReader : BitPacking.BitPackingReader {
    override fun read() = 0
}

class OneBitPackingWriter(private val out: OutputStream) : BitPacking.BitPackingWriter {
    private var buffer = 0
    private var count = 0

    override fun write(value: Int) {
        buffer = buffer shl 1
        buffer = buffer or value
        if (++count == 8) {
            out.write(buffer)
            buffer = 0
            count = 0
        }
    }
    override fun finish() {
        while (count != 0) {
            write(0)
        }
    }
}

class OneBitPackingReader(private val input: InputStream) : BitPacking.BitPackingReader {
    private var buffer = 0
    private var count = 0

    override fun read(): Int {
        if (count == 0) {
            buffer = input.read()
            count = 8
        }
        return buffer ushr (--count) and 1
    }
}

class TwoBitPackingWriter(private val out: OutputStream) : BitPacking.BitPackingWriter {
    private var buffer = 0
    private var count = 0

    override fun write(value: Int) {
        buffer = buffer shl 2
        buffer = buffer or value
        if (++count == 4) {
            out.write(buffer)
            buffer = 0
            count = 0
        }
    }
    override fun finish() {
        while (count != 0) {
            write(0)
        }
    }
}

class TwoBitPackingReader(private val input: InputStream) : BitPacking.BitPackingReader {
    private var buffer = 0
    private var count = 0

    override fun read(): Int {
        if (count == 0) {
            buffer = input.read()
            count = 4
        }
        return buffer ushr (--count shl 1) and 3
    }
}

class ThreeBitPackingWriter(private val out: OutputStream) : BaseBitPackingWriter() {
    private var buffer = 0
    private var count = 0

    override fun write(value: Int) {
        buffer = buffer shl 3
        buffer = buffer or value
        if (++count == 8) {
            out.write(buffer ushr 16)
            out.write(buffer ushr 8)
            out.write(buffer)
            buffer = 0
            count = 0
        }
    }
    override fun finish() {
        while (count != 0) {
            val numberOfBits = count * 3
            finish(numberOfBits, buffer, out)
            buffer = 0
            count = 0
        }
    }
}

class ThreeBitPackingReader(
    private val input: InputStream,
    private val valueCount: Long,
) : BaseBitPackingReader() {
    private var buffer = 0
    private var count = 0
    private var totalRead = 0L

    override fun read(): Int {
        if (count == 0) {
            if (valueCount - totalRead < 8) {
                buffer = 0
                val bitsToRead = 3 * (valueCount - totalRead)
                val bytesToRead = alignToBytes(bitsToRead.toInt())
                for (i in 3 - 1 downTo 3 - bytesToRead) {
                    buffer = buffer or (input.read() shl (i shl 3))
                }
                count = 8
                totalRead = valueCount
            } else {
                buffer = (input.read() shl 16) or (input.read() shl 8) or input.read()
                count = 8
                totalRead += 8
            }
        }
        return buffer ushr (--count * 3) and 7
    }
}

class FourBitPackingWriter(private val out: OutputStream) : BitPacking.BitPackingWriter {
    private var buffer = 0
    private var count = 0

    override fun write(value: Int) {
        buffer = buffer shl 4
        buffer = buffer or value
        if (++count == 2) {
            out.write(buffer)
            buffer = 0
            count = 0
        }
    }
    override fun finish() {
        while (count != 0) {
            // downside: this aligns on whatever the buffer size is.
            write(0)
        }
    }
}

class FourBitPackingReader(private val input: InputStream) : BitPacking.BitPackingReader {
    private var buffer = 0
    private var count = 0

    override fun read(): Int {
        if (count == 0) {
            buffer = input.read()
            count = 2
        }
        return buffer ushr (--count shl 2) and 15
    }
}

class FiveBitPackingWriter(private val out: OutputStream) : BaseBitPackingWriter() {
    private var buffer = 0L
    private var count = 0

    override fun write(value: Int) {
        buffer = buffer shl 5
        buffer = buffer or value.toLong()
        if (++count == 8) {
            out.write(buffer ushr 32)
            out.write(buffer ushr 24)
            out.write(buffer ushr 16)
            out.write(buffer ushr 8)
            out.write(buffer)
            buffer = 0
            count = 0
        }
    }
    override fun finish() {
        while (count != 0) {
            val numberOfBits = count * 5
            finish(numberOfBits, buffer, out)
            buffer = 0
            count = 0
        }
    }
}

class FiveBitPackingReader(
    private val input: InputStream,
    private val valueCount: Long,
) : BaseBitPackingReader() {
    private var buffer = 0L
    private var count = 0
    private var totalRead = 0L

    override fun read(): Int {
        if (count == 0) {
            if (valueCount - totalRead < 8) {
                buffer = 0
                val bitsToRead = 5 * (valueCount - totalRead)
                val bytesToRead = alignToBytes(bitsToRead.toInt())
                for (i in 5 - 1 downTo 5 - bytesToRead) {
                    buffer = buffer or (input.readLong() shl (i shl 3))
                }
                count = 8
                totalRead = valueCount
            } else {
                buffer = (input.readLong() shl 32) or
                        (input.readLong() shl 24) or
                        (input.readLong() shl 16) or
                        (input.readLong() shl 8) or
                        input.readLong()
                count = 8
                totalRead += 8
            }
        }
        return (buffer ushr (--count * 5) and 31).toInt()
    }
}

class SixBitPackingWriter(private val out: OutputStream) : BaseBitPackingWriter() {
    private var buffer = 0
    private var count = 0

    override fun write(value: Int) {
        buffer = buffer shl 6
        buffer = buffer or value
        ++count
        if (count == 4) {
            out.write(buffer ushr 16)
            out.write(buffer ushr 8)
            out.write(buffer)
            buffer = 0
            count = 0
        }
    }
    override fun finish() {
        while (count != 0) {
            val numberOfBits = count * 6
            finish(numberOfBits, buffer, out)
            buffer = 0
            count = 0
        }
    }
}

class SixBitPackingReader(
    private val input: InputStream,
    private val valueCount: Long,
) : BaseBitPackingReader() {
    private var buffer = 0
    private var count = 0
    private var totalRead = 0L

    override fun read(): Int {
        if (count == 0) {
            if (valueCount - totalRead < 4) {
                buffer = 0
                val bitsToRead = 6 * (valueCount - totalRead)
                val bytesToRead = alignToBytes(bitsToRead.toInt())
                for (i in 3 - 1 downTo 3 - bytesToRead) {
                    buffer = buffer or (input.read() shl (i shl 3))
                }
                count = 4
                totalRead = valueCount
            } else {
                buffer = (input.read() shl 16) or (input.read() shl 8) or input.read()
                count = 4
                totalRead += 4
            }
        }
        return buffer ushr (--count * 6) and 63
    }
}

class SevenBitPackingWriter(private val out: OutputStream) : BaseBitPackingWriter() {
    private var buffer = 0L
    private var count = 0

    override fun write(value: Int) {
        buffer = buffer shl 7
        buffer = buffer or value.toLong()
        if (++count == 8) {
            out.write(buffer ushr 48)
            out.write(buffer ushr 40)
            out.write(buffer ushr 32)
            out.write(buffer ushr 24)
            out.write(buffer ushr 16)
            out.write(buffer ushr 8)
            out.write(buffer)
            buffer = 0
            count = 0
        }
    }
    override fun finish() {
        while (count != 0) {
            val numberOfBits = count * 7
            finish(numberOfBits, buffer, out)
            buffer = 0
            count = 0
        }
    }
}

class SevenBitPackingReader(
    private val input: InputStream,
    private val valueCount: Long,
) : BaseBitPackingReader() {
    private var buffer = 0L
    private var count = 0
    private var totalRead = 0L

    override fun read(): Int {
        if (count == 0) {
            if (valueCount - totalRead < 8) {
                buffer = 0
                val bitsToRead = 7 * (valueCount - totalRead)
                val bytesToRead = alignToBytes(bitsToRead.toInt())
                for (i in 7 - 1 downTo 7 - bytesToRead) {
                    buffer = buffer or (input.readLong() shl (i shl 3))
                }
                count = 8
                totalRead = valueCount
            } else {
                buffer = (input.readLong() shl 48) or
                        (input.readLong() shl 40) or
                        (input.readLong() shl 32) or
                        (input.readLong() shl 24) or
                        (input.readLong() shl 16) or
                        (input.readLong() shl 8) or
                        input.readLong()
                count = 8
                totalRead += 8
            }
        }
        return (buffer ushr (--count * 7) and 127).toInt()
    }
}

class EightBitPackingWriter(private val out: OutputStream) : BitPacking.BitPackingWriter {
    override fun write(value: Int) {
        out.write(value)
    }
    override fun finish() = Unit
}

class EightBitPackingReader(private val input: InputStream) : BitPacking.BitPackingReader {
    override fun read(): Int {
        return input.read()
    }
}

@Suppress("NOTHING_TO_INLINE")
private inline fun InputStream.readLong(): Long = (read() and 0xff).toLong()

@Suppress("NOTHING_TO_INLINE")
private inline fun OutputStream.write(b: Long) = write((b and 0xff).toInt())
