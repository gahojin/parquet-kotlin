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
package org.apache.parquet.column.values.rle

import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.Companion.from
import org.apache.parquet.bytes.BytesUtils.writeIntLittleEndianPaddedOnBitWidth
import org.apache.parquet.bytes.BytesUtils.writeUnsignedVarInt
import org.apache.parquet.bytes.CapacityByteArrayOutputStream
import org.apache.parquet.column.values.bitpacking.BytePacker
import org.apache.parquet.column.values.bitpacking.Packer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException

/**
 * Encodes values using a combination of run length encoding and bit packing,
 * according to the following grammar:
 *
 * ```
 * rle-bit-packed-hybrid: <length> <encoded-data>
 * length := length of the <encoded-data> in bytes stored as 4 bytes little endian
 * encoded-data := <run>*
 * run := <bit-packed-run> | <rle-run>
 * bit-packed-run := <bit-packed-header> <bit-packed-values>
 * bit-packed-header := varint-encode(<bit-pack-count> << 1 | 1)
 * // we always bit-pack a multiple of 8 values at a time, so we only store the number of values / 8
 * bit-pack-count := (number of values in this run) / 8
 * bit-packed-values :=  bit packed back to back, from LSB to MSB
 * rle-run := <rle-header> <repeated-value>
 * rle-header := varint-encode( (number of times repeated) << 1)
 * repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
 * ```
 *
 * NOTE: this class is only responsible for creating and returning the `<encoded-data>`
 * portion of the above grammar. The `<length>` portion is done by
 * [RunLengthBitPackingHybridValuesWriter]
 *
 * Only supports positive values (including 0) // TODO: is that ok? Should we make a signed version?
 *
 * @property bitWidth The bit width used for bit-packing and for writing the repeated-value
 */
class RunLengthBitPackingHybridEncoder(
    private val bitWidth: Int,
    initialCapacity: Int,
    pageSize: Int,
    allocator: ByteBufferAllocator,
) : AutoCloseable {
    private val packer: BytePacker

    private val baos = CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator)

    /**
     * Values that are bit packed 8 at at a time are packed into this
     * buffer, which is then written to baos
     */
    private val packBuffer: ByteArray

    /**
     * Previous value written, used to detect repeated values
     */
    private var previousValue = 0

    /**
     * We buffer 8 values at a time, and either bit pack them
     * or discard them after writing a rle-run
     */
    private val bufferedValues = IntArray(8)

    private var numBufferedValues = 0

    /**
     * How many times a value has been repeated
     */
    private var repeatCount = 0

    /**
     * How many groups of 8 values have been written
     * to the current bit-packed-run
     */
    private var bitPackedGroupCount = 0

    /**
     * A "pointer" to a single byte in baos,
     * which we use as our bit-packed-header. It's really
     * the logical index of the byte in baos.
     *
     *
     * We are only using one byte for this header,
     * which limits us to writing 504 values per bit-packed-run.
     *
     *
     * MSB must be 0 for varint encoding, LSB must be 1 to signify
     * that this is a bit-packed-header leaves 6 bits to write the
     * number of 8-groups -> (2^6 - 1) * 8 = 504
     */
    private var bitPackedRunHeaderPointer: Long = 0

    private var toBytesCalled = false


    val bufferedSize: Long
        get() = baos.size()

    val allocatedSize: Long
        get() = baos.capacity.toLong()

    init {
        LOG.debug(
            "Encoding: RunLengthBitPackingHybridEncoder with " + "bithWidth: {} initialCapacity {}",
            bitWidth,
            initialCapacity,
        )

        require(bitWidth >= 0 && bitWidth <= 32) { "bitWidth must be >= 0 and <= 32" }

        packBuffer = ByteArray(bitWidth)
        packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth)
        reset(false)
    }

    private fun reset(resetBaos: Boolean) {
        if (resetBaos) {
            baos.reset()
        }
        previousValue = 0
        numBufferedValues = 0
        repeatCount = 0
        bitPackedGroupCount = 0
        bitPackedRunHeaderPointer = -1
        toBytesCalled = false
    }

    @Throws(IOException::class)
    fun writeInt(value: Int) {
        if (value == previousValue) {
            // keep track of how many times we've seen this value
            // consecutively
            ++repeatCount

            if (repeatCount >= 8) {
                // we've seen this at least 8 times, we're
                // certainly going to write an rle-run,
                // so just keep on counting repeats for now
                return
            }
        } else {
            // This is a new value, check if it signals the end of an rle-run
            if (repeatCount >= 8) {
                // it does! write an rle-run
                writeRleRun()
            }

            // this is a new value so we've only seen it once
            repeatCount = 1
            // start tracking this value for repeats
            previousValue = value
        }

        // We have not seen enough repeats to justify an rle-run yet,
        // so buffer this value in case we decide to write a bit-packed-run
        bufferedValues[numBufferedValues] = value
        ++numBufferedValues

        if (numBufferedValues == 8) {
            // we've encountered less than 8 repeated values, so
            // either start a new bit-packed-run or append to the
            // current bit-packed-run
            writeOrAppendBitPackedRun()
        }
    }

    @Throws(IOException::class)
    private fun writeOrAppendBitPackedRun() {
        if (bitPackedGroupCount >= 63) {
            // we've packed as many values as we can for this run,
            // end it and start a new one
            endPreviousBitPackedRun()
        }

        if (bitPackedRunHeaderPointer == -1L) {
            // this is a new bit-packed-run, allocate a byte for the header
            // and keep a "pointer" to it so that it can be mutated later
            baos.write(0) // write a sentinel value
            bitPackedRunHeaderPointer = baos.currentIndex
        }

        packer.pack8Values(bufferedValues, 0, packBuffer, 0)
        baos.write(packBuffer)

        // empty the buffer, they've all been written
        numBufferedValues = 0

        // clear the repeat count, as some repeated values
        // may have just been bit packed into this run
        repeatCount = 0

        ++bitPackedGroupCount
    }

    /**
     * If we are currently writing a bit-packed-run, update the
     * bit-packed-header and consider this run to be over
     *
     * does nothing if we're not currently writing a bit-packed run
     */
    private fun endPreviousBitPackedRun() {
        if (bitPackedRunHeaderPointer == -1L) {
            // we're not currently in a bit-packed-run
            return
        }

        // create bit-packed-header, which needs to fit in 1 byte
        val bitPackHeader = ((bitPackedGroupCount shl 1) or 1).toByte()

        // update this byte
        baos.setByte(bitPackedRunHeaderPointer, bitPackHeader)

        // mark that this run is over
        bitPackedRunHeaderPointer = -1

        // reset the number of groups
        bitPackedGroupCount = 0
    }

    @Throws(IOException::class)
    private fun writeRleRun() {
        // we may have been working on a bit-packed-run
        // so close that run if it exists before writing this
        // rle-run
        endPreviousBitPackedRun()

        // write the rle-header (lsb of 0 signifies a rle run)
        writeUnsignedVarInt(repeatCount shl 1, baos)
        // write the repeated-value
        writeIntLittleEndianPaddedOnBitWidth(baos, previousValue, bitWidth)

        // reset the repeat count
        repeatCount = 0

        // throw away all the buffered values, they were just repeats and they've been written
        numBufferedValues = 0
    }

    @Throws(IOException::class)
    fun toBytes(): BytesInput {
        require(!toBytesCalled) { "You cannot call toBytes() more than once without calling reset()" }

        // write anything that is buffered / queued up for an rle-run
        if (repeatCount >= 8) {
            writeRleRun()
        } else if (numBufferedValues > 0) {
            for (i in numBufferedValues..7) {
                bufferedValues[i] = 0
            }
            writeOrAppendBitPackedRun()
            endPreviousBitPackedRun()
        } else {
            endPreviousBitPackedRun()
        }

        toBytesCalled = true
        return from(baos)
    }

    /**
     * Reset this encoder for re-use
     */
    fun reset() {
        reset(true)
    }

    override fun close() {
        reset(false)
        baos.close()
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(RunLengthBitPackingHybridEncoder::class.java)
    }
}
