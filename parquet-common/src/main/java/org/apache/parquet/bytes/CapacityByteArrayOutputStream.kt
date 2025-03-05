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

import org.apache.parquet.OutputStreamCloseException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import kotlin.math.pow

/**
 * Similar to a [java.io.ByteArrayOutputStream], but uses a different strategy for growing that does not involve copying.
 * Where ByteArrayOutputStream is backed by a single array that "grows" by copying into a new larger array, this output
 * stream grows by allocating a new array (slab) and adding it to a list of previous arrays.
 *
 * Each new slab is allocated to be the same size as all the previous slabs combined, so these allocations become
 * exponentially less frequent, just like ByteArrayOutputStream, with one difference. This output stream accepts a
 * max capacity hint, which is a hint describing the max amount of data that will be written to this stream. As the
 * total size of this stream nears this max, this stream starts to grow linearly instead of exponentially.
 * So new slabs are allocated to be 1/5th of the max capacity hint,
 * instead of equal to the total previous size of all slabs. This is useful because it prevents allocating roughly
 * twice the needed space when a new slab is added just before the stream is done being used.
 *
 * When reusing this stream it will adjust the initial slab size based on the previous data size, aiming for fewer
 * allocations, with the assumption that a similar amount of data will be written to this stream on re-use.
 * See ([CapacityByteArrayOutputStream.reset]).
 *
 * @property initialSlabSize the size to make the first slab
 * @property maxCapacityHint a hint (not guarantee) of the max amount of data written to this stream
 * @property allocator       an allocator to use when creating byte buffers for slabs
 */
class CapacityByteArrayOutputStream(
    private var initialSlabSize: Int,
    private val maxCapacityHint: Int,
    private val allocator: ByteBufferAllocator,
) : OutputStream() {
    private val slabs = ArrayList<ByteBuffer>()
    private var currentSlab: ByteBuffer = EMPTY_SLAB

    /** The total size in bytes currently allocated for this stream */
    var capacity: Int = 0
        private set
    private var bytesUsed = 0

    /**
     * Defaults maxCapacityHint to 1MB
     *
     * @param initialSlabSize an initial slab size
     */
    @Deprecated("use {@link CapacityByteArrayOutputStream#CapacityByteArrayOutputStream(int, int, ByteBufferAllocator)}")
    constructor(initialSlabSize: Int) : this(initialSlabSize, 1024 * 1024, HeapByteBufferAllocator.INSTANCE)

    /**
     * Defaults maxCapacityHint to 1MB
     *
     * @param initialSlabSize an initial slab size
     * @param allocator       an allocator to use when creating byte buffers for slabs
     */
    @Deprecated("use {@link CapacityByteArrayOutputStream#CapacityByteArrayOutputStream(int, int, ByteBufferAllocator)}")
    constructor(initialSlabSize: Int, allocator: ByteBufferAllocator) : this(initialSlabSize, 1024 * 1024, allocator)

    /**
     * @param initialSlabSize the size to make the first slab
     * @param maxCapacityHint a hint (not guarantee) of the max amount of data written to this stream
     */
    @Deprecated("use {@link CapacityByteArrayOutputStream#CapacityByteArrayOutputStream(int, int, ByteBufferAllocator)}")
    constructor(initialSlabSize: Int, maxCapacityHint: Int) : this(initialSlabSize, maxCapacityHint, HeapByteBufferAllocator())

    init {
        require(initialSlabSize > 0) { "initialSlabSize must be > 0" }
        require(maxCapacityHint > 0) { "maxCapacityHint must be > 0" }
        require(maxCapacityHint >= initialSlabSize) {
            "maxCapacityHint can't be less than initialSlabSize $initialSlabSize $maxCapacityHint"
        }
        reset()
    }

    /**
     * the new slab is guaranteed to be at least minimumSize
     *
     * @param minimumSize the size of the data we want to copy in the new slab
     */
    private fun addSlab(minimumSize: Int) {
        var nextSlabSize: Int

        // check for overflow
        try {
            Math.addExact(bytesUsed, minimumSize)
        } catch (e: ArithmeticException) {
            // This is interpreted as a request for a value greater than Integer.MAX_VALUE
            // We throw OOM because that is what java.io.ByteArrayOutputStream also does
            throw OutOfMemoryError("Size of data exceeded Integer.MAX_VALUE (" + e.message + ")")
        }

        nextSlabSize = if (bytesUsed == 0) {
            initialSlabSize
        } else if (bytesUsed > maxCapacityHint / 5) {
            // to avoid an overhead of up to twice the needed size, we get linear when approaching target page size
            maxCapacityHint / 5
        } else {
            // double the size every time
            bytesUsed
        }

        if (nextSlabSize < minimumSize) {
            LOG.debug("slab size {} too small for value of size {}. Bumping up slab size", nextSlabSize, minimumSize)
            nextSlabSize = minimumSize
        }

        LOG.debug("used {} slabs, adding new slab of size {}", slabs.size, nextSlabSize)

        currentSlab = allocator.allocate(nextSlabSize).also {
            slabs.add(it)
        }
        capacity = Math.addExact(capacity, nextSlabSize)
    }

    override fun write(b: Int) {
        if (!currentSlab.hasRemaining()) {
            addSlab(1)
        }
        currentSlab.put(b.toByte())
        bytesUsed = Math.addExact(bytesUsed, 1)
    }

    override fun write(b: ByteArray, off: Int, len: Int) {
        if ((off < 0) || (off > b.size) || (len < 0) || ((off + len) - b.size > 0)) {
            throw IndexOutOfBoundsException(
                String.format(
                    "Given byte array of size %d, with requested length(%d) and offset(%d)", b.size, len, off
                )
            )
        }
        if (len > currentSlab.remaining()) {
            val length1 = currentSlab.remaining()
            currentSlab.put(b, off, length1)
            val length2 = len - length1
            addSlab(length2)
            currentSlab.put(b, off + length1, length2)
        } else {
            currentSlab.put(b, off, len)
        }
        bytesUsed = Math.addExact(bytesUsed, len)
    }

    @Throws(IOException::class)
    private fun writeToOutput(out: OutputStream, buf: ByteBuffer, len: Int) {
        if (buf.hasArray()) {
            out.write(buf.array(), buf.arrayOffset(), len)
        } else {
            // The OutputStream interface only takes a byte[], unfortunately this means that a ByteBuffer
            // not backed by a byte array must be copied to fulfil this interface
            val copy = ByteArray(len)
            buf.flip()
            buf[copy]
            out.write(copy)
        }
    }

    /**
     * Writes the complete contents of this buffer to the specified output stream argument. the output
     * stream's write method `out.write(slab, 0, slab.length)`) will be called once per slab.
     *
     * @param out the output stream to which to write the data.
     * @throws IOException if an I/O error occurs.
     */
    @Throws(IOException::class)
    fun writeTo(out: OutputStream) {
        for (slab in slabs) {
            writeToOutput(out, slab, slab.position())
        }
    }

    /**
     * It is expected that the buffer is large enough to fit the content of this.
     */
    fun writeInto(buffer: ByteBuffer) {
        for (slab in slabs) {
            slab.flip()
            buffer.put(slab)
        }
    }

    /**
     * @return The total size in bytes of data written to this stream.
     */
    fun size(): Long {
        return bytesUsed.toLong()
    }

    /**
     * When re-using an instance with reset, it will adjust slab size based on previous data size.
     * The intent is to reuse the same instance for the same type of data (for example, the same column).
     * The assumption is that the size in the buffer will be consistent.
     */
    fun reset() {
        // readjust slab size.
        // 7 = 2^3 - 1 so that doubling the initial size 3 times will get to the same size
        initialSlabSize = maxOf(bytesUsed / 7, initialSlabSize)
        LOG.debug("initial slab of size {}", initialSlabSize)
        for (slab in slabs) {
            allocator.release(slab)
        }
        slabs.clear()
        capacity = 0
        bytesUsed = 0
        currentSlab = EMPTY_SLAB
    }

    /** the index of the last value written to this stream, which can be passed to [.setByte] in order to change it */
    val currentIndex: Long
        get() {
            require(bytesUsed > 0) { "This is an empty stream" }
            return (bytesUsed - 1).toLong()
        }

    /**
     * Replace the byte stored at position index in this stream with value
     *
     * @param index which byte to replace
     * @param value the value to replace it with
     */
    fun setByte(index: Long, value: Byte) {
        require(index < bytesUsed) { "Index: $index is >= the current size of: $bytesUsed" }

        var seen = 0L
        for (i in slabs.indices) {
            val slab = slabs[i]
            if (index < seen + slab.limit()) {
                // ok found index
                slab.put((index - seen).toInt(), value)
                break
            }
            seen += slab.limit().toLong()
        }
    }

    /**
     * @param prefix a prefix to be used for every new line in the string
     * @return a text representation of the memory usage of this structure
     */
    fun memUsageString(prefix: String): String {
        return "%s %s %d slabs, %,d bytes".format(prefix, javaClass.simpleName, slabs.size, capacity)
    }

    /** the total number of allocated slabs */
    val slabCount: Int
        get() = slabs.size

    val internalByteBuffer: ByteBuffer?
        get() {
            if (slabs.size == 1) {
                val buf = slabs[0].duplicate()
                buf.flip()
                return buf.slice()
            }
            return null
        }

    override fun close() {
        for (slab in slabs) {
            allocator.release(slab)
        }
        slabs.clear()
        try {
            super.close()
        } catch (e: IOException) {
            throw OutputStreamCloseException(e)
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(CapacityByteArrayOutputStream::class.java)
        private val EMPTY_SLAB: ByteBuffer = ByteBuffer.wrap(ByteArray(0))

        /**
         * Return an initial slab size such that a CapacityByteArrayOutputStream constructed with it
         * will end up allocating targetNumSlabs in order to reach targetCapacity. This aims to be
         * a balance between the overhead of creating new slabs and wasting memory by eagerly making
         * initial slabs too big.
         *
         * Note that targetCapacity here need not match maxCapacityHint in the constructor of
         * CapacityByteArrayOutputStream, though often that would make sense.
         *
         * @param minSlabSize    no matter what we shouldn't make slabs any smaller than this
         * @param targetCapacity after we've allocated targetNumSlabs how much capacity should we have?
         * @param targetNumSlabs how many slabs should it take to reach targetCapacity?
         * @return an initial slab size
         */
        @JvmStatic
        fun initialSlabSizeHeuristic(minSlabSize: Int, targetCapacity: Int, targetNumSlabs: Int): Int {
            // initialSlabSize = (targetCapacity / (2^targetNumSlabs)) means we double targetNumSlabs times
            // before reaching the targetCapacity
            // eg for page size of 1MB we start at 1024 bytes.
            // we also don't want to start too small, so we also apply a minimum.
            return maxOf(minSlabSize, (targetCapacity / 2.0.pow(targetNumSlabs)).toInt())
        }

        /**
         * Construct a CapacityByteArrayOutputStream configured such that its initial slab size is
         * determined by [.initialSlabSizeHeuristic], with targetCapacity == maxCapacityHint
         *
         * @param minSlabSize     a minimum slab size
         * @param maxCapacityHint a hint for the maximum required capacity
         * @param targetNumSlabs  the target number of slabs
         * @param allocator       an allocator to use when creating byte buffers for slabs
         * @return a capacity baos
         */
        @JvmOverloads
        fun withTargetNumSlabs(
            minSlabSize: Int,
            maxCapacityHint: Int,
            targetNumSlabs: Int,
            allocator: ByteBufferAllocator = HeapByteBufferAllocator.INSTANCE,
        ): CapacityByteArrayOutputStream {
            return CapacityByteArrayOutputStream(
                initialSlabSizeHeuristic(minSlabSize, maxCapacityHint, targetNumSlabs), maxCapacityHint, allocator
            )
        }
    }
}
