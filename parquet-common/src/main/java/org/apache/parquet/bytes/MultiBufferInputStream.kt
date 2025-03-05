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

internal class MultiBufferInputStream(buffers: List<ByteBuffer>) : ByteBufferInputStream() {
    private val length: Long = buffers.sumOf { it.remaining().toLong() }

    private var iterator: Iterator<ByteBuffer> = buffers.iterator()
    private var current: ByteBuffer? = EMPTY
    private var position: Long = 0

    private var mark: Long = -1
    private var markLimit: Long = 0
    private var markBuffers: MutableList<ByteBuffer> = ArrayList<ByteBuffer>()

    init {
        nextBuffer()
    }

    override fun position() = position

    override fun skip(n: Long): Long {
        if (n <= 0) {
            return 0
        }

        var bytesSkipped: Long = 0
        while (bytesSkipped < n) {
            val current = current ?: return -1

            if (current.remaining() > 0) {
                val bytesToSkip = minOf(n - bytesSkipped, current.remaining().toLong())
                current.position(current.position() + bytesToSkip.toInt())
                bytesSkipped += bytesToSkip
                position += bytesToSkip
            } else if (!nextBuffer()) {
                // there are no more buffers
                return if (bytesSkipped > 0) bytesSkipped else -1
            }
        }

        return bytesSkipped
    }

    override fun read(out: ByteBuffer): Int {
        val len = out.remaining()
        if (len <= 0) {
            return 0
        }

        var bytesCopied = 0
        while (bytesCopied < len) {
            val current = current ?: return -1

            if (current.remaining() > 0) {
                val bytesToCopy: Int
                val copyBuffer: ByteBuffer?
                if (current.remaining() <= out.remaining()) {
                    // copy all of the current buffer
                    bytesToCopy = current.remaining()
                    copyBuffer = current
                } else {
                    // copy a slice of the current buffer
                    bytesToCopy = out.remaining()
                    copyBuffer = current.duplicate()
                    copyBuffer.limit(copyBuffer.position() + bytesToCopy)
                    current.position(copyBuffer.position() + bytesToCopy)
                }

                out.put(copyBuffer)
                bytesCopied += bytesToCopy
                position += bytesToCopy.toLong()
            } else if (!nextBuffer()) {
                // there are no more buffers
                return if (bytesCopied > 0) bytesCopied else -1
            }
        }

        return bytesCopied
    }

    @Throws(EOFException::class)
    override fun slice(length: Int): ByteBuffer {
        if (length <= 0) {
            return EMPTY
        }

        val current = current ?: throw EOFException()

        val slice: ByteBuffer
        if (length > current.remaining()) {
            // a copy is needed to return a single buffer
            // TODO: use an allocator
            slice = ByteBuffer.allocate(length)
            val bytesCopied = read(slice)
            slice.flip()
            if (bytesCopied < length) {
                throw EOFException()
            }
        } else {
            slice = current.duplicate()
            slice.limit(slice.position() + length)
            current.position(slice.position() + length)
            position += length.toLong()
        }

        return slice
    }

    @Throws(EOFException::class)
    override fun sliceBuffers(len: Long): List<ByteBuffer> {
        if (len <= 0) {
            return emptyList()
        }

        val buffers: MutableList<ByteBuffer> = ArrayList<ByteBuffer>()
        var bytesAccumulated: Long = 0
        while (bytesAccumulated < len) {
            val current = current ?: throw EOFException()

            if (current.remaining() > 0) {
                // get a slice of the current buffer to return
                // always fits in an int because remaining returns an int that is >= 0
                val bufLen = minOf((len - bytesAccumulated).toDouble(), current.remaining().toDouble()).toInt()
                val slice = current.duplicate()
                slice.limit(slice.position() + bufLen)
                buffers.add(slice)
                bytesAccumulated += bufLen.toLong()

                // update state; the bytes are considered read
                current.position(current.position() + bufLen)
                position += bufLen.toLong()
            } else if (!nextBuffer()) {
                // there are no more buffers
                throw EOFException()
            }
        }

        return buffers
    }

    override fun remainingBuffers(): List<ByteBuffer> {
        if (position >= length) {
            return emptyList()
        }

        try {
            return sliceBuffers(length - position)
        } catch (_: EOFException) {
            throw RuntimeException("[Parquet bug] Stream is bad: incorrect bytes remaining ${length - position}")
        }
    }

    override fun read(bytes: ByteArray, off: Int, len: Int): Int {
        if (len <= 0) {
            if (len < 0) {
                throw IndexOutOfBoundsException("Read length must be greater than 0: $len")
            }
            return 0
        }

        var bytesRead = 0
        while (bytesRead < len) {
            val current = current ?: return -1

            if (current.remaining() > 0) {
                val bytesToRead = minOf((len - bytesRead).toDouble(), current.remaining().toDouble()).toInt()
                current.get(bytes, off + bytesRead, bytesToRead)
                bytesRead += bytesToRead
                position += bytesToRead.toLong()
            } else if (!nextBuffer()) {
                // there are no more buffers
                return if (bytesRead > 0) bytesRead else -1
            }
        }

        return bytesRead
    }

    override fun read(bytes: ByteArray): Int {
        return read(bytes, 0, bytes.size)
    }

    @Throws(IOException::class)
    override fun read(): Int {
        while (true) {
            val current = current ?: throw EOFException()

            if (current.remaining() > 0) {
                position++
                return current.get().toInt() and 0xFF // as unsigned
            } else if (!nextBuffer()) {
                // there are no more buffers
                throw EOFException()
            }
        }
    }

    override fun available(): Int {
        val remaining = length - position
        if (remaining > Int.Companion.MAX_VALUE) {
            return Int.Companion.MAX_VALUE
        } else {
            return remaining.toInt()
        }
    }

    override fun mark(readlimit: Int) {
        if (mark >= 0) {
            discardMark()
        }
        mark = position
        markLimit = mark + readlimit + 1
        current?.also {
            markBuffers.add(it.duplicate())
        }
    }

    @Throws(IOException::class)
    override fun reset() {
        if (mark >= 0 && position < markLimit) {
            position = mark
            // replace the current iterator with one that adds back the buffers that
            // have been used since mark was called.
            iterator = concat(markBuffers.iterator(), iterator)
            discardMark()
            nextBuffer() // go back to the marked buffers
        } else {
            throw IOException("No mark defined or has read past the previous mark limit")
        }
    }

    private fun discardMark() {
        mark = -1
        markLimit = 0
        markBuffers = ArrayList<ByteBuffer>()
    }

    override fun markSupported(): Boolean {
        return true
    }

    private fun nextBuffer(): Boolean {
        if (!iterator.hasNext()) {
            current = null
            return false
        }

        val current = iterator.next().duplicate()
        this.current = current

        if (mark >= 0) {
            if (position < markLimit) {
                // the mark is defined and valid. save the new buffer
                markBuffers.add(current.duplicate())
            } else {
                // the mark has not been used and is no longer valid
                discardMark()
            }
        }

        return true
    }

    private class ConcatIterator<E>(private val first: Iterator<E>, private val second: Iterator<E>) : Iterator<E> {
        var useFirst: Boolean = true

        override fun hasNext(): Boolean {
            if (useFirst) {
                if (first.hasNext()) {
                    return true
                } else {
                    useFirst = false
                    return second.hasNext()
                }
            }
            return second.hasNext()
        }

        override fun next(): E {
            if (useFirst && !first.hasNext()) {
                useFirst = false
            }

            if (!useFirst && !second.hasNext()) {
                throw NoSuchElementException()
            }

            if (useFirst) {
                return first.next()
            }

            return second.next()
        }
    }

    companion object {
        private val EMPTY: ByteBuffer = ByteBuffer.allocate(0)

        private fun <E> concat(first: Iterator<E>, second: Iterator<E>): Iterator<E> {
            return ConcatIterator<E>(first, second)
        }
    }
}
