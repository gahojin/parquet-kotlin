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

import org.apache.parquet.bytes.ByteBufferInputStream.Companion.wrap
import org.apache.parquet.bytes.BytesUtils.writeIntLittleEndian
import org.apache.parquet.bytes.BytesUtils.writeUnsignedVarInt
import org.apache.parquet.bytes.BytesUtils.writeUnsignedVarLong
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.Channels
import java.util.function.Consumer

/**
 * A source of bytes capable of writing itself to an output.
 * A BytesInput should be consumed right away.
 * It is not a container.
 * For example if it is referring to a stream,
 * subsequent BytesInput reads from the stream will be incorrect
 * if the previous has not been consumed.
 */
abstract class BytesInput {
    /**
     * writes the bytes into a stream
     *
     * @param out an output stream
     * @throws IOException if there is an exception writing
     */
    @Throws(IOException::class)
    abstract fun writeAllTo(out: OutputStream)

    /**
     * For internal use only. It is expected that the buffer is large enough to fit the content of this [BytesInput]
     * object.
     */
    abstract fun writeInto(buffer: ByteBuffer)

    /**
     * @return a new byte array materializing the contents of this input
     * @throws IOException if there is an exception reading
     */
    @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
    @Throws(IOException::class)
    open fun toByteArray(): ByteArray {
        val size = size()
        if (size > Int.Companion.MAX_VALUE) {
            throw IOException("Page size, $size, is larger than allowed ${Int.MAX_VALUE}."
                    + " Usually caused by a Parquet writer writing too big column chunks on encountering highly skewed dataset."
                    + " Please set page.size.row.check.max to a lower value on the writer, default value is 10000."
                    + " You can try setting it to ${10000 / (size / Int.Companion.MAX_VALUE)} or lower.")
        }
        val baos = BAOS(size().toInt())
        this.writeAllTo(baos)
        LOG.debug("converted {} to byteArray of {} bytes", size(), baos.size())
        return baos.buf
    }

    /**
     * @return a new ByteBuffer materializing the contents of this input
     * @throws IOException if there is an exception reading
     */
    @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
    @Throws(IOException::class)
    open fun toByteBuffer(): ByteBuffer {
        return ByteBuffer.wrap(toByteArray())
    }

    /**
     * Copies the content of this [BytesInput] object to a newly created [ByteBuffer] and returns it wrapped
     * in a [BytesInput] object.
     *
     * **The data content shall be able to be fit in a [ByteBuffer] object!** (In case of the size of
     * this [BytesInput] object cannot fit in an `int`, an [ArithmeticException] will be thrown. The
     * `allocator` might throw an [OutOfMemoryError] if it is unable to allocate the required
     * [ByteBuffer].)
     *
     * @param allocator the allocator to be used for creating the new [ByteBuffer] object
     * @param callback  the callback called with the newly created [ByteBuffer] object; to be used for make it
     * released at the proper time
     * @return the newly created [BytesInput] object wrapping the copied content of the specified one
     */
    fun copy(allocator: ByteBufferAllocator, callback: Consumer<ByteBuffer?>): BytesInput {
        val buf = allocator.allocate(Math.toIntExact(size()))
        callback.accept(buf)
        writeInto(buf)
        buf.flip()
        return from(buf)
    }

    /**
     * Similar to [.copy] where the allocator and the callback are in the specified
     * [ByteBufferReleaser].
     */
    fun copy(releaser: ByteBufferReleaser): BytesInput {
        return copy(releaser.allocator, Consumer { buffer: ByteBuffer? -> releaser.releaseLater(buffer!!) })
    }

    /**
     * Returns a [ByteBuffer] object referencing the data behind this [BytesInput] object. It may create a new
     * [ByteBuffer] object if this [BytesInput] is not backed by a single [ByteBuffer]. In the latter
     * case the specified [ByteBufferAllocator] object will be used. In case of allocation the specified callback
     * will be invoked so the release of the newly allocated [ByteBuffer] object can be released at a proper time.
     *
     * **The data content shall be able to be fit in a [ByteBuffer] object!** (In case of the size of
     * this [BytesInput] object cannot fit in an `int`, an [ArithmeticException] will be thrown. The
     * `allocator` might throw an [OutOfMemoryError] if it is unable to allocate the required
     * [ByteBuffer].)
     *
     * @param allocator the [ByteBufferAllocator] to be used for potentially allocating a new [ByteBuffer]
     * object
     * @param callback  the callback to be called with the new [ByteBuffer] object potentially allocated
     * @return the [ByteBuffer] object with the data content of this [BytesInput] object. (Might be a copy of
     * the content or directly referencing the same memory as this [BytesInput] object.)
     */
    fun toByteBuffer(allocator: ByteBufferAllocator, callback: Consumer<ByteBuffer?>): ByteBuffer {
        var buf = internalByteBuffer
        // The internal buffer should be direct iff the allocator is direct as well but let's be sure
        if (buf == null || buf.isDirect != allocator.isDirect) {
            buf = allocator.allocate(Math.toIntExact(size()))
            callback.accept(buf)
            writeInto(buf)
            buf.flip()
        }
        return buf
    }

    /**
     * Similar to [.toByteBuffer] where the allocator and the callback are in the
     * specified [ByteBufferReleaser].
     */
    fun toByteBuffer(releaser: ByteBufferReleaser): ByteBuffer {
        return toByteBuffer(releaser.allocator, Consumer { buffer: ByteBuffer? -> releaser.releaseLater(buffer!!) })
    }

    /**
     * For internal use only.
     *
     * Returns a [ByteBuffer] object referencing to the internal data of this [BytesInput] without copying if
     * applicable. If it is not possible (because there are multiple [ByteBuffer]s internally or cannot be
     * referenced as a [ByteBuffer]), `null` value will be returned.
     *
     * @return the internal data of this [BytesInput] or `null`
     */
    protected open val internalByteBuffer: ByteBuffer?
        get() = null

    /**
     * @return a new InputStream materializing the contents of this input
     * @throws IOException if there is an exception reading
     */
    @Throws(IOException::class)
    open fun toInputStream(): ByteBufferInputStream {
        return wrap(toByteBuffer())
    }

    /**
     * @return the size in bytes that would be written
     */
    abstract fun size(): Long

    private class BAOS(size: Int) : ByteArrayOutputStream(size) {
        val buf: ByteArray
            get() = super.buf
    }

    private class StreamBytesInput(private val `in`: InputStream, private val byteCount: Int) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            LOG.debug("write All {} bytes", byteCount)
            // TODO: more efficient
            out.write(this.toByteArray())
        }

        override fun writeInto(buffer: ByteBuffer) {
            try {
                // Needs a duplicate buffer to set the correct limit (we do not want to over-read the stream)
                val workBuf = buffer.duplicate()
                val pos = buffer.position()
                workBuf.limit(pos + byteCount)
                val channel = Channels.newChannel(`in`)
                var remaining = byteCount
                while (remaining > 0) {
                    val bytesRead = channel.read(workBuf)
                    if (bytesRead < 0) {
                        throw EOFException("Reached the end of stream with $remaining bytes left to read")
                    }
                    remaining -= bytesRead
                }
                buffer.position(pos + byteCount)
            } catch (e: IOException) {
                throw RuntimeException("Exception occurred during reading input stream", e)
            }
        }

        @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
        @Throws(IOException::class)
        override fun toByteArray(): ByteArray {
            LOG.debug("read all {} bytes", byteCount)
            val buf = ByteArray(byteCount)
            DataInputStream(`in`).readFully(buf)
            return buf
        }

        override fun size(): Long {
            return byteCount.toLong()
        }

        companion object {
            private val LOG: Logger = LoggerFactory.getLogger(StreamBytesInput::class.java)
        }
    }

    private class SequenceBytesIn(
        private val inputs: List<BytesInput>,
    ) : BytesInput() {
        private val size: Long = inputs.sumOf { it.size() }

        @Suppress("unused")
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            for (input in inputs) {
                LOG.debug("write {} bytes to out", input.size())
                if (input is SequenceBytesIn) LOG.debug("{")
                input.writeAllTo(out)
                if (input is SequenceBytesIn) LOG.debug("}")
            }
        }

        override fun writeInto(buffer: ByteBuffer) {
            for (input in inputs) {
                input.writeInto(buffer)
            }
        }

        override val internalByteBuffer: ByteBuffer?
            get() = if (inputs.size == 1) inputs[0].internalByteBuffer else null

        override fun size() = size

        companion object {
            private val LOG: Logger = LoggerFactory.getLogger(SequenceBytesIn::class.java)
        }
    }

    private class IntBytesInput(private val intValue: Int) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            writeIntLittleEndian(out, intValue)
        }

        override fun writeInto(buffer: ByteBuffer) {
            buffer.order(ByteOrder.LITTLE_ENDIAN).putInt(intValue)
        }

        @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
        override fun toByteBuffer(): ByteBuffer {
            val buf = ByteBuffer.allocate(4)
            writeInto(buf)
            buf.flip()
            return buf
        }

        override fun size() = 4L
    }

    private class UnsignedVarIntBytesInput(private val intValue: Int) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            writeUnsignedVarInt(intValue, out)
        }

        override fun writeInto(buffer: ByteBuffer) {
            try {
                writeUnsignedVarInt(intValue, buffer)
            } catch (e: IOException) {
                // It does not actually throw an I/O exception, but we cannot remove throws for compatibility
                throw RuntimeException(e)
            }
        }

        @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
        override fun toByteBuffer(): ByteBuffer {
            val ret = ByteBuffer.allocate(size().toInt())
            writeInto(ret)
            ret.flip()
            return ret
        }

        override fun size(): Long {
            val s = (38 - Integer.numberOfLeadingZeros(intValue)) / 7
            return (if (s == 0) 1 else s).toLong()
        }
    }

    private class UnsignedVarLongBytesInput(private val longValue: Long) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            writeUnsignedVarLong(longValue, out)
        }

        override fun writeInto(buffer: ByteBuffer) {
            writeUnsignedVarLong(longValue, buffer)
        }

        override fun size(): Long {
            val s = (70 - java.lang.Long.numberOfLeadingZeros(longValue)) / 7
            return (if (s == 0) 1 else s).toLong()
        }
    }

    private class EmptyBytesInput : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) = Unit

        override fun writeInto(buffer: ByteBuffer) = Unit

        override fun size() = 0L

        @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
        override fun toByteBuffer(): ByteBuffer {
            return ByteBuffer.allocate(0)
        }
    }

    private class CapacityBAOSBytesInput(
        private val arrayOut: CapacityByteArrayOutputStream,
    ) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            arrayOut.writeTo(out)
        }

        override fun writeInto(buffer: ByteBuffer) {
            arrayOut.writeInto(buffer)
        }

        override val internalByteBuffer: ByteBuffer?
            get() = arrayOut.internalByteBuffer

        override fun size() = arrayOut.size()
    }

    private class BAOSBytesInput(private val arrayOut: ByteArrayOutputStream) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            arrayOut.writeTo(out)
        }

        override fun writeInto(buffer: ByteBuffer) {
            buffer.put(arrayOut.toByteArray())
        }

        override fun size() = arrayOut.size().toLong()
    }

    private class ByteArrayBytesInput(
        private val `in`: ByteArray,
        private val offset: Int,
        private val length: Int,
    ) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            out.write(`in`, offset, length)
        }

        override fun writeInto(buffer: ByteBuffer) {
            buffer.put(`in`, offset, length)
        }

        @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
        @Throws(IOException::class)
        override fun toByteBuffer(): ByteBuffer {
            return ByteBuffer.wrap(`in`, offset, length)
        }

        override fun size() = length.toLong()
    }

    private class BufferListBytesInput(
        private val buffers: List<ByteBuffer>,
    ) : BytesInput() {
        private val length: Long = buffers.sumOf { it.remaining().toLong() }

        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            val channel = Channels.newChannel(out)
            for (buffer in buffers) {
                channel.write(buffer.duplicate())
            }
        }

        override fun writeInto(target: ByteBuffer) {
            for (buffer in buffers) {
                target.put(buffer.duplicate())
            }
        }

        override fun toInputStream(): ByteBufferInputStream {
            return wrap(buffers)
        }

        override fun size() = length
    }

    private class ByteBufferBytesInput(private val buffer: ByteBuffer) : BytesInput() {
        @Throws(IOException::class)
        override fun writeAllTo(out: OutputStream) {
            Channels.newChannel(out).write(buffer.duplicate())
        }

        override fun writeInto(target: ByteBuffer) {
            target.put(buffer.duplicate())
        }

        override val internalByteBuffer: ByteBuffer?
            get() = buffer.slice()

        override fun toInputStream(): ByteBufferInputStream {
            return wrap(buffer)
        }

        override fun size() = buffer.remaining().toLong()

        @Deprecated("Use {@link #toByteBuffer(ByteBufferAllocator, Consumer)}")
        @Throws(IOException::class)
        override fun toByteBuffer(): ByteBuffer {
            return buffer.slice()
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(BytesInput::class.java)
        private val EMPTY_BYTES_INPUT = EmptyBytesInput()

        /**
         * logically concatenate the provided inputs
         *
         * @param inputs the inputs to concatenate
         * @return a concatenated input
         */
        @JvmStatic
        fun concat(vararg inputs: BytesInput): BytesInput {
            return SequenceBytesIn(inputs.toList())
        }

        /**
         * logically concatenate the provided inputs
         *
         * @param inputs the inputs to concatenate
         * @return a concatenated input
         */
        @JvmStatic
        fun concat(inputs: List<BytesInput>): BytesInput {
            return SequenceBytesIn(inputs)
        }

        /**
         * @param in    an input stream
         * @param bytes number of bytes to read
         * @return a BytesInput that will read that number of bytes from the stream
         */
        @JvmStatic
        fun from(`in`: InputStream, bytes: Int): BytesInput {
            return StreamBytesInput(`in`, bytes)
        }

        /**
         * @param buffer
         * @param length number of bytes to read
         * @return a BytesInput that will read the given bytes from the ByteBuffer
         */
        @Deprecated("Will be removed in 2.0.0")
        @JvmStatic
        fun from(buffer: ByteBuffer, offset: Int, length: Int): BytesInput {
            val tmp = buffer.duplicate()
            tmp.position(offset)
            val slice = tmp.slice()
            slice.limit(length)
            return ByteBufferBytesInput(slice)
        }

        /**
         * @param buffers an array of byte buffers
         * @return a BytesInput that will read the given bytes from the ByteBuffers
         */
        @JvmStatic
        fun from(vararg buffers: ByteBuffer): BytesInput {
            if (buffers.size == 1) {
                return ByteBufferBytesInput(buffers[0])
            }
            return BufferListBytesInput(buffers.toList())
        }

        /**
         * @param buffers a list of byte buffers
         * @return a BytesInput that will read the given bytes from the ByteBuffers
         */
        @JvmStatic
        fun from(buffers: List<ByteBuffer>): BytesInput {
            if (buffers.size == 1) {
                return ByteBufferBytesInput(buffers.get(0))
            }
            return BufferListBytesInput(buffers)
        }

        /**
         * @param in a byte array
         * @return a Bytes input that will write the given bytes
         */
        @JvmStatic
        fun from(`in`: ByteArray): BytesInput {
            LOG.debug("BytesInput from array of {} bytes", `in`.size)
            return ByteArrayBytesInput(`in`, 0, `in`.size)
        }

        @JvmStatic
        fun from(`in`: ByteArray, offset: Int, length: Int): BytesInput {
            LOG.debug("BytesInput from array of {} bytes", length)
            return ByteArrayBytesInput(`in`, offset, length)
        }

        /**
         * @param intValue the int to write
         * @return a BytesInput that will write 4 bytes in little endian
         */
        @JvmStatic
        fun fromInt(intValue: Int): BytesInput {
            return IntBytesInput(intValue)
        }

        /**
         * @param intValue the int to write
         * @return a BytesInput that will write var int
         */
        @JvmStatic
        fun fromUnsignedVarInt(intValue: Int): BytesInput {
            return UnsignedVarIntBytesInput(intValue)
        }

        /**
         * @param intValue the int to write
         * @return a ByteInput that contains the int value as a variable-length zig-zag encoded int
         */
        @JvmStatic
        fun fromZigZagVarInt(intValue: Int): BytesInput {
            val zigZag = (intValue shl 1) xor (intValue shr 31)
            return UnsignedVarIntBytesInput(zigZag)
        }

        /**
         * @param longValue the long to write
         * @return a BytesInput that will write var long
         */
        @JvmStatic
        fun fromUnsignedVarLong(longValue: Long): BytesInput {
            return UnsignedVarLongBytesInput(longValue)
        }

        /**
         * @param longValue the long to write
         * @return a ByteInput that contains the long value as a variable-length zig-zag encoded long
         */
        @JvmStatic
        fun fromZigZagVarLong(longValue: Long): BytesInput {
            val zigZag = (longValue shl 1) xor (longValue shr 63)
            return UnsignedVarLongBytesInput(zigZag)
        }

        /**
         * @param arrayOut a capacity byte array output stream to wrap into a BytesInput
         * @return a BytesInput that will write the content of the buffer
         */
        @JvmStatic
        fun from(arrayOut: CapacityByteArrayOutputStream): BytesInput {
            return CapacityBAOSBytesInput(arrayOut)
        }

        /**
         * @param baos - stream to wrap into a BytesInput
         * @return a BytesInput that will write the content of the buffer
         */
        @JvmStatic
        fun from(baos: ByteArrayOutputStream): BytesInput {
            return BAOSBytesInput(baos)
        }

        /**
         * @return an empty bytes input
         */
        @JvmStatic
        fun empty(): BytesInput {
            return EMPTY_BYTES_INPUT
        }

        /**
         * copies the input into a new byte array
         *
         * @param bytesInput a BytesInput
         * @return a copy of the BytesInput
         * @throws IOException if there is an exception when reading bytes from the BytesInput
         */
        @JvmStatic
        @Deprecated("Use {@link #copy(ByteBufferAllocator, Consumer)} instead")
        @Throws(IOException::class)
        fun copy(bytesInput: BytesInput): BytesInput {
            return from(bytesInput.toByteArray())
        }
    }
}
