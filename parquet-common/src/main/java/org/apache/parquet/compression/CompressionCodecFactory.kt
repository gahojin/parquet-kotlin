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
package org.apache.parquet.compression

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import java.io.IOException
import java.nio.ByteBuffer

/**
 * Factory for creating (and potentially caching) [BytesInputCompressor] and [BytesInputDecompressor]
 * instances to compress/decompress page data.
 *
 * The factory instance shall be released after use. The compressor/decompressor instances shall not be used after
 * release.
 *
 * @see .release
 */
interface CompressionCodecFactory {
    /**
     * Returns a [BytesInputCompressor] instance for the specified codec name to be used for compressing page data.
     *
     * The compressor is not thread-safe, so one instance for each working thread is required.
     *
     * @param codecName the codec name which the compressor instance is to be returned
     * @return the compressor instance for the specified codec name
     * @see BytesInputCompressor.release
     */
    fun getCompressor(codecName: CompressionCodecName): BytesInputCompressor

    /**
     * Returns a [BytesInputDecompressor] instance for the specified codec name to be used for decompressing page
     * data.
     *
     * The decompressor is not thread-safe, so one instance for each working thread is required.
     *
     * @param codecName the codec name which the decompressor instance is to be returned
     * @return the decompressor instance for the specified codec name
     * @see BytesInputDecompressor.release
     */
    fun getDecompressor(codecName: CompressionCodecName): BytesInputDecompressor

    /**
     * Releasing this factory instance.
     *
     * Each compressor/decompressor instance shall be released before invoking this. Nor the compressor/decompressor
     * instances retrieved from this factory nor this factory instance itself shall be used after release.
     *
     * @see BytesInputCompressor.release
     * @see BytesInputDecompressor.release
     */
    fun release()

    /**
     * Compressor instance of a specific codec to be used for compressing page data.
     *
     * This compressor shall be released after use. This compressor shall not be used after release.
     *
     * @see .release
     */
    interface BytesInputCompressor {
        /** the codec name of this compressor. */
        val codecName: CompressionCodecName

        /**
         * Compresses the specified [BytesInput] data and returns it as [BytesInput].
         *
         * Depending on the implementation `bytes` might be completely consumed. The returned [BytesInput]
         * instance needs to be consumed before using this compressor again. This is because the implementation might use
         * its internal buffer to directly provide the returned [BytesInput] instance.
         *
         * @param bytes the page data to be compressed
         * @return a [BytesInput] containing the compressed data. Needs to be consumed before using this compressor
         * again.
         * @throws IOException if any I/O error occurs during the compression
         */
        @Throws(IOException::class)
        fun compress(bytes: BytesInput): BytesInput

        /**
         * Releases this compressor instance.
         *
         * No subsequent calls on this instance nor the returned [BytesInput] instance returned by
         * [.compress] shall be used after release.
         */
        fun release()
    }

    /**
     * Decompressor instance of a specific codec to be used for decompressing page data.
     *
     *
     * This decompressor shall be released after use. This decompressor shall not be used after release.
     *
     * @see .release
     */
    interface BytesInputDecompressor {
        /**
         * Decompresses the specified [BytesInput] data and returns it as [BytesInput].
         *
         * The decompressed data must have the size specified. Depending on the implementation `bytes` might be
         * completely consumed. The returned [BytesInput] instance needs to be consumed before using this decompressor
         * again. This is because the implementation might use its internal buffer to directly provide the returned
         * [BytesInput] instance.
         *
         * @param bytes            the page data to be decompressed
         * @param decompressedSize the exact size of the decompressed data
         * @return a [BytesInput] containing the decompressed data. Needs to be consumed before using this
         * decompressor again.
         * @throws IOException if any I/O error occurs during the decompression
         */
        @Throws(IOException::class)
        fun decompress(bytes: BytesInput, decompressedSize: Int): BytesInput

        /**
         * Decompresses `compressedSize` bytes from `input` from the current position. The decompressed bytes is
         * to be written int `output` from its current position. The decompressed data must have the size specified.
         *
         * `output` must have the available bytes of `decompressedSize`. According to the [ByteBuffer]
         * contract the position of `input` will be increased by `compressedSize`, and the position of
         * `output` will be increased by `decompressedSize`. (It means, one would have to flip the output buffer
         * before reading the decompressed data from it.)
         *
         * @param input            the input buffer where the data is to be decompressed from
         * @param compressedSize   the exact size of the compressed (input) data
         * @param output           the output buffer where the data is to be decompressed into
         * @param decompressedSize the exact size of the decompressed (output) data
         * @throws IOException if any I/O error occurs during the decompression
         */
        @Throws(IOException::class)
        fun decompress(input: ByteBuffer, compressedSize: Int, output: ByteBuffer, decompressedSize: Int)

        /**
         * Releases this decompressor instance. No subsequent calls on this instance nor the returned [BytesInput]
         * instance returned by [.decompress] shall be used after release.
         */
        fun release()
    }
}
