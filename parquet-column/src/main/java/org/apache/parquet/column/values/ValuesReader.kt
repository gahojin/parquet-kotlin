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
package org.apache.parquet.column.values

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.bytes.ByteBufferInputStream.Companion.wrap
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary
import java.io.IOException
import java.nio.ByteBuffer

/**
 * Base class to implement an encoding for a given column type.
 *
 * A ValuesReader is provided with a page (byte-buffer) and is responsible
 * for deserializing the primitive values stored in that page.
 *
 * Given that pages are homogeneous (store only a single type), typical subclasses
 * will only override one of the read*() methods.
 */
abstract class ValuesReader {
    // To be used to maintain the deprecated behavior of getNextOffset(); -1 means undefined
    private var actualOffset = -1
    private var nextOffset = 0

    /**
     * Called to initialize the column reader from a part of a page.
     *
     * The underlying implementation knows how much data to read, so a length
     * is not provided.
     *
     * Each page may contain several sections:
     *
     *  *  repetition levels column
     *  *  definition levels column
     *  *  data column
     *
     * This function is called with 'offset' pointing to the beginning of one of these sections,
     * and should return the offset to the section following it.
     *
     * @param valueCount count of values in this page
     * @param page       the array to read from containing the page data (repetition levels, definition levels, data)
     * @param offset     where to start reading from in the page
     * @throws IOException
     */
    @Deprecated("Will be removed in 2.0.0")
    @Throws(IOException::class)
    open fun initFromPage(valueCount: Int, page: ByteBuffer, offset: Int) {
        require(offset >= 0) { "Illegal offset: $offset" }
        actualOffset = offset
        val pageWithOffset = page.duplicate()
        pageWithOffset.position(offset)
        initFromPage(valueCount, wrap(pageWithOffset))
        actualOffset = -1
    }

    /**
     * Same functionality as method of the same name that takes a ByteBuffer instead of a byte[].
     *
     * This method is only provided for backward compatibility and will be removed in a future release.
     * Please update any code using it as soon as possible.
     *
     * @see .initFromPage
     */
    @Deprecated("")
    @Throws(IOException::class)
    fun initFromPage(valueCount: Int, page: ByteArray, offset: Int) {
        initFromPage(valueCount, ByteBuffer.wrap(page), offset)
    }

    /**
     * Called to initialize the column reader from a part of a page.
     *
     * Implementations must consume all bytes from the input stream, leaving the
     * stream ready to read the next section of data. The underlying
     * implementation knows how much data to read, so a length is not provided.
     *
     * Each page may contain several sections:
     *
     *  *  repetition levels column
     *  *  definition levels column
     *  *  data column
     *
     * @param valueCount count of values in this page
     * @param in         an input stream containing the page data at the correct offset
     * @throws IOException if there is an exception while reading from the input stream
     */
    @Throws(IOException::class)
    open fun initFromPage(valueCount: Int, `in`: ByteBufferInputStream) {
        if (actualOffset != -1) {
            throw UnsupportedOperationException(
                "Either initFromPage(int, ByteBuffer, int) or initFromPage(int, ByteBufferInputStream) must be implemented in ${javaClass.name}"
            )
        }
        initFromPage(valueCount, `in`.slice(valueCount), 0)
    }

    /**
     * Called to return offset of the next section
     *
     * @return offset of the next section
     */
    @Deprecated("Will be removed in 2.0.0")
    open fun getNextOffset(): Int {
        return if (nextOffset == -1) {
            throw ParquetDecodingException("Unsupported: cannot get offset of the next section.")
        } else {
            nextOffset
        }
    }

    // To be used to maintain the deprecated behavior of getNextOffset();
    // bytesRead is the number of bytes read in the last initFromPage call
    protected fun updateNextOffset(bytesRead: Int) {
        nextOffset = if (actualOffset == -1) -1 else actualOffset + bytesRead
    }

    /**
     * usable when the encoding is dictionary based
     *
     * @return the id of the next value from the page
     */
    open fun readValueDictionaryId(): Int {
        throw UnsupportedOperationException()
    }

    /**
     * @return the next boolean from the page
     */
    open fun readBoolean(): Boolean {
        throw UnsupportedOperationException()
    }

    /**
     * @return the next Binary from the page
     */
    open fun readBytes(): Binary {
        throw UnsupportedOperationException()
    }

    /**
     * @return the next float from the page
     */
    open fun readFloat(): Float {
        throw UnsupportedOperationException()
    }

    /**
     * @return the next double from the page
     */
    open fun readDouble(): Double {
        throw UnsupportedOperationException()
    }

    /**
     * @return the next integer from the page
     */
    open fun readInteger(): Int {
        throw UnsupportedOperationException()
    }

    /**
     * @return the next long from the page
     */
    open fun readLong(): Long {
        throw UnsupportedOperationException()
    }

    /**
     * Skips the next value in the page
     */
    abstract fun skip()

    /**
     * Skips the next n values in the page
     *
     * @param n the number of values to be skipped
     */
    open fun skip(n: Int) {
        for (i in 0 until n) {
             skip()
        }
    }
}
