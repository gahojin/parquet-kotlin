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
package org.apache.parquet.column.page

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.statistics.SizeStatistics
import org.apache.parquet.column.statistics.Statistics
import java.io.IOException

/**
 * a writer for all the pages of a given column chunk
 */
interface PageWriter : AutoCloseable {
    /**
     * @return the current size used in the memory buffer for that column chunk
     */
    val memSize: Long

    /**
     * writes a single page
     *
     * @param bytesInput     the bytes for the page
     * @param valueCount     the number of values in that page
     * @param statistics     the statistics for that page
     * @param rlEncoding     repetition level encoding
     * @param dlEncoding     definition level encoding
     * @param valuesEncoding values encoding
     * @throws IOException if there is an exception while writing page data
     */
    @Deprecated("""will be removed in 2.0.0. This method does not support writing column indexes;
        Use [#writePage(BytesInput, int, int, Statistics, Encoding, Encoding, Encoding)] instead""")
    @Throws(IOException::class)
    fun writePage(
        bytesInput: BytesInput,
        valueCount: Int,
        statistics: Statistics<*>,
        rlEncoding: Encoding,
        dlEncoding: Encoding,
        valuesEncoding: Encoding,
    )

    /**
     * writes a single page
     *
     * @param bytesInput     the bytes for the page
     * @param valueCount     the number of values in that page
     * @param rowCount       the number of rows in that page
     * @param statistics     the statistics for that page
     * @param rlEncoding     repetition level encoding
     * @param dlEncoding     definition level encoding
     * @param valuesEncoding values encoding
     * @throws IOException
     */
    @Throws(IOException::class)
    fun writePage(
        bytesInput: BytesInput,
        valueCount: Int,
        rowCount: Int,
        statistics: Statistics<*>,
        rlEncoding: Encoding,
        dlEncoding: Encoding,
        valuesEncoding: Encoding,
    )

    /**
     * writes a single page
     * @param bytesInput the bytes for the page
     * @param valueCount the number of values in that page
     * @param rowCount the number of rows in that page
     * @param statistics the statistics for that page
     * @param sizeStatistics the size statistics for that page
     * @param rlEncoding repetition level encoding
     * @param dlEncoding definition level encoding
     * @param valuesEncoding values encoding
     * @throws IOException
     */
    @Throws(IOException::class)
    fun writePage(
        bytesInput: BytesInput,
        valueCount: Int,
        rowCount: Int,
        statistics: Statistics<*>,
        sizeStatistics: SizeStatistics? = null,
        rlEncoding: Encoding,
        dlEncoding: Encoding,
        valuesEncoding: Encoding,
    ) {
        throw UnsupportedOperationException("writePage with SizeStatistics is not implemented")
    }

    /**
     * writes a single page in the new format
     *
     * @param rowCount         the number of rows in this page
     * @param nullCount        the number of null values (out of valueCount)
     * @param valueCount       the number of values in that page (there could be multiple values per row for repeated fields)
     * @param repetitionLevels the repetition levels encoded in RLE without any size header
     * @param definitionLevels the definition levels encoded in RLE without any size header
     * @param dataEncoding     the encoding for the data
     * @param data             the data encoded with dataEncoding
     * @param statistics       optional stats for this page
     * @throws IOException if there is an exception while writing page data
     */
    @Throws(IOException::class)
    fun writePageV2(
        rowCount: Int,
        nullCount: Int,
        valueCount: Int,
        repetitionLevels: BytesInput,
        definitionLevels: BytesInput,
        dataEncoding: Encoding,
        data: BytesInput,
        statistics: Statistics<*>,
    )

    /**
     * writes a single page in the new format
     * @param rowCount the number of rows in this page
     * @param nullCount the number of null values (out of valueCount)
     * @param valueCount the number of values in that page (there could be multiple values per row for repeated fields)
     * @param repetitionLevels the repetition levels encoded in RLE without any size header
     * @param definitionLevels the definition levels encoded in RLE without any size header
     * @param dataEncoding the encoding for the data
     * @param data the data encoded with dataEncoding
     * @param statistics optional stats for this page
     * @param sizeStatistics optional size stats for this page
     * @throws IOException if there is an exception while writing page data
     */
    @Throws(IOException::class)
    fun writePageV2(
        rowCount: Int,
        nullCount: Int,
        valueCount: Int,
        repetitionLevels: BytesInput,
        definitionLevels: BytesInput,
        dataEncoding: Encoding,
        data: BytesInput,
        statistics: Statistics<*>,
        sizeStatistics: SizeStatistics? = null,
    ) {
        throw UnsupportedOperationException("writePageV2 with SizeStatistics is not implemented")
    }

    /**
     * @return the allocated size for the buffer ( &gt; getMemSize() )
     */
    fun allocatedSize(): Long

    /**
     * writes a dictionary page
     *
     * @param dictionaryPage the dictionary page containing the dictionary data
     * @throws IOException if there was an exception while writing
     */
    @Throws(IOException::class)
    fun writeDictionaryPage(dictionaryPage: DictionaryPage)

    /**
     * @param prefix a prefix header to add at every line
     * @return a string presenting a summary of how memory is used
     */
    fun memUsageString(prefix: String): String

    // No-op default implementation for compatibility
    override fun close() = Unit
}
