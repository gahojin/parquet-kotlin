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
import org.apache.parquet.column.statistics.Statistics
import java.util.OptionalInt

/**
 * @param bytes            the bytes for this page
 * @param valueCount       count of values in this page
 * @param uncompressedSize the uncompressed size of the page
 * @param firstRowIndex    the index of the first row in this page
 * @param rowCount         the number of rows in this page
 * @param statistics       of the page's values (max, min, num_null)
 * @param rlEncoding       the repetition level encoding for this page
 * @param dlEncoding       the definition level encoding for this page
 * @param valueEncoding    the values encoding for this page
 */
class DataPageV1(
    val bytes: BytesInput,
    valueCount: Int,
    uncompressedSize: Int,
    firstRowIndex: Long,
    private val rowCount: Int,
    val statistics: Statistics<*>,
    val rlEncoding: Encoding,
    val dlEncoding: Encoding,
    val valueEncoding: Encoding,
) : DataPage(
    compressedSize = Math.toIntExact(bytes.size()),
    uncompressedSize = uncompressedSize,
    valueCount = valueCount,
    firstRowIndex = firstRowIndex,
) {

    /**
     * @param bytes            the bytes for this page
     * @param valueCount       count of values in this page
     * @param uncompressedSize the uncompressed size of the page
     * @param statistics       of the page's values (max, min, num_null)
     * @param rlEncoding       the repetition level encoding for this page
     * @param dlEncoding       the definition level encoding for this page
     * @param valueEncoding    the values encoding for this page
     */
    constructor(
        bytes: BytesInput,
        valueCount: Int,
        uncompressedSize: Int,
        statistics: Statistics<*>,
        rlEncoding: Encoding,
        dlEncoding: Encoding,
        valueEncoding: Encoding,
    ) : this(
        bytes = bytes,
        valueCount = valueCount,
        uncompressedSize = uncompressedSize,
        firstRowIndex = -1L,
        rowCount = -1,
        statistics = statistics,
        rlEncoding = rlEncoding,
        dlEncoding = dlEncoding,
        valueEncoding = valueEncoding,
    )

    override fun toString(): String {
        return "Page [bytes.size=${bytes.size()}, valueCount=$valueCount, uncompressedSize=$uncompressedSize]"
    }

    override fun <T> accept(visitor: Visitor<T>): T {
        return visitor.visit(this)
    }

    override fun getIndexRowCount(): OptionalInt {
        return if (rowCount < 0) OptionalInt.empty() else OptionalInt.of(rowCount)
    }
}
