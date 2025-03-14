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
import java.util.*

class DataPageV2 private constructor(
    val rowCount: Int,
    val nullCount: Int,
    valueCount: Int,
    firstRowIndex: Long,
    val repetitionLevels: BytesInput,
    val definitionLevels: BytesInput,
    val dataEncoding: Encoding,
    val data: BytesInput,
    uncompressedSize: Int,
    val statistics: Statistics<*>,
    val isCompressed: Boolean,
) : DataPage(
    compressedSize = Math.toIntExact(repetitionLevels.size() + definitionLevels.size() + data.size()),
    uncompressedSize = uncompressedSize,
    valueCount = valueCount,
    firstRowIndex = firstRowIndex,
) {

    constructor(
        rowCount: Int,
        nullCount: Int,
        valueCount: Int,
        repetitionLevels: BytesInput,
        definitionLevels: BytesInput,
        dataEncoding: Encoding,
        data: BytesInput,
        uncompressedSize: Int,
        statistics: Statistics<*>,
        isCompressed: Boolean
    ) : this(
        rowCount = rowCount,
        nullCount = nullCount,
        valueCount = valueCount,
        firstRowIndex = -1L,
        repetitionLevels = repetitionLevels,
        definitionLevels = definitionLevels,
        dataEncoding = dataEncoding,
        data = data,
        uncompressedSize = uncompressedSize,
        statistics = statistics,
        isCompressed = isCompressed,
    )

    override fun getIndexRowCount(): OptionalInt = OptionalInt.of(rowCount)

    override fun <T> accept(visitor: Visitor<T>): T {
        return visitor.visit(this)
    }

    override fun toString(): String {
        return "Page V2 [dl size=${definitionLevels.size()}, rl size=${repetitionLevels.size()}, data size=${data.size()}, data enc=${dataEncoding}, valueCount=${valueCount}, rowCount=${rowCount}, is compressed=${isCompressed}, uncompressedSize=${uncompressedSize}]"
    }

    companion object {
        /**
         * @param rowCount         count of rows
         * @param nullCount        count of nulls
         * @param valueCount       count of values
         * @param repetitionLevels RLE encoded repetition levels
         * @param definitionLevels RLE encoded definition levels
         * @param dataEncoding     encoding for the data
         * @param data             data encoded with dataEncoding
         * @param statistics       optional statistics for this page
         * @return an uncompressed page
         */
        @JvmStatic
        fun uncompressed(
            rowCount: Int,
            nullCount: Int,
            valueCount: Int,
            repetitionLevels: BytesInput,
            definitionLevels: BytesInput,
            dataEncoding: Encoding,
            data: BytesInput,
            statistics: Statistics<*>,
        ): DataPageV2 = DataPageV2(
            rowCount = rowCount,
            nullCount = nullCount,
            valueCount = valueCount,
            repetitionLevels = repetitionLevels,
            definitionLevels = definitionLevels,
            dataEncoding = dataEncoding,
            data = data,
            uncompressedSize = Math.toIntExact(repetitionLevels.size() + definitionLevels.size() + data.size()),
            statistics = statistics,
            isCompressed = false,
        )

        /**
         * @param rowCount         count of rows
         * @param nullCount        count of nulls
         * @param valueCount       count of values
         * @param firstRowIndex    the index of the first row in this page
         * @param repetitionLevels RLE encoded repetition levels
         * @param definitionLevels RLE encoded definition levels
         * @param dataEncoding     encoding for the data
         * @param data             data encoded with dataEncoding
         * @param statistics       optional statistics for this page
         * @return an uncompressed page
         */
        @JvmStatic
        fun uncompressed(
            rowCount: Int,
            nullCount: Int,
            valueCount: Int,
            firstRowIndex: Long,
            repetitionLevels: BytesInput,
            definitionLevels: BytesInput,
            dataEncoding: Encoding,
            data: BytesInput,
            statistics: Statistics<*>,
        ): DataPageV2 = DataPageV2(
            rowCount = rowCount,
            nullCount = nullCount,
            valueCount = valueCount,
            firstRowIndex = firstRowIndex,
            repetitionLevels = repetitionLevels,
            definitionLevels = definitionLevels,
            dataEncoding = dataEncoding,
            data = data,
            uncompressedSize = Math.toIntExact(repetitionLevels.size() + definitionLevels.size() + data.size()),
            statistics = statistics,
            isCompressed = false,
        )

        /**
         * @param rowCount         count of rows
         * @param nullCount        count of nulls
         * @param valueCount       count of values
         * @param repetitionLevels RLE encoded repetition levels
         * @param definitionLevels RLE encoded definition levels
         * @param dataEncoding     encoding for the data
         * @param data             data encoded with dataEncoding and compressed
         * @param uncompressedSize total size uncompressed (rl + dl + data)
         * @param statistics       optional statistics for this page
         * @return a compressed page
         */
        @JvmStatic
        fun compressed(
            rowCount: Int,
            nullCount: Int,
            valueCount: Int,
            repetitionLevels: BytesInput,
            definitionLevels: BytesInput,
            dataEncoding: Encoding,
            data: BytesInput,
            uncompressedSize: Int,
            statistics: Statistics<*>,
        ): DataPageV2 = DataPageV2(
            rowCount = rowCount,
            nullCount = nullCount,
            valueCount = valueCount,
            repetitionLevels = repetitionLevels,
            definitionLevels = definitionLevels,
            dataEncoding = dataEncoding,
            data = data,
            uncompressedSize = uncompressedSize,
            statistics = statistics,
            isCompressed = true,
        )
    }
}
