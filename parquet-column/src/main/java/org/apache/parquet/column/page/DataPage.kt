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

import java.util.OptionalInt
import java.util.OptionalLong

/**
 * one data page in a chunk
 *
 * @property valueCount the number of values in that page
 */
abstract class DataPage @JvmOverloads internal constructor(
    compressedSize: Int,
    uncompressedSize: Int,
    val valueCount: Int,
    private val firstRowIndex: Long = -1L,
) : Page(compressedSize, uncompressedSize) {
    /**
     * @return the index of the first row in this page if the related data is available (the optional column-index
     * contains this value)
     */
    fun getFirstRowIndex(): OptionalLong {
        return if (firstRowIndex < 0) OptionalLong.empty() else OptionalLong.of(firstRowIndex)
    }

    /**
     * @return the number of rows in this page if the related data is available (in case of pageV1 the optional
     * column-index contains this value)
     */
    abstract fun getIndexRowCount(): OptionalInt

    abstract fun <T> accept(visitor: Visitor<T>): T

    interface Visitor<T> {
        fun visit(dataPageV1: DataPageV1): T

        fun visit(dataPageV2: DataPageV2): T
    }
}
