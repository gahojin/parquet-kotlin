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

import org.apache.parquet.column.ColumnDescriptor
import java.lang.AutoCloseable
import java.util.*
import java.util.PrimitiveIterator

/**
 * contains all the readers for all the columns of the corresponding row group
 *
 * TODO: rename to RowGroup?
 */
interface PageReadStore : AutoCloseable {
    /**
     * @return the total number of rows in that row group
     */
    val rowCount: Long

    /**
     * @return the optional of the long representing the row index offset of this row group or an empty optional if the
     * related data is not available
     */
    val rowIndexOffset: Optional<Long>
        get() = Optional.empty<Long>()

    /**
     * Returns the indexes of the rows to be read/built if the related data is available. All the rows which index is not
     * returned shall be skipped.
     *
     * @return the optional of the incremental iterator of the row indexes or an empty optional if the related data is not
     * available
     */
    val rowIndexes: Optional<PrimitiveIterator.OfLong>
        get() = Optional.empty<PrimitiveIterator.OfLong>()

    /**
     * @param descriptor the descriptor of the column
     * @return the page reader for that column
     */
    fun getPageReader(descriptor: ColumnDescriptor): PageReader

    // No-op default implementation for compatibility
    override fun close() = Unit
}
