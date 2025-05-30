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
package org.apache.parquet.internal.column.columnindex

import java.util.Optional

/**
 * Offset index containing the offset and size of the page and the index of the first row in the page.
 *
 * @see org.apache.parquet.format.OffsetIndex
 */
interface OffsetIndex {
    /** the number of pages */
    val pageCount: Int

    /**
     * @param pageIndex the index of the page
     * @return the offset of the page in the file
     */
    fun getOffset(pageIndex: Int): Long

    /**
     * @param pageIndex the index of the page
     * @return the compressed size of the page (including page header)
     */
    fun getCompressedPageSize(pageIndex: Int): Int

    /**
     * @param pageIndex the index of the page
     * @return the index of the first row in the page
     */
    fun getFirstRowIndex(pageIndex: Int): Long

    /**
     * @param pageIndex the index of the page
     * @return the original ordinal of the page in the column chunk
     */
    fun getPageOrdinal(pageIndex: Int): Int {
        return pageIndex
    }

    /**
     * @param pageIndex        the index of the page
     * @param rowGroupRowCount the total number of rows in the row-group
     * @return the calculated index of the last row of the given page
     */
    fun getLastRowIndex(pageIndex: Int, rowGroupRowCount: Long): Long {
        val nextPageIndex = pageIndex + 1
        return (if (nextPageIndex >= this.pageCount) rowGroupRowCount else getFirstRowIndex(nextPageIndex)) - 1
    }

    /**
     * @param pageIndex
     * the index of the page
     * @return unencoded/uncompressed size for BYTE_ARRAY types; or empty for other types.
     * Please note that even for BYTE_ARRAY types, this value might not have been written.
     */
    fun getUnencodedByteArrayDataBytes(pageIndex: Int): Optional<Long> {
        throw UnsupportedOperationException("Un-encoded byte array data bytes is not implemented")
    }
}
