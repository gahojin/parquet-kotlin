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
package org.apache.parquet.column.impl

import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.PageReader
import org.apache.parquet.io.api.PrimitiveConverter
import java.util.*

/**
 * A [ColumnReader] implementation for utilizing indexes. When filtering using column indexes we might skip
 * reading some pages for different columns. Because the rows are not aligned between the pages of the different columns
 * it might be required to skip some values in this [ColumnReader] so we provide only the required values for the
 * higher API ([RecordReader]) and they do not need to handle or know about the skipped pages. The values (and the
 * related rl and dl) are skipped based on the iterator of the required row indexes and the first row index of each
 * page.<br></br>
 * For example:
 *
 * ```
 * rows   col1   col2   col3
 *      ┌──────┬──────┬──────┐
 *   0  │  p0  │      │      │
 *      ╞══════╡  p0  │  p0  │
 *  20  │ p1(X)│------│------│
 *      ╞══════╪══════╡      │
 *  40  │ p2(X)│      │------│
 *      ╞══════╡ p1(X)╞══════╡
 *  60  │ p3(X)│      │------│
 *      ╞══════╪══════╡      │
 *  80  │  p4  │      │  p1  │
 *      ╞══════╡  p2  │      │
 * 100  │  p5  │      │      │
 *      └──────┴──────┴──────┘
 * ```
 *
 * The pages 1, 2, 3 in col1 are skipped so we have to skip the rows [20, 79]. Because page 1 in col2 contains values
 * only for the rows [40, 79] we skip this entire page as well. To synchronize the row reading we have to skip the
 * values (and the related rl and dl) for the rows [20, 39] in the end of the page 0 for col2. Similarly, we have to
 * skip values while reading page0 and page1 for col3.
 */
internal class SynchronizingColumnReader(
    path: ColumnDescriptor,
    pageReader: PageReader,
    converter: PrimitiveConverter,
    writerVersion: ParsedVersion?,
    private val rowIndexes: PrimitiveIterator.OfLong
) : ColumnReaderBase(path, pageReader, converter, writerVersion) {
    private var currentRow: Long = 0L
    private var targetRow: Long = Long.MIN_VALUE
    private var lastRowInPage: Long = 0L
    private var valuesReadFromPage = 0L

    init {
        consume()
    }

    override val isPageFullyConsumed: Boolean
        get() = pageValueCount <= valuesReadFromPage || lastRowInPage < targetRow

    override val isFullyConsumed: Boolean
        get() = !rowIndexes.hasNext()

    override fun skipRL(rl: Int): Boolean {
        ++valuesReadFromPage
        if (rl == 0) {
            ++currentRow
            if (currentRow > targetRow) {
                targetRow = if (rowIndexes.hasNext()) rowIndexes.nextLong() else Long.MAX_VALUE
            }
        }
        return currentRow < targetRow
    }

    override fun newPageInitialized(page: DataPage) {
        val firstRowIndex = page.getFirstRowIndex().orElseThrow {
            IllegalArgumentException("Missing firstRowIndex for synchronizing values")
        }
        val rowCount = page.getIndexRowCount().orElseThrow {
            IllegalArgumentException("Missing rowCount for synchronizing values")
        }
        currentRow = firstRowIndex - 1
        lastRowInPage = firstRowIndex + rowCount - 1
        valuesReadFromPage = 0
    }
}
