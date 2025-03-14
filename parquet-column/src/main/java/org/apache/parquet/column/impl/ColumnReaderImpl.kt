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

/**
 * ColumnReader implementation for the scenario when column indexes are not used (all values are read)
 *
 * @param path          the descriptor for the corresponding column
 * @param pageReader    the underlying store to read from
 * @param converter     a converter that materializes the values in this column in the current record
 * @param writerVersion writer version string from the Parquet file being read
 */
internal class ColumnReaderImpl(
    path: ColumnDescriptor,
    pageReader: PageReader,
    converter: PrimitiveConverter,
    writerVersion: ParsedVersion?,
) : ColumnReaderBase(path, pageReader, converter, writerVersion) {
    init {
        consume()
    }

    override fun skipRL(rl: Int): Boolean = false

    override fun newPageInitialized(page: DataPage) = Unit
}
