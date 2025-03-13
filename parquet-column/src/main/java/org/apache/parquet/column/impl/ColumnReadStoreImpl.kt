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
import org.apache.parquet.VersionParser.parse
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ColumnReadStore
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.Type

/**
 * Implementation of the ColumnReadStore
 *
 * Initializes individual columns based on schema and converter
 *
 * @param pageReadStore   underlying page storage
 * @param recordConverter the user provided converter to materialize records
 * @param schema          the schema we are reading
 * @param createdBy       writer version string from the Parquet file being read
 */
class ColumnReadStoreImpl(
    private val pageReadStore: PageReadStore,
    private val recordConverter: GroupConverter,
    private val schema: MessageType,
    createdBy: String?,
) : ColumnReadStore {
    private val writerVersion: ParsedVersion? = runCatching {
        createdBy?.let { parse(it) }
    }.getOrNull()

    override fun getColumnReader(path: ColumnDescriptor): ColumnReader {
        val converter = getPrimitiveConverter(path)
        val pageReader = pageReadStore.getPageReader(path)
        val rowIndexes = pageReadStore.rowIndexes
        return if (rowIndexes.isPresent) {
            SynchronizingColumnReader(path, pageReader, converter, writerVersion, rowIndexes.get())
        } else {
            ColumnReaderImpl(path, pageReader, converter, writerVersion)
        }
    }

    private fun getPrimitiveConverter(path: ColumnDescriptor): PrimitiveConverter {
        var currentType: Type = schema
        var currentConverter: Converter = recordConverter
        for (fieldName in path.path) {
            val groupType = currentType.asGroupType()
            val fieldIndex = groupType.getFieldIndex(fieldName)
            currentType = groupType.getType(fieldName)
            currentConverter = currentConverter.asGroupConverter().getConverter(fieldIndex)
        }
        return currentConverter.asPrimitiveConverter()
    }
}
