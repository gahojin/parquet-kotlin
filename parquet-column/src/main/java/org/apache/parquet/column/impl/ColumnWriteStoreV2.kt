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

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.page.PageWriteStore
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter
import org.apache.parquet.schema.MessageType

class ColumnWriteStoreV2 : ColumnWriteStoreBase {
    constructor(schema: MessageType, pageWriteStore: PageWriteStore, props: ParquetProperties) :
            super(schema, pageWriteStore, props)

    constructor(
        schema: MessageType,
        pageWriteStore: PageWriteStore,
        bloomFilterWriteStore: BloomFilterWriteStore,
        props: ParquetProperties,
    ) : super(schema, pageWriteStore, bloomFilterWriteStore, props)

    override fun createColumnWriter(
        path: ColumnDescriptor,
        pageWriter: PageWriter,
        bloomFilterWriter: BloomFilterWriter?,
        props: ParquetProperties,
    ): ColumnWriterBase {
        return ColumnWriterV2(path, pageWriter, bloomFilterWriter, props)
    }
}
