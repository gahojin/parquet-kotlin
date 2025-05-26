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

import org.apache.parquet.bytes.BytesInput.Companion.concat
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.column.statistics.SizeStatistics
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter
import java.io.IOException

/**
 * Writes (repetition level, definition level, value) triplets and deals with writing pages to the underlying layer.
 */
internal class ColumnWriterV1 : ColumnWriterBase {
    constructor(path: ColumnDescriptor, pageWriter: PageWriter, props: ParquetProperties) :
            super(path, pageWriter, props)

    constructor(
        path: ColumnDescriptor,
        pageWriter: PageWriter,
        bloomFilterWriter: BloomFilterWriter?,
        props: ParquetProperties,
    ) : super(path, pageWriter, bloomFilterWriter, props)

    override fun createRLWriter(props: ParquetProperties, path: ColumnDescriptor): ValuesWriter {
        return props.newRepetitionLevelWriter(path)
    }

    override fun createDLWriter(props: ParquetProperties, path: ColumnDescriptor): ValuesWriter {
        return props.newDefinitionLevelWriter(path)
    }

    @Throws(IOException::class)
    override fun writePage(
        rowCount: Int,
        valueCount: Int,
        statistics: Statistics<*>,
        sizeStatistics: SizeStatistics,
        geospatialStatistics: GeospatialStatistics,
        repetitionLevels: ValuesWriter,
        definitionLevels: ValuesWriter,
        values: ValuesWriter
    ) {
        pageWriter.writePage(
            concat(repetitionLevels.bytes, definitionLevels.bytes, values.bytes),
            valueCount,
            rowCount,
            statistics,
            sizeStatistics,
            geospatialStatistics,
            repetitionLevels.encoding,
            definitionLevels.encoding,
            values.encoding,
        )
    }
}
