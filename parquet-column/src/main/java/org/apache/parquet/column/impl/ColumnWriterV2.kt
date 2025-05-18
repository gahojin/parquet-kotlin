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

import okio.IOException
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.column.statistics.SizeStatistics
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter
import org.apache.parquet.io.ParquetEncodingException

/**
 * Writes (repetition level, definition level, value) triplets and deals with writing pages to the underlying layer.
 */
internal class ColumnWriterV2 : ColumnWriterBase {
    // Extending the original implementation to not to write the size of the data as the original writer would
    private class RLEWriterForV2(encoder: RunLengthBitPackingHybridEncoder) : RunLengthBitPackingHybridValuesWriter(encoder) {
        override val bytes: BytesInput
            get() = try {
                encoder.toBytes()
            } catch (e: IOException) {
                throw ParquetEncodingException(e)
            }
    }

    constructor(path: ColumnDescriptor, pageWriter: PageWriter, props: ParquetProperties) :
            super(path, pageWriter, props)

    constructor(
        path: ColumnDescriptor,
        pageWriter: PageWriter,
        bloomFilterWriter: BloomFilterWriter?,
        props: ParquetProperties,
    ) : super(path, pageWriter, bloomFilterWriter, props)

    override fun createRLWriter(props: ParquetProperties, path: ColumnDescriptor): ValuesWriter {
        return if (path.maxRepetitionLevel == 0) {
            NULL_WRITER
        } else {
            RLEWriterForV2(props.newRepetitionLevelEncoder(path))
        }
    }

    override fun createDLWriter(props: ParquetProperties, path: ColumnDescriptor): ValuesWriter {
        return if (path.maxDefinitionLevel == 0) {
            NULL_WRITER
        } else {
            RLEWriterForV2(props.newDefinitionLevelEncoder(path))
        }
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
        values: ValuesWriter,
    ) {
        // TODO: rework this API. The bytes shall be retrieved before the encoding (encoding might be different
        // otherwise)
        val bytes = values.bytes
        val encoding = values.encoding
        pageWriter.writePageV2(
            rowCount,
            Math.toIntExact(statistics.numNulls),
            valueCount,
            repetitionLevels.bytes,
            definitionLevels.bytes,
            encoding,
            bytes,
            statistics,
            sizeStatistics,
            geospatialStatistics,
        )
    }

    companion object {
        private val NULL_WRITER: ValuesWriter = DevNullValuesWriter
    }
}
