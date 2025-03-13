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
package org.apache.parquet.column.values.factory

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.ParquetProperties.WriterVersion
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter
import org.apache.parquet.column.values.fallback.FallbackValuesWriter.Companion.of
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
 * Handles ValuesWriter creation statically based on the types of the columns and the writer version.
 */
class DefaultValuesWriterFactory : ValuesWriterFactory {
    private lateinit var delegateFactory: ValuesWriterFactory

    override fun initialize(properties: ParquetProperties) {
        delegateFactory = if (properties.writerVersion == WriterVersion.PARQUET_1_0) {
            DEFAULT_V1_WRITER_FACTORY
        } else {
            DEFAULT_V2_WRITER_FACTORY
        }.also {
            it.initialize(properties)
        }
    }

    override fun newValuesWriter(descriptor: ColumnDescriptor): ValuesWriter {
        return delegateFactory.newValuesWriter(descriptor)
    }

    companion object {
        private val DEFAULT_V1_WRITER_FACTORY = DefaultV1ValuesWriterFactory()
        private val DEFAULT_V2_WRITER_FACTORY = DefaultV2ValuesWriterFactory()

        fun dictionaryWriter(
            path: ColumnDescriptor,
            properties: ParquetProperties,
            dictPageEncoding: Encoding,
            dataPageEncoding: Encoding,
        ): DictionaryValuesWriter {
            return when (path.getType()) {
                PrimitiveTypeName.BOOLEAN -> throw IllegalArgumentException("no dictionary encoding for BOOLEAN")
                PrimitiveTypeName.BINARY -> PlainBinaryDictionaryValuesWriter(
                    properties.dictionaryPageSizeThreshold,
                    dataPageEncoding,
                    dictPageEncoding,
                    properties.allocator,
                )

                PrimitiveTypeName.INT32 -> PlainIntegerDictionaryValuesWriter(
                    properties.dictionaryPageSizeThreshold,
                    dataPageEncoding,
                    dictPageEncoding,
                    properties.allocator,
                )

                PrimitiveTypeName.INT64 -> PlainLongDictionaryValuesWriter(
                    properties.dictionaryPageSizeThreshold,
                    dataPageEncoding,
                    dictPageEncoding,
                    properties.allocator,
                )

                PrimitiveTypeName.INT96 -> PlainFixedLenArrayDictionaryValuesWriter(
                    properties.dictionaryPageSizeThreshold,
                    12,
                    dataPageEncoding,
                    dictPageEncoding,
                    properties.allocator,
                )

                PrimitiveTypeName.DOUBLE -> PlainDoubleDictionaryValuesWriter(
                    properties.dictionaryPageSizeThreshold,
                    dataPageEncoding,
                    dictPageEncoding,
                    properties.allocator,
                )

                PrimitiveTypeName.FLOAT -> PlainFloatDictionaryValuesWriter(
                    properties.dictionaryPageSizeThreshold,
                    dataPageEncoding,
                    dictPageEncoding,
                    properties.allocator,
                )

                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> PlainFixedLenArrayDictionaryValuesWriter(
                    properties.dictionaryPageSizeThreshold,
                    path.typeLength,
                    dataPageEncoding,
                    dictPageEncoding,
                    properties.allocator,
                )
            }
        }

        @JvmStatic
        fun dictWriterWithFallBack(
            path: ColumnDescriptor,
            parquetProperties: ParquetProperties,
            dictPageEncoding: Encoding,
            dataPageEncoding: Encoding,
            writerToFallBackTo: ValuesWriter,
        ): ValuesWriter {
            return if (parquetProperties.isDictionaryEnabled(path)) {
                of(dictionaryWriter(path, parquetProperties, dictPageEncoding, dataPageEncoding), writerToFallBackTo)
            } else {
                writerToFallBackTo
            }
        }
    }
}
