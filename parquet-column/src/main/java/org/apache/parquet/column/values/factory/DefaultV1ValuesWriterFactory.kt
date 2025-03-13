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
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter.LongByteStreamSplitValuesWriter
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter
import org.apache.parquet.column.values.plain.PlainValuesWriter
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

class DefaultV1ValuesWriterFactory : ValuesWriterFactory {
    private lateinit var parquetProperties: ParquetProperties

    private val encodingForDataPage: Encoding = Encoding.PLAIN_DICTIONARY

    private val encodingForDictionaryPage: Encoding = Encoding.PLAIN_DICTIONARY

    // no dictionary encoding for boolean
    private val booleanValuesWriter: ValuesWriter
        get() = BooleanPlainValuesWriter()

    override fun initialize(properties: ParquetProperties) {
        parquetProperties = properties
    }

    override fun newValuesWriter(descriptor: ColumnDescriptor): ValuesWriter {
        return when (descriptor.getType()) {
            PrimitiveTypeName.BOOLEAN -> booleanValuesWriter
            PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> getFixedLenByteArrayValuesWriter(descriptor)
            PrimitiveTypeName.BINARY -> getBinaryValuesWriter(descriptor)
            PrimitiveTypeName.INT32 -> getInt32ValuesWriter(descriptor)
            PrimitiveTypeName.INT64 -> getInt64ValuesWriter(descriptor)
            PrimitiveTypeName.INT96 -> getInt96ValuesWriter(descriptor)
            PrimitiveTypeName.DOUBLE -> getDoubleValuesWriter(descriptor)
            PrimitiveTypeName.FLOAT -> getFloatValuesWriter(descriptor)
        }
    }

    private fun getFixedLenByteArrayValuesWriter(path: ColumnDescriptor): ValuesWriter {
        // dictionary encoding was not enabled in PARQUET 1.0
        return if (parquetProperties.isByteStreamSplitEnabled(path)) {
            FixedLenByteArrayByteStreamSplitValuesWriter(
                path.primitiveType.typeLength,
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        } else {
            FixedLenByteArrayPlainValuesWriter(
                path.primitiveType.typeLength,
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        }
    }

    private fun getBinaryValuesWriter(path: ColumnDescriptor): ValuesWriter {
        val fallbackWriter: ValuesWriter = PlainValuesWriter(
            parquetProperties.initialSlabSize,
            parquetProperties.pageSizeThreshold,
            parquetProperties.allocator,
        )
        return DefaultValuesWriterFactory.dictWriterWithFallBack(
            path, parquetProperties, encodingForDictionaryPage, encodingForDataPage, fallbackWriter
        )
    }

    private fun getInt32ValuesWriter(path: ColumnDescriptor): ValuesWriter {
        val fallbackWriter: ValuesWriter
        if (parquetProperties.isByteStreamSplitEnabled(path)) {
            fallbackWriter = IntegerByteStreamSplitValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        } else {
            fallbackWriter = PlainValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        }
        return DefaultValuesWriterFactory.dictWriterWithFallBack(
            path, parquetProperties, encodingForDictionaryPage, encodingForDataPage, fallbackWriter
        )
    }

    private fun getInt64ValuesWriter(path: ColumnDescriptor): ValuesWriter {
        val fallbackWriter: ValuesWriter
        if (parquetProperties.isByteStreamSplitEnabled(path)) {
            fallbackWriter = LongByteStreamSplitValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        } else {
            fallbackWriter = PlainValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        }
        return DefaultValuesWriterFactory.dictWriterWithFallBack(
            path, parquetProperties, encodingForDictionaryPage, encodingForDataPage, fallbackWriter
        )
    }

    private fun getInt96ValuesWriter(path: ColumnDescriptor): ValuesWriter {
        val fallbackWriter: ValuesWriter = FixedLenByteArrayPlainValuesWriter(
            12,
            parquetProperties.initialSlabSize,
            parquetProperties.pageSizeThreshold,
            parquetProperties.allocator,
        )
        return DefaultValuesWriterFactory.dictWriterWithFallBack(
            path, parquetProperties, encodingForDictionaryPage, encodingForDataPage, fallbackWriter
        )
    }

    private fun getDoubleValuesWriter(path: ColumnDescriptor): ValuesWriter {
        val fallbackWriter: ValuesWriter
        if (parquetProperties.isByteStreamSplitEnabled(path)) {
            fallbackWriter = DoubleByteStreamSplitValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        } else {
            fallbackWriter = PlainValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        }
        return DefaultValuesWriterFactory.dictWriterWithFallBack(
            path, parquetProperties, encodingForDictionaryPage, encodingForDataPage, fallbackWriter
        )
    }

    private fun getFloatValuesWriter(path: ColumnDescriptor): ValuesWriter {
        val fallbackWriter: ValuesWriter
        if (parquetProperties.isByteStreamSplitEnabled(path)) {
            fallbackWriter = FloatByteStreamSplitValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        } else {
            fallbackWriter = PlainValuesWriter(
                parquetProperties.initialSlabSize,
                parquetProperties.pageSizeThreshold,
                parquetProperties.allocator,
            )
        }
        return DefaultValuesWriterFactory.dictWriterWithFallBack(
            path, parquetProperties, encodingForDictionaryPage, encodingForDataPage, fallbackWriter
        )
    }
}
