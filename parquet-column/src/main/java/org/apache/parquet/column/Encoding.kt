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
package org.apache.parquet.column

import org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader
import org.apache.parquet.column.values.bitpacking.Packer
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForDouble
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFLBA
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFloat
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForInteger
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForLong
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainBinaryDictionary
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainDoubleDictionary
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainFloatDictionary
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainIntegerDictionary
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainLongDictionary
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader
import org.apache.parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader
import org.apache.parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader
import org.apache.parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader
import org.apache.parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader
import org.apache.parquet.column.values.rle.ZeroIntegerValuesReader
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import java.io.IOException

/**
 * encoding of the data
 */
enum class Encoding {
    PLAIN {
        override fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
            return when (descriptor.getType()) {
                PrimitiveTypeName.BOOLEAN -> BooleanPlainValuesReader()
                PrimitiveTypeName.BINARY -> BinaryPlainValuesReader()
                PrimitiveTypeName.FLOAT -> FloatPlainValuesReader()
                PrimitiveTypeName.DOUBLE -> DoublePlainValuesReader()
                PrimitiveTypeName.INT32 -> IntegerPlainValuesReader()
                PrimitiveTypeName.INT64 -> LongPlainValuesReader()
                PrimitiveTypeName.INT96 -> FixedLenByteArrayPlainValuesReader(12)
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> FixedLenByteArrayPlainValuesReader(descriptor.typeLength)
            }
        }

        @Throws(IOException::class)
        override fun initDictionary(descriptor: ColumnDescriptor, dictionaryPage: DictionaryPage): Dictionary {
            return when (descriptor.getType()) {
                PrimitiveTypeName.BINARY -> PlainBinaryDictionary(dictionaryPage)
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> PlainBinaryDictionary(dictionaryPage, descriptor.typeLength)
                PrimitiveTypeName.INT96 -> PlainBinaryDictionary(dictionaryPage, 12)
                PrimitiveTypeName.INT64 -> PlainLongDictionary(dictionaryPage)
                PrimitiveTypeName.DOUBLE -> PlainDoubleDictionary(dictionaryPage)
                PrimitiveTypeName.INT32 -> PlainIntegerDictionary(dictionaryPage)
                PrimitiveTypeName.FLOAT -> PlainFloatDictionary(dictionaryPage)
                else -> throw ParquetDecodingException("Dictionary encoding not supported for type: ${descriptor.getType()}")
            }
        }
    },

    /**
     * Actually a combination of bit packing and run length encoding.
     * TODO: Should we rename this to be more clear?
     */
    RLE {
        override fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
            val bitWidth = getWidthFromMaxInt(getMaxLevel(descriptor, valuesType))
            if (bitWidth == 0) {
                return ZeroIntegerValuesReader()
            }
            return RunLengthBitPackingHybridValuesReader(bitWidth)
        }
    },

    BYTE_STREAM_SPLIT {
        override fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
            return when (descriptor.getType()) {
                PrimitiveTypeName.FLOAT -> ByteStreamSplitValuesReaderForFloat()
                PrimitiveTypeName.DOUBLE -> ByteStreamSplitValuesReaderForDouble()
                PrimitiveTypeName.INT32 -> ByteStreamSplitValuesReaderForInteger()
                PrimitiveTypeName.INT64 -> ByteStreamSplitValuesReaderForLong()
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> ByteStreamSplitValuesReaderForFLBA(descriptor.typeLength)
                else -> throw ParquetDecodingException("no byte stream split reader for type ${descriptor.getType()}")
            }
        }
    },

    @Deprecated(
        """This is no longer used, and has been replaced by {@link #RLE}
    which is combination of bit packing and rle"""
    )
    BIT_PACKED {
        override fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
            return ByteBitPackingValuesReader(getMaxLevel(descriptor, valuesType), Packer.BIG_ENDIAN)
        }
    },

    @Deprecated("now replaced by RLE_DICTIONARY for the data page encoding and PLAIN for the dictionary page encoding")
    PLAIN_DICTIONARY {
        override fun getDictionaryBasedValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType, dictionary: Dictionary): ValuesReader {
            return RLE_DICTIONARY.getDictionaryBasedValuesReader(descriptor, valuesType, dictionary)
        }

        @Throws(IOException::class)
        override fun initDictionary(descriptor: ColumnDescriptor, dictionaryPage: DictionaryPage): Dictionary {
            return PLAIN.initDictionary(descriptor, dictionaryPage)
        }

        override fun usesDictionary() = true
    },

    /**
     * Delta encoding for integers. This can be used for int columns and works best
     * on sorted data
     */
    DELTA_BINARY_PACKED {
        override fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
            if (descriptor.getType() !== PrimitiveTypeName.INT32 && descriptor.getType() !== PrimitiveTypeName.INT64) {
                throw ParquetDecodingException("Encoding DELTA_BINARY_PACKED is only supported for type INT32 and INT64")
            }
            return DeltaBinaryPackingValuesReader()
        }
    },

    /**
     * Encoding for byte arrays to separate the length values and the data. The lengths
     * are encoded using DELTA_BINARY_PACKED
     */
    DELTA_LENGTH_BYTE_ARRAY {
        override fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
            if (descriptor.getType() !== PrimitiveTypeName.BINARY) {
                throw ParquetDecodingException("Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY")
            }
            return DeltaLengthByteArrayValuesReader()
        }
    },

    /**
     * Incremental-encoded byte array. Prefix lengths are encoded using DELTA_BINARY_PACKED.
     * Suffixes are stored as delta length byte arrays.
     */
    DELTA_BYTE_ARRAY {
        override fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
            if (descriptor.getType() !== PrimitiveTypeName.BINARY && descriptor.getType() !== PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                throw ParquetDecodingException("Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY")
            }
            return DeltaByteArrayReader()
        }
    },

    /**
     * Dictionary encoding: the ids are encoded using the RLE encoding
     */
    RLE_DICTIONARY {
        override fun getDictionaryBasedValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType, dictionary: Dictionary): ValuesReader {
            return when (descriptor.getType()) {
                PrimitiveTypeName.BINARY,
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                PrimitiveTypeName.INT96,
                PrimitiveTypeName.INT64,
                PrimitiveTypeName.DOUBLE,
                PrimitiveTypeName.INT32,
                PrimitiveTypeName.FLOAT -> DictionaryValuesReader(dictionary)

                else -> throw ParquetDecodingException("Dictionary encoding not supported for type: ${descriptor.getType()}")
            }
        }

        override fun usesDictionary() = true
    };

    fun getMaxLevel(descriptor: ColumnDescriptor, valuesType: ValuesType): Int {
        val maxLevel: Int
        when (valuesType) {
            ValuesType.REPETITION_LEVEL -> maxLevel = descriptor.maxRepetitionLevel
            ValuesType.DEFINITION_LEVEL -> maxLevel = descriptor.maxDefinitionLevel
            ValuesType.VALUES -> {
                if (descriptor.getType() != PrimitiveTypeName.BOOLEAN) {
                    throw ParquetDecodingException("Unsupported encoding for values: $this")
                }
                maxLevel = 1
            }
        }
        return maxLevel
    }

    /**
     * @return whether this encoding requires a dictionary
     */
    open fun usesDictionary(): Boolean = false

    /**
     * initializes a dictionary from a page
     *
     * @param descriptor     the column descriptor for the dictionary-encoded column
     * @param dictionaryPage a dictionary page
     * @return the corresponding dictionary
     * @throws IOException                   if there is an exception while reading the dictionary page
     * @throws UnsupportedOperationException if the encoding is not dictionary based
     */
    @Throws(IOException::class)
    open fun initDictionary(descriptor: ColumnDescriptor, dictionaryPage: DictionaryPage): Dictionary {
        throw UnsupportedOperationException("$name does not support dictionary")
    }

    /**
     * To read decoded values that don't require a dictionary
     *
     * @param descriptor the column to read
     * @param valuesType the type of values
     * @return the proper values reader for the given column
     * @throws UnsupportedOperationException if the encoding is dictionary based
     */
    open fun getValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType): ValuesReader {
        throw UnsupportedOperationException("Error decoding $descriptor. $name is dictionary based")
    }

    /**
     * To read decoded values that require a dictionary
     *
     * @param descriptor the column to read
     * @param valuesType the type of values
     * @param dictionary the dictionary
     * @return the proper values reader for the given column
     * @throws UnsupportedOperationException if the encoding is not dictionary based
     */
    open fun getDictionaryBasedValuesReader(descriptor: ColumnDescriptor, valuesType: ValuesType, dictionary: Dictionary): ValuesReader {
        throw UnsupportedOperationException("$name is not dictionary based")
    }
}
