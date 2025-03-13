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
import org.apache.parquet.CorruptDeltaByteArrays.requiresSequentialReads
import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.column.Dictionary
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.ValuesType
import org.apache.parquet.column.page.DataPage
import org.apache.parquet.column.page.DataPageV1
import org.apache.parquet.column.page.DataPageV2
import org.apache.parquet.column.page.PageReader
import org.apache.parquet.column.values.RequiresPreviousReader
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeNameConverter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Base superclass for [ColumnReader] implementations.
 *
 * @param path          the descriptor for the corresponding column
 * @param pageReader    the underlying store to read from
 * @param converter     a converter that materializes the values in this column in the current record
 * @param writerVersion writer version string from the Parquet file being read
 */
internal abstract class ColumnReaderBase(
    path: ColumnDescriptor,
    private val pageReader: PageReader,
    private val converter: PrimitiveConverter,
    private val writerVersion: ParsedVersion?,
) : ColumnReader {
    /**
     * binds the lower level page decoder to the record converter materializing the records
     */
    private abstract class Binding {
        /**
         * @return current value
         */
        open val integer: Int
            get() = throw UnsupportedOperationException()

        /**
         * @return current value
         */
        open val boolean: Boolean
            get() = throw UnsupportedOperationException()

        /**
         * @return current value
         */
        open val long: Long
            get() = throw UnsupportedOperationException()

        /**
         * @return current value
         */
        open val binary: Binary
            get() = throw UnsupportedOperationException()

        /**
         * @return current value
         */
        open val float: Float
            get() = throw UnsupportedOperationException()

        /**
         * @return current value
         */
        open val double: Double
            get() = throw UnsupportedOperationException()

        /**
         * read one value from the underlying page
         */
        abstract fun read()

        /**
         * skip one value from the underlying page
         */
        abstract fun skip()

        /**
         * Skips n values from the underlying page
         *
         * @param n the number of values to be skipped
         */
        abstract fun skip(n: Int)

        /**
         * write current value to converter
         */
        abstract fun writeValue()

        /**
         * @return current value
         */
        open fun getDictionaryId(): Int {
            throw UnsupportedOperationException()
        }
    }

    override val descriptor: ColumnDescriptor = path

    @get:Deprecated("")
    override val totalValueCount: Long = pageReader.totalValueCount
    private val dictionary: Dictionary? = pageReader.readDictionaryPage()?.let { dictionaryPage ->
        try {
            dictionaryPage.encoding.initDictionary(path, dictionaryPage).also {
                if (converter.hasDictionarySupport()) {
                    converter.setDictionary(it)
                }
            }
        } catch (e: IOException) {
            throw ParquetDecodingException("could not decode the dictionary for $path", e)
        }
    }

    private lateinit var repetitionLevelColumn: IntIterator
    private lateinit var definitionLevelColumn: IntIterator
    protected lateinit var dataColumn: ValuesReader
    private var currentEncoding: Encoding? = null

    final override var currentRepetitionLevel: Int = 0
        private set

    final override var currentDefinitionLevel: Int = 0
        private set
    private var dictionaryId = 0

    private var endOfPageValueCount: Long = 0
    private var readValues: Long = 0
    var pageValueCount: Int = 0
        private set

    private lateinit var binding: Binding
    private val maxDefinitionLevel: Int = path.maxDefinitionLevel

    // this is needed because we will attempt to read the value twice when filtering
    // TODO: rework that
    private var valueRead = false

    internal open val isFullyConsumed: Boolean
        get() = readValues >= totalValueCount

    override val currentValueDictionaryID: Int
        get() {
            readValue()
            return binding.getDictionaryId()
        }

    override val integer: Int
        get() {
            readValue()
            return binding.integer
        }

    override val boolean: Boolean
        get() {
            readValue()
            return binding.boolean
        }

    override val long: Long
        get() {
            readValue()
            return binding.long
        }

    override val binary: Binary
        get() {
            readValue()
            return binding.binary
        }

    override val float: Float
        get() {
            readValue()
            return binding.float
        }

    override val double: Double
        get() {
            readValue()
            return binding.double
        }

    internal open val isPageFullyConsumed: Boolean
        get() = readValues >= endOfPageValueCount

    /**
     * creates a reader for triplets
     *
     */
    init {
        if (totalValueCount <= 0) {
            throw ParquetDecodingException("totalValueCount '$totalValueCount' <= 0")
        }
    }

    private fun bindToDictionary(dictionary: Dictionary) {
        binding = object : Binding() {
            override val integer: Int
                get() = dictionary.decodeToInt(dictionaryId)

            override val boolean: Boolean
                get() = dictionary.decodeToBoolean(dictionaryId)

            override val long: Long
                get() = dictionary.decodeToLong(dictionaryId)

            override val binary: Binary
                get() = dictionary.decodeToBinary(dictionaryId)

            override val float: Float
                get() = dictionary.decodeToFloat(dictionaryId)

            override val double: Double
                get() = dictionary.decodeToDouble(dictionaryId)

            override fun read() {
                dictionaryId = dataColumn.readValueDictionaryId()
            }

            override fun skip() {
                dataColumn.skip()
            }

            override fun skip(n: Int) {
                dataColumn.skip(n)
            }

            override fun getDictionaryId(): Int {
                return dictionaryId
            }

            override fun writeValue() {
                converter.addValueFromDictionary(dictionaryId)
            }
        }
    }

    private fun bind(type: PrimitiveTypeName) {
        binding = type.convert(object : PrimitiveTypeNameConverter<Binding, RuntimeException> {
            @Throws(RuntimeException::class)
            override fun convertFLOAT(primitiveTypeName: PrimitiveTypeName): Binding {
                return object : Binding() {
                    var current: Float = 0f

                    override val float: Float
                        get() = current

                    override fun read() {
                        current = dataColumn.readFloat()
                    }

                    override fun skip() {
                        current = 0f
                        dataColumn.skip()
                    }

                    override fun skip(n: Int) {
                        current = 0f
                        dataColumn.skip(n)
                    }

                    override fun writeValue() {
                        converter.addFloat(current)
                    }
                }
            }

            @Throws(RuntimeException::class)
            override fun convertDOUBLE(primitiveTypeName: PrimitiveTypeName): Binding {
                return object : Binding() {
                    var current: Double = 0.0

                    override val double: Double
                        get() = current

                    override fun read() {
                        current = dataColumn!!.readDouble()
                    }

                    override fun skip() {
                        current = 0.0
                        dataColumn!!.skip()
                    }

                    override fun skip(n: Int) {
                        current = 0.0
                        dataColumn!!.skip(n)
                    }

                    override fun writeValue() {
                        converter.addDouble(current)
                    }
                }
            }

            @Throws(RuntimeException::class)
            override fun convertINT32(primitiveTypeName: PrimitiveTypeName): Binding {
                return object : Binding() {
                    var current: Int = 0

                    override val integer: Int
                        get() = current

                    override fun read() {
                        current = dataColumn!!.readInteger()
                    }

                    override fun skip() {
                        current = 0
                        dataColumn!!.skip()
                    }

                    override fun skip(n: Int) {
                        current = 0
                        dataColumn!!.skip(n)
                    }

                    override fun writeValue() {
                        converter.addInt(current)
                    }
                }
            }

            @Throws(RuntimeException::class)
            override fun convertINT64(primitiveTypeName: PrimitiveTypeName): Binding {
                return object : Binding() {
                    var current: Long = 0L

                    override val long: Long
                        get() = current

                    override fun read() {
                        current = dataColumn.readLong()
                    }

                    override fun skip() {
                        current = 0
                        dataColumn.skip()
                    }

                    override fun skip(n: Int) {
                        current = 0
                        dataColumn.skip(n)
                    }

                    override fun writeValue() {
                        converter.addLong(current)
                    }
                }
            }

            @Throws(RuntimeException::class)
            override fun convertINT96(primitiveTypeName: PrimitiveTypeName): Binding {
                return convertBINARY(primitiveTypeName)
            }

            @Throws(RuntimeException::class)
            override fun convertFIXED_LEN_BYTE_ARRAY(primitiveTypeName: PrimitiveTypeName): Binding {
                return convertBINARY(primitiveTypeName)
            }

            @Throws(RuntimeException::class)
            override fun convertBOOLEAN(primitiveTypeName: PrimitiveTypeName): Binding {
                return object : Binding() {
                    var current: Boolean = false

                    override val boolean: Boolean
                        get() = current

                    override fun read() {
                        current = dataColumn.readBoolean()
                    }

                    override fun skip() {
                        current = false
                        dataColumn.skip()
                    }

                    override fun skip(n: Int) {
                        current = false
                        dataColumn.skip(n)
                    }

                    override fun writeValue() {
                        converter.addBoolean(current)
                    }
                }
            }

            @Throws(RuntimeException::class)
            override fun convertBINARY(primitiveTypeName: PrimitiveTypeName): Binding {
                return object : Binding() {
                    var current: Binary? = null

                    override val binary: Binary
                        get() = requireNotNull(current)

                    override fun read() {
                        current = dataColumn.readBytes()
                    }

                    override fun skip() {
                        current = null
                        dataColumn.skip()
                    }

                    override fun skip(n: Int) {
                        current = null
                        dataColumn.skip(n)
                    }

                    override fun writeValue() {
                        converter.addBinary(requireNotNull(current))
                    }
                }
            }
        })
    }

    override fun writeCurrentValueToConverter() {
        readValue()
        binding.writeValue()
    }

    /**
     * Reads the value into the binding.
     */
    fun readValue() {
        try {
            if (!valueRead) {
                binding.read()
                valueRead = true
            }
        } catch (e: RuntimeException) {
            if (requiresSequentialReads(writerVersion, currentEncoding) && e is ArrayIndexOutOfBoundsException) {
                // this is probably PARQUET-246, which may happen if reading data with
                // MR because this can't be detected without reading all footers
                throw ParquetDecodingException(
                    "Read failure possibly due to " + "PARQUET-246: try setting parquet.split.files to false",
                    ParquetDecodingException(
                        "Can't read value in column %s at value %d out of %d, %d out of %d in currentPage. repetition level: %d, definition level: %d".format(
                            descriptor,
                            readValues,
                            totalValueCount,
                            readValues - (endOfPageValueCount - pageValueCount),
                            pageValueCount,
                            currentRepetitionLevel,
                            currentDefinitionLevel,
                        ),
                        e,
                    )
                )
            }
            throw ParquetDecodingException(
                "Can't read value in column %s at value %d out of %d, %d out of %d in currentPage. repetition level: %d, definition level: %d".format(
                    descriptor,
                    readValues,
                    totalValueCount,
                    readValues - (endOfPageValueCount - pageValueCount),
                    pageValueCount,
                    currentRepetitionLevel,
                    currentDefinitionLevel,
                ),
                e,
            )
        }
    }

    override fun skip() {
        if (!valueRead) {
            binding.skip()
            valueRead = true
        }
    }

    private fun checkRead() {
        var rl: Int
        var dl: Int
        var skipValues = 0
        while (true) {
            if (isPageFullyConsumed) {
                if (isFullyConsumed) {
                    LOG.debug("end reached")
                    currentRepetitionLevel = 0 // the next repetition level
                    return
                }
                readPage()
                skipValues = 0
            }
            rl = repetitionLevelColumn.nextInt()
            dl = definitionLevelColumn.nextInt()
            ++readValues
            if (!skipRL(rl)) {
                break
            }
            if (dl == maxDefinitionLevel) {
                ++skipValues
            }
        }
        binding.skip(skipValues)
        currentRepetitionLevel = rl
        currentDefinitionLevel = dl
    }

    /*
     * Returns if current levels / value shall be skipped based on the specified repetition level.
     */
    internal abstract fun skipRL(rl: Int): Boolean

    private fun readPage() {
        LOG.debug("loading page")
        val page = pageReader.readPage()
        page.accept(object : DataPage.Visitor<Void?> {
            override fun visit(dataPageV1: DataPageV1): Void? {
                readPageV1(dataPageV1)
                return null
            }

            override fun visit(dataPageV2: DataPageV2): Void? {
                readPageV2(dataPageV2)
                return null
            }
        })
    }

    private fun initDataReader(dataEncoding: Encoding, `in`: ByteBufferInputStream, valueCount: Int) {
        val previousReader = if (::dataColumn.isInitialized) dataColumn else null

        currentEncoding = dataEncoding
        pageValueCount = valueCount
        endOfPageValueCount = readValues + pageValueCount

        val dataColumn = if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw ParquetDecodingException(
                    ("could not read page in col $descriptor as the dictionary was missing for encoding $dataEncoding")
                )
            }
            dataEncoding.getDictionaryBasedValuesReader(descriptor, ValuesType.VALUES, dictionary)
        } else {
            dataEncoding.getValuesReader(descriptor, ValuesType.VALUES)
        }
        this.dataColumn = dataColumn

        if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
            bindToDictionary(dictionary!!)
        } else {
            bind(descriptor.getType())
        }

        try {
            dataColumn.initFromPage(pageValueCount, `in`)
        } catch (e: IOException) {
            throw ParquetDecodingException("could not read page in col $descriptor", e)
        }

        if (requiresSequentialReads(writerVersion, dataEncoding) && dataColumn is RequiresPreviousReader) {
            // previous reader can only be set if reading sequentially
            (dataColumn as RequiresPreviousReader).setPreviousReader(previousReader)
        }
    }

    private fun readPageV1(page: DataPageV1) {
        val rlReader = page.rlEncoding.getValuesReader(descriptor, ValuesType.REPETITION_LEVEL)
        val dlReader = page.dlEncoding.getValuesReader(descriptor, ValuesType.DEFINITION_LEVEL)
        repetitionLevelColumn = ValuesReaderIntIterator(rlReader)
        definitionLevelColumn = ValuesReaderIntIterator(dlReader)
        val valueCount = page.valueCount
        try {
            val bytes = page.bytes
            LOG.debug("page size {} bytes and {} values", bytes.size(), valueCount)
            LOG.debug("reading repetition levels at 0")
            val `in` = bytes.toInputStream()
            rlReader.initFromPage(valueCount, `in`)
            LOG.debug("reading definition levels at {}", `in`.position())
            dlReader.initFromPage(valueCount, `in`)
            LOG.debug("reading data at {}", `in`.position())
            initDataReader(page.valueEncoding, `in`, valueCount)
        } catch (e: IOException) {
            throw ParquetDecodingException("could not read page $page in col $descriptor", e)
        }
        newPageInitialized(page)
    }

    private fun readPageV2(page: DataPageV2) {
        repetitionLevelColumn = newRLEIterator(descriptor.maxRepetitionLevel, page.repetitionLevels)
        definitionLevelColumn = newRLEIterator(descriptor.maxDefinitionLevel, page.definitionLevels)
        val valueCount = page.valueCount
        LOG.debug("page data size {} bytes and {} values", page.data.size(), valueCount)
        try {
            initDataReader(page.dataEncoding, page.data.toInputStream(), valueCount)
        } catch (e: IOException) {
            throw ParquetDecodingException("could not read page $page in col $descriptor", e)
        }
        newPageInitialized(page)
    }

    protected abstract fun newPageInitialized(page: DataPage)

    private fun newRLEIterator(maxLevel: Int, bytes: BytesInput): IntIterator {
        try {
            if (maxLevel == 0) {
                return NullIntIterator()
            }
            return RLEIntIterator(
                RunLengthBitPackingHybridDecoder(
                    getWidthFromMaxInt(maxLevel), bytes.toInputStream()
                )
            )
        } catch (e: IOException) {
            throw ParquetDecodingException("could not read levels in page for col $descriptor", e)
        }
    }

    override fun consume() {
        checkRead()
        valueRead = false
    }

    internal abstract class IntIterator {
        abstract fun nextInt(): Int
    }

    internal class ValuesReaderIntIterator(var delegate: ValuesReader) : IntIterator() {
        override fun nextInt(): Int {
            return delegate.readInteger()
        }
    }

    internal class RLEIntIterator(var delegate: RunLengthBitPackingHybridDecoder) : IntIterator() {
        override fun nextInt(): Int {
            try {
                return delegate.readInt()
            } catch (e: IOException) {
                throw ParquetDecodingException(e)
            }
        }
    }

    private class NullIntIterator : IntIterator() {
        override fun nextInt(): Int {
            return 0
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(ColumnReaderBase::class.java)
    }
}
