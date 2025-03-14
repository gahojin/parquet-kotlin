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

import org.apache.parquet.bytes.ByteBufferAllocator
import org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt
import org.apache.parquet.bytes.CapacityByteArrayOutputStream.Companion.initialSlabSizeHeuristic
import org.apache.parquet.bytes.HeapByteBufferAllocator
import org.apache.parquet.column.impl.ColumnWriteStoreV1
import org.apache.parquet.column.impl.ColumnWriteStoreV2
import org.apache.parquet.column.page.PageWriteStore
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore
import org.apache.parquet.column.values.factory.DefaultValuesWriterFactory
import org.apache.parquet.column.values.factory.ValuesWriterFactory
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import java.util.OptionalDouble
import java.util.OptionalLong

/**
 * This class represents all the configurable Parquet properties.
 */
@ConsistentCopyVisibility
data class ParquetProperties private constructor(
    val initialSlabSize: Int,
    val pageSizeThreshold: Int,
    val pageValueCountThreshold: Int,
    val dictionaryPageSizeThreshold: Int,
    val writerVersion: WriterVersion,
    private val dictionaryEnabled: ColumnProperty<Boolean>,
    val minRowCountForPageSizeCheck: Int,
    val maxRowCountForPageSizeCheck: Int,
    private val estimateNextSizeCheck: Boolean,
    val allocator: ByteBufferAllocator,
    val valuesWriterFactory: ValuesWriterFactory,
    val columnIndexTruncateLength: Int,
    val statisticsTruncateLength: Int,
    private val statisticsEnabled: Boolean,
    private val sizeStatisticsEnabled: Boolean,

    // The expected NDV (number of distinct values) for each columns
    private val bloomFilterNDVs: ColumnProperty<Long?>,
    private val bloomFilterFPPs: ColumnProperty<Double>,
    val maxBloomFilterBytes: Int,
    private val bloomFilterEnabled: ColumnProperty<Boolean>,
    private val adaptiveBloomFilterEnabled: ColumnProperty<Boolean>,
    private val numBloomFilterCandidates: ColumnProperty<Int>,
    val pageRowCountLimit: Int,
    val pageWriteChecksumEnabled: Boolean,
    private val byteStreamSplitEnabled: ColumnProperty<ByteStreamSplitMode>,
    val extraMetaData: Map<String, String>,
    private val statistics: ColumnProperty<Boolean>,
    private val sizeStatistics: ColumnProperty<Boolean>,
) {
    internal enum class ByteStreamSplitMode {
        NONE,
        FLOATING_POINT,
        EXTENDED,
    }

    enum class WriterVersion(private val shortName: String) {
        PARQUET_1_0("v1"),
        PARQUET_2_0("v2"),
        ;

        companion object {
            @JvmStatic
            fun fromString(name: String?): WriterVersion {
                for (v in entries) {
                    if (v.shortName == name) {
                        return v
                    }
                }
                // Throws IllegalArgumentException if name does not exact match with enum name
                return WriterVersion.valueOf(name!!)
            }
        }
    }

    @get:Deprecated("")
    val isEnableDictionary: Boolean
        get() = dictionaryEnabled.defaultValue

    private constructor(builder: Builder) : this(
        pageValueCountThreshold = builder.pageValueCountThreshold,
        initialSlabSize = initialSlabSizeHeuristic(MIN_SLAB_SIZE, builder.pageSize, 10),
        pageSizeThreshold = builder.pageSize,
        dictionaryPageSizeThreshold = builder.dictPageSize,
        writerVersion = builder.writerVersion,
        dictionaryEnabled = builder.enableDict.build(),
        minRowCountForPageSizeCheck = builder.minRowCountForPageSizeCheck,
        maxRowCountForPageSizeCheck = builder.maxRowCountForPageSizeCheck,
        estimateNextSizeCheck = builder.estimateNextSizeCheck,
        allocator = builder.allocator,
        valuesWriterFactory = builder.valuesWriterFactory,
        columnIndexTruncateLength = builder.columnIndexTruncateLength,
        statisticsTruncateLength = builder.statisticsTruncateLength,
        statisticsEnabled = builder.statisticsEnabled,
        sizeStatisticsEnabled = builder.sizeStatisticsEnabled,
        bloomFilterNDVs = builder.bloomFilterNDVs.build(),
        bloomFilterFPPs = builder.bloomFilterFPPs.build(),
        bloomFilterEnabled = builder.bloomFilterEnabled.build(),
        maxBloomFilterBytes = builder.maxBloomFilterBytes,
        adaptiveBloomFilterEnabled = builder.adaptiveBloomFilterEnabled.build(),
        numBloomFilterCandidates = builder.numBloomFilterCandidates.build(),
        pageRowCountLimit = builder.pageRowCountLimit,
        pageWriteChecksumEnabled = builder.pageWriteChecksumEnabled,
        byteStreamSplitEnabled = builder.byteStreamSplitEnabled.build(),
        extraMetaData = builder.extraMetaData,
        statistics = builder.statistics.build(),
        sizeStatistics = builder.sizeStatistics.build(),
    )

    fun newRepetitionLevelWriter(path: ColumnDescriptor): ValuesWriter {
        return newColumnDescriptorValuesWriter(path.maxRepetitionLevel)
    }

    fun newDefinitionLevelWriter(path: ColumnDescriptor): ValuesWriter {
        return newColumnDescriptorValuesWriter(path.maxDefinitionLevel)
    }

    private fun newColumnDescriptorValuesWriter(maxLevel: Int): ValuesWriter {
        return if (maxLevel == 0) {
            DevNullValuesWriter
        } else {
            RunLengthBitPackingHybridValuesWriter(
                getWidthFromMaxInt(maxLevel), MIN_SLAB_SIZE, pageSizeThreshold, allocator
            )
        }
    }

    fun newRepetitionLevelEncoder(path: ColumnDescriptor): RunLengthBitPackingHybridEncoder {
        return newLevelEncoder(path.maxRepetitionLevel)
    }

    fun newDefinitionLevelEncoder(path: ColumnDescriptor): RunLengthBitPackingHybridEncoder {
        return newLevelEncoder(path.maxDefinitionLevel)
    }

    private fun newLevelEncoder(maxLevel: Int): RunLengthBitPackingHybridEncoder {
        return RunLengthBitPackingHybridEncoder(
            getWidthFromMaxInt(maxLevel), MIN_SLAB_SIZE, pageSizeThreshold, allocator
        )
    }

    fun newValuesWriter(path: ColumnDescriptor): ValuesWriter {
        return valuesWriterFactory.newValuesWriter(path)
    }

    fun isDictionaryEnabled(column: ColumnDescriptor): Boolean {
        return dictionaryEnabled.getValue(column)
    }

    @Deprecated("")
    fun isByteStreamSplitEnabled(): Boolean {
        return byteStreamSplitEnabled.defaultValue != ByteStreamSplitMode.NONE
    }

    fun isByteStreamSplitEnabled(column: ColumnDescriptor): Boolean {
        return when (column.primitiveType.primitiveTypeName) {
            PrimitiveTypeName.FLOAT, PrimitiveTypeName.DOUBLE -> byteStreamSplitEnabled.getValue(column) != ByteStreamSplitMode.NONE
            PrimitiveTypeName.INT32, PrimitiveTypeName.INT64, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> byteStreamSplitEnabled.getValue(column) == ByteStreamSplitMode.EXTENDED

            else -> false
        }
    }

    fun newColumnWriteStore(
        schema: MessageType,
        pageStore: PageWriteStore,
    ): ColumnWriteStore {
        return when (writerVersion) {
            WriterVersion.PARQUET_1_0 -> ColumnWriteStoreV1(schema, pageStore, this)
            WriterVersion.PARQUET_2_0 -> ColumnWriteStoreV2(schema, pageStore, this)
        }
    }

    fun newColumnWriteStore(
        schema: MessageType,
        pageStore: PageWriteStore,
        bloomFilterWriteStore: BloomFilterWriteStore,
    ): ColumnWriteStore {
        return when (writerVersion) {
            WriterVersion.PARQUET_1_0 -> ColumnWriteStoreV1(schema, pageStore, bloomFilterWriteStore, this)
            WriterVersion.PARQUET_2_0 -> ColumnWriteStoreV2(schema, pageStore, bloomFilterWriteStore, this)
        }
    }

    fun estimateNextSizeCheck(): Boolean {
        return estimateNextSizeCheck
    }

    fun getBloomFilterNDV(column: ColumnDescriptor): OptionalLong {
        val ndv = bloomFilterNDVs.getValue(column)
        return ndv?.let { OptionalLong.of(it) } ?: OptionalLong.empty()
    }

    fun getBloomFilterFPP(column: ColumnDescriptor): OptionalDouble {
        return OptionalDouble.of(bloomFilterFPPs.getValue(column))
    }

    fun isBloomFilterEnabled(column: ColumnDescriptor): Boolean {
        return bloomFilterEnabled.getValue(column)
    }

    fun getAdaptiveBloomFilterEnabled(column: ColumnDescriptor): Boolean {
        return adaptiveBloomFilterEnabled.getValue(column)
    }

    fun getBloomFilterCandidatesCount(column: ColumnDescriptor): Int {
        return numBloomFilterCandidates.getValue(column)
    }

    fun getStatisticsEnabled(column: ColumnDescriptor): Boolean {
        // First check column-specific setting
        return statistics.getValue(column)
    }

    fun getSizeStatisticsEnabled(column: ColumnDescriptor): Boolean {
        return sizeStatistics.getValue(column)
    }

    override fun toString(): String {
        return ("Parquet page size to " + pageSizeThreshold + '\n'
                + "Parquet dictionary page size to " + dictionaryPageSizeThreshold + '\n'
                + "Dictionary is " + dictionaryEnabled + '\n'
                + "Writer version is: " + writerVersion + '\n'
                + "Page size checking is: " + (if (estimateNextSizeCheck()) "estimated" else "constant") + '\n'
                + "Min row count for page size check is: " + minRowCountForPageSizeCheck + '\n'
                + "Max row count for page size check is: " + maxRowCountForPageSizeCheck + '\n'
                + "Truncate length for column indexes is: " + columnIndexTruncateLength + '\n'
                + "Truncate length for statistics min/max  is: " + statisticsTruncateLength + '\n'
                + "Bloom filter enabled: " + bloomFilterEnabled + '\n'
                + "Max Bloom filter size for a column is " + maxBloomFilterBytes + '\n'
                + "Bloom filter expected number of distinct values are: " + bloomFilterNDVs + '\n'
                + "Bloom filter false positive probabilities are: " + bloomFilterFPPs + '\n'
                + "Page row count limit to " + pageRowCountLimit + '\n'
                + "Writing page checksums is: " + (if (pageWriteChecksumEnabled) "on" else "off") + '\n'
                + "Statistics enabled: " + statisticsEnabled + '\n'
                + "Size statistics enabled: " + sizeStatisticsEnabled)
    }

    class Builder {
        internal var pageSize: Int = DEFAULT_PAGE_SIZE
        internal var dictPageSize: Int = DEFAULT_DICTIONARY_PAGE_SIZE
        internal val enableDict: ColumnProperty.Builder<Boolean>
        internal var writerVersion: WriterVersion = DEFAULT_WRITER_VERSION
        internal var minRowCountForPageSizeCheck: Int = DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK
        internal var maxRowCountForPageSizeCheck: Int = DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK
        internal var pageValueCountThreshold: Int = DEFAULT_PAGE_VALUE_COUNT_THRESHOLD
        internal var estimateNextSizeCheck: Boolean = DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK
        internal var allocator: ByteBufferAllocator = HeapByteBufferAllocator.INSTANCE
        internal var valuesWriterFactory: ValuesWriterFactory = DEFAULT_VALUES_WRITER_FACTORY
        internal var columnIndexTruncateLength: Int = DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH
        internal var statisticsTruncateLength: Int = DEFAULT_STATISTICS_TRUNCATE_LENGTH
        internal var statisticsEnabled: Boolean = DEFAULT_STATISTICS_ENABLED
        internal val sizeStatisticsEnabled: Boolean = DEFAULT_SIZE_STATISTICS_ENABLED
        internal val bloomFilterNDVs: ColumnProperty.Builder<Long?>
        internal val bloomFilterFPPs: ColumnProperty.Builder<Double>
        internal var maxBloomFilterBytes: Int = DEFAULT_MAX_BLOOM_FILTER_BYTES
        internal val adaptiveBloomFilterEnabled: ColumnProperty.Builder<Boolean>
        internal val numBloomFilterCandidates: ColumnProperty.Builder<Int>
        internal val bloomFilterEnabled: ColumnProperty.Builder<Boolean>
        internal var pageRowCountLimit: Int = DEFAULT_PAGE_ROW_COUNT_LIMIT
        internal var pageWriteChecksumEnabled: Boolean = DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED
        internal val byteStreamSplitEnabled: ColumnProperty.Builder<ByteStreamSplitMode>
        internal var extraMetaData: Map<String, String> = emptyMap()
        internal val statistics: ColumnProperty.Builder<Boolean>
        internal val sizeStatistics: ColumnProperty.Builder<Boolean>

        internal constructor() {
            enableDict = ColumnProperty.builder<Boolean>(DEFAULT_IS_DICTIONARY_ENABLED)
            byteStreamSplitEnabled = ColumnProperty.builder<ByteStreamSplitMode> {
                if (DEFAULT_IS_BYTE_STREAM_SPLIT_ENABLED)
                    ByteStreamSplitMode.FLOATING_POINT
                else
                    ByteStreamSplitMode.NONE
            }
            bloomFilterEnabled = ColumnProperty.builder<Boolean>(DEFAULT_BLOOM_FILTER_ENABLED)
            bloomFilterNDVs = ColumnProperty.builder<Long?>(null)
            bloomFilterFPPs = ColumnProperty.builder<Double>(DEFAULT_BLOOM_FILTER_FPP)
            adaptiveBloomFilterEnabled = ColumnProperty.builder<Boolean>(DEFAULT_ADAPTIVE_BLOOM_FILTER_ENABLED)
            numBloomFilterCandidates = ColumnProperty.builder<Int>(DEFAULT_BLOOM_FILTER_CANDIDATES_NUMBER)
            statistics = ColumnProperty.builder<Boolean>(DEFAULT_STATISTICS_ENABLED)
            sizeStatistics = ColumnProperty.builder<Boolean>(DEFAULT_SIZE_STATISTICS_ENABLED)
        }

        internal constructor(toCopy: ParquetProperties) {
            this.pageSize = toCopy.pageSizeThreshold
            this.enableDict = ColumnProperty.builder<Boolean>(toCopy.dictionaryEnabled)
            this.dictPageSize = toCopy.dictionaryPageSizeThreshold
            this.writerVersion = toCopy.writerVersion
            this.minRowCountForPageSizeCheck = toCopy.minRowCountForPageSizeCheck
            this.maxRowCountForPageSizeCheck = toCopy.maxRowCountForPageSizeCheck
            this.estimateNextSizeCheck = toCopy.estimateNextSizeCheck
            this.valuesWriterFactory = toCopy.valuesWriterFactory
            this.allocator = toCopy.allocator
            this.pageRowCountLimit = toCopy.pageRowCountLimit
            this.pageWriteChecksumEnabled = toCopy.pageWriteChecksumEnabled
            this.bloomFilterNDVs = ColumnProperty.builder<Long?>(toCopy.bloomFilterNDVs)
            this.bloomFilterFPPs = ColumnProperty.builder<Double>(toCopy.bloomFilterFPPs)
            this.bloomFilterEnabled = ColumnProperty.builder<Boolean>(toCopy.bloomFilterEnabled)
            this.adaptiveBloomFilterEnabled = ColumnProperty.builder<Boolean>(toCopy.adaptiveBloomFilterEnabled)
            this.numBloomFilterCandidates = ColumnProperty.builder<Int>(toCopy.numBloomFilterCandidates)
            this.maxBloomFilterBytes = toCopy.maxBloomFilterBytes
            this.byteStreamSplitEnabled = ColumnProperty.builder<ByteStreamSplitMode>(toCopy.byteStreamSplitEnabled)
            this.extraMetaData = toCopy.extraMetaData
            this.statistics = ColumnProperty.builder<Boolean>(toCopy.statistics)
            this.sizeStatistics = ColumnProperty.builder<Boolean>(toCopy.sizeStatistics)
        }

        /**
         * Set the Parquet format page size.
         *
         * @param pageSize an integer size in bytes
         * @return this builder for method chaining.
         */
        fun withPageSize(pageSize: Int): Builder = apply {
            require(pageSize > 0) { "Invalid page size (negative): $pageSize" }
            this.pageSize = pageSize
        }

        /**
         * Enable or disable dictionary encoding.
         *
         * @param enableDictionary whether dictionary encoding should be enabled
         * @return this builder for method chaining.
         */
        fun withDictionaryEncoding(enableDictionary: Boolean): Builder = apply {
            enableDict.withDefaultValue(enableDictionary)
        }

        /**
         * Enable or disable dictionary encoding for the specified column.
         *
         * @param columnPath       the path of the column (dot-string)
         * @param enableDictionary whether dictionary encoding should be enabled
         * @return this builder for method chaining.
         */
        fun withDictionaryEncoding(columnPath: String, enableDictionary: Boolean): Builder = apply {
            enableDict.withValue(columnPath, enableDictionary)
        }

        /**
         * Enable or disable BYTE_STREAM_SPLIT encoding for FLOAT and DOUBLE columns.
         *
         * @param enable whether BYTE_STREAM_SPLIT encoding should be enabled
         * @return this builder for method chaining.
         */
        fun withByteStreamSplitEncoding(enable: Boolean): Builder = apply {
            val value = if (enable) ByteStreamSplitMode.FLOATING_POINT else ByteStreamSplitMode.NONE
            byteStreamSplitEnabled.withDefaultValue(value)
        }

        /**
         * Enable or disable BYTE_STREAM_SPLIT encoding for specified columns.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enable     whether BYTE_STREAM_SPLIT encoding should be enabled
         * @return this builder for method chaining.
         */
        fun withByteStreamSplitEncoding(columnPath: String, enable: Boolean): Builder = apply {
            val value = if (enable) ByteStreamSplitMode.EXTENDED else ByteStreamSplitMode.NONE
            this.byteStreamSplitEnabled.withValue(columnPath, value)
        }

        /**
         * Enable or disable BYTE_STREAM_SPLIT encoding for FLOAT, DOUBLE, INT32, INT64 and FIXED_LEN_BYTE_ARRAY columns.
         *
         * @param enable whether BYTE_STREAM_SPLIT encoding should be enabled
         * @return this builder for method chaining.
         */
        fun withExtendedByteStreamSplitEncoding(enable: Boolean): Builder = apply {
            val value = if (enable) ByteStreamSplitMode.EXTENDED else ByteStreamSplitMode.NONE
            byteStreamSplitEnabled.withDefaultValue(value)
        }

        /**
         * Set the Parquet format dictionary page size.
         *
         * @param dictionaryPageSize an integer size in bytes
         * @return this builder for method chaining.
         */
        fun withDictionaryPageSize(dictionaryPageSize: Int): Builder = apply {
            require(dictionaryPageSize > 0) { "Invalid dictionary page size (negative): $dictionaryPageSize" }
            dictPageSize = dictionaryPageSize
        }

        /**
         * Set the [format version][WriterVersion].
         *
         * @param version a `WriterVersion`
         * @return this builder for method chaining.
         */
        fun withWriterVersion(version: WriterVersion): Builder = apply {
            writerVersion = version
        }

        fun withMinRowCountForPageSizeCheck(min: Int): Builder = apply {
            require(min > 0) { "Invalid row count for page size check (negative): $min" }
            minRowCountForPageSizeCheck = min
        }

        fun withMaxRowCountForPageSizeCheck(max: Int): Builder = apply {
            require(max > 0) { "Invalid row count for page size check (negative): $max" }
            maxRowCountForPageSizeCheck = max
        }

        fun withPageValueCountThreshold(value: Int): Builder = apply {
            require(value > 0) { "Invalid page value count threshold (negative): $value" }
            pageValueCountThreshold = value
        }

        // Do not attempt to predict next size check.  Prevents issues with rows that vary significantly in size.
        fun estimateRowCountForPageSizeCheck(estimateNextSizeCheck: Boolean): Builder = apply {
            this.estimateNextSizeCheck = estimateNextSizeCheck
        }

        fun withAllocator(allocator: ByteBufferAllocator): Builder = apply {
            this.allocator = allocator
        }

        fun withValuesWriterFactory(factory: ValuesWriterFactory): Builder = apply {
            valuesWriterFactory = factory
        }

        fun withColumnIndexTruncateLength(length: Int): Builder = apply {
            require(length > 0) { "Invalid column index min/max truncate length (negative or zero) : $length" }
            columnIndexTruncateLength = length
        }

        fun withStatisticsTruncateLength(length: Int): Builder = apply {
            require(length > 0) { "Invalid statistics min/max truncate length (negative or zero) : $length"  }
            statisticsTruncateLength = length
        }

        /**
         * Set max Bloom filter bytes for related columns.
         *
         * @param maxBloomFilterBytes the max bytes of a Bloom filter bitset for a column.
         * @return this builder for method chaining
         */
        fun withMaxBloomFilterBytes(maxBloomFilterBytes: Int): Builder = apply {
            this.maxBloomFilterBytes = maxBloomFilterBytes
        }

        /**
         * Set Bloom filter NDV (number of distinct values) for the specified column.
         * If set for a column then the writing of the bloom filter for that column will be automatically enabled (see
         * [.withBloomFilterEnabled]).
         *
         * @param columnPath the path of the column (dot-string)
         * @param ndv        the NDV of the column
         * @return this builder for method chaining
         */
        fun withBloomFilterNDV(columnPath: String, ndv: Long): Builder = apply {
            require(ndv > 0) { "Invalid NDV for column \"$columnPath\": $ndv" }
            bloomFilterNDVs.withValue(columnPath, ndv)
            // Setting an NDV for a column implies writing a bloom filter
            bloomFilterEnabled.withValue(columnPath, true)
        }

        fun withBloomFilterFPP(columnPath: String, fpp: Double): Builder = apply {
            require(fpp > 0.0 && fpp < 1.0) { "Invalid FPP for column \"$columnPath\": $fpp" }
            bloomFilterFPPs.withValue(columnPath, fpp)
        }

        /**
         * Enable or disable the bloom filter for the columns not specified by
         * [.withBloomFilterEnabled].
         *
         * @param enabled whether bloom filter shall be enabled for all columns
         * @return this builder for method chaining
         */
        fun withBloomFilterEnabled(enabled: Boolean): Builder = apply {
            bloomFilterEnabled.withDefaultValue(enabled)
        }

        /**
         * Whether to use adaptive bloom filter to automatically adjust the bloom filter size according to
         * `parquet.bloom.filter.max.bytes`.
         * If NDV (number of distinct values) for a specified column is set, it will be ignored
         *
         * @param enabled whether to use adaptive bloom filter
         */
        fun withAdaptiveBloomFilterEnabled(enabled: Boolean): Builder = apply {
            adaptiveBloomFilterEnabled.withDefaultValue(enabled)
        }

        /**
         * When `AdaptiveBloomFilter` is enabled, set how many bloom filter candidates to use.
         *
         * @param columnPath the path of the column (dot-string)
         * @param number     the number of candidates
         */
        fun withBloomFilterCandidatesNumber(columnPath: String, number: Int): Builder = apply {
            require(number > 0) { "Invalid candidates number for column \"$columnPath\": $number" }
            numBloomFilterCandidates.withDefaultValue(number)
        }

        /**
         * Enable or disable the bloom filter for the specified column.
         * One may either disable bloom filters for all columns by invoking [.withBloomFilterEnabled] with a
         * `false` value and then enable the bloom filters for the required columns one-by-one by invoking this
         * method or vice versa.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enabled    whether bloom filter shall be enabled
         * @return this builder for method chaining
         */
        fun withBloomFilterEnabled(columnPath: String, enabled: Boolean): Builder = apply {
            bloomFilterEnabled.withValue(columnPath, enabled)
        }

        fun withPageRowCountLimit(rowCount: Int): Builder = apply {
            require(rowCount > 0) { "Invalid row count limit for pages: $rowCount" }
            pageRowCountLimit = rowCount
        }

        fun withPageWriteChecksumEnabled(value: Boolean): Builder = apply {
            pageWriteChecksumEnabled = value
        }

        fun withExtraMetaData(extraMetaData: Map<String, String>): Builder = apply {
            this.extraMetaData = extraMetaData
        }

        /**
         * Enable or disable the statistics for given column. All column statistics are enabled by default.
         *
         * @param columnPath the given column
         * @param enabled enable or disable
         * @return this builder for method chaining
         */
        fun withStatisticsEnabled(columnPath: String, enabled: Boolean): Builder = apply {
            statistics.withValue(columnPath, enabled)
        }

        fun withStatisticsEnabled(enabled: Boolean): Builder = apply {
            statistics.withDefaultValue(enabled)
            statisticsEnabled = enabled
        }

        /**
         * Sets whether size statistics are enabled globally. When disabled, size statistics will not be collected
         * for any column unless explicitly enabled for specific columns.
         *
         * @param enabled whether to collect size statistics globally
         * @return this builder for method chaining
         */
        fun withSizeStatisticsEnabled(enabled: Boolean): Builder = apply {
            sizeStatistics.withDefaultValue(enabled)
        }

        /**
         * Sets the size statistics enabled/disabled for the specified column. All column size statistics are enabled by default.
         *
         * @param columnPath the path of the column (dot-string)
         * @param enabled    whether to collect size statistics for the column
         * @return this builder for method chaining
         */
        fun withSizeStatisticsEnabled(columnPath: String, enabled: Boolean): Builder = apply {
            sizeStatistics.withValue(columnPath, enabled)
        }

        fun build(): ParquetProperties {
            val properties = ParquetProperties(this)
            // we pass a constructed but uninitialized factory to ParquetProperties above as currently
            // creation of ValuesWriters is invoked from within ParquetProperties. In the future
            // we'd like to decouple that and won't need to pass an object to properties and then pass the
            // properties to the object.
            valuesWriterFactory.initialize(properties)

            return properties
        }
    }

    companion object {
        const val DEFAULT_PAGE_SIZE: Int = 1024 * 1024
        const val DEFAULT_DICTIONARY_PAGE_SIZE: Int = DEFAULT_PAGE_SIZE
        const val DEFAULT_IS_DICTIONARY_ENABLED: Boolean = true
        const val DEFAULT_IS_BYTE_STREAM_SPLIT_ENABLED: Boolean = false
        @JvmField val DEFAULT_WRITER_VERSION: WriterVersion = WriterVersion.PARQUET_1_0
        const val DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK: Boolean = true
        const val DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK: Int = 100
        const val DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK: Int = 10000
        const val DEFAULT_PAGE_VALUE_COUNT_THRESHOLD: Int = Int.Companion.MAX_VALUE / 2
        const val DEFAULT_COLUMN_INDEX_TRUNCATE_LENGTH: Int = 64
        const val DEFAULT_STATISTICS_TRUNCATE_LENGTH: Int = Int.Companion.MAX_VALUE
        const val DEFAULT_PAGE_ROW_COUNT_LIMIT: Int = 20000
        const val DEFAULT_MAX_BLOOM_FILTER_BYTES: Int = 1024 * 1024
        const val DEFAULT_BLOOM_FILTER_ENABLED: Boolean = false
        const val DEFAULT_BLOOM_FILTER_FPP: Double = 0.01
        const val DEFAULT_ADAPTIVE_BLOOM_FILTER_ENABLED: Boolean = false
        const val DEFAULT_BLOOM_FILTER_CANDIDATES_NUMBER: Int = 5
        const val DEFAULT_STATISTICS_ENABLED: Boolean = true
        const val DEFAULT_SIZE_STATISTICS_ENABLED: Boolean = true

        const val DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED: Boolean = true

        @JvmField val DEFAULT_VALUES_WRITER_FACTORY: ValuesWriterFactory = DefaultValuesWriterFactory()

        private const val MIN_SLAB_SIZE = 64

        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }

        @JvmStatic
        fun copy(toCopy: ParquetProperties): Builder {
            return Builder(toCopy)
        }
    }
}
