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
import org.apache.parquet.column.statistics.SizeStatistics
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.column.values.bloomfilter.AdaptiveBlockSplitBloomFilter
import org.apache.parquet.column.values.bloomfilter.BlockSplitBloomFilter
import org.apache.parquet.column.values.bloomfilter.BloomFilter
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter
import org.apache.parquet.io.api.Binary
import java.io.IOException
import java.io.OutputStream

// An internal class to collect column values to build column statistics and bloom filter.
internal class ColumnValueCollector(
    private val path: ColumnDescriptor,
    bloomFilterWriter: BloomFilterWriter?,
    props: ParquetProperties
) {
    private val statisticsEnabled: Boolean = props.getStatisticsEnabled(path)
    private val sizeStatisticsEnabled: Boolean = props.getSizeStatisticsEnabled(path)
    private var bloomFilterWriter: BloomFilterWriter? = null
    private lateinit var bloomFilter: BloomFilter
    lateinit var statistics: Statistics<*>
        private set
    private lateinit var sizeStatisticsBuilder: SizeStatistics.Builder
    val sizeStatistics: SizeStatistics
        get() = sizeStatisticsBuilder.build()

    init {
        resetPageStatistics()
        initBloomFilter(bloomFilterWriter, props)
    }

    fun resetPageStatistics() {
        statistics = if (statisticsEnabled) {
            Statistics.createStats(path.primitiveType)
        } else {
            Statistics.noopStats(path.primitiveType)
        }
        sizeStatisticsBuilder = if (sizeStatisticsEnabled) {
            SizeStatistics.newBuilder(path.primitiveType, path.maxRepetitionLevel, path.maxDefinitionLevel)
        } else {
            SizeStatistics.noopBuilder(path.primitiveType, path.maxRepetitionLevel, path.maxDefinitionLevel)
        }
    }

    fun writeNull(repetitionLevel: Int, definitionLevel: Int) {
        statistics.incrementNumNulls()
        sizeStatisticsBuilder.add(repetitionLevel, definitionLevel)
    }

    fun write(value: Boolean, repetitionLevel: Int, definitionLevel: Int) {
        statistics.updateStats(value)
        sizeStatisticsBuilder.add(repetitionLevel, definitionLevel)
    }

    fun write(value: Int, repetitionLevel: Int, definitionLevel: Int) {
        statistics.updateStats(value)
        sizeStatisticsBuilder.add(repetitionLevel, definitionLevel)
        bloomFilter.insertHash(bloomFilter.hash(value))
    }

    fun write(value: Long, repetitionLevel: Int, definitionLevel: Int) {
        statistics.updateStats(value)
        sizeStatisticsBuilder.add(repetitionLevel, definitionLevel)
        bloomFilter.insertHash(bloomFilter.hash(value))
    }

    fun write(value: Float, repetitionLevel: Int, definitionLevel: Int) {
        statistics.updateStats(value)
        sizeStatisticsBuilder.add(repetitionLevel, definitionLevel)
        bloomFilter.insertHash(bloomFilter.hash(value))
    }

    fun write(value: Double, repetitionLevel: Int, definitionLevel: Int) {
        statistics.updateStats(value)
        sizeStatisticsBuilder.add(repetitionLevel, definitionLevel)
        bloomFilter.insertHash(bloomFilter.hash(value))
    }

    fun write(value: Binary, repetitionLevel: Int, definitionLevel: Int) {
        statistics.updateStats(value)
        sizeStatisticsBuilder.add(repetitionLevel, definitionLevel, value)
        bloomFilter.insertHash(bloomFilter.hash(value))
    }

    fun initBloomFilter(bloomFilterWriter: BloomFilterWriter?, props: ParquetProperties) {
        this.bloomFilterWriter = bloomFilterWriter ?: run {
            bloomFilter = object : BloomFilter {
                override val bitsetSize = 0

                override val hashStrategy: BloomFilter.HashStrategy? = null

                override val algorithm: BloomFilter.Algorithm? = null

                override val compression: BloomFilter.Compression? = null

                override fun equals(`object`: Any?) = false

                @Throws(IOException::class)
                override fun writeTo(out: OutputStream) = Unit

                override fun insertHash(hash: Long) = Unit

                override fun findHash(hash: Long) = false

                override fun hash(value: Int) = 0L

                override fun hash(value: Long) = 0L

                override fun hash(value: Double) = 0L

                override fun hash(value: Float) = 0L

                override fun hash(value: Binary) = 0L

                override fun hash(value: Any) = 0L
            }
            return
        }

        val maxBloomFilterSize = props.maxBloomFilterBytes
        val ndv = props.getBloomFilterNDV(path)
        val fpp = props.getBloomFilterFPP(path)
        // If user specify the column NDV, we construct Bloom filter from it.
        bloomFilter = if (ndv.isPresent) {
            val optimalNumOfBits = BlockSplitBloomFilter.optimalNumOfBits(ndv.getAsLong(), fpp.getAsDouble())
            BlockSplitBloomFilter(optimalNumOfBits / 8, maxBloomFilterSize)
        } else if (props.getAdaptiveBloomFilterEnabled(path)) {
            val numCandidates = props.getBloomFilterCandidatesCount(path)
            AdaptiveBlockSplitBloomFilter(maxBloomFilterSize, numCandidates, fpp.getAsDouble(), path)
        } else {
            BlockSplitBloomFilter(maxBloomFilterSize, maxBloomFilterSize)
        }
    }

    fun finalizeColumnChunk() {
        bloomFilterWriter?.writeBloomFilter(bloomFilter)
    }
}
