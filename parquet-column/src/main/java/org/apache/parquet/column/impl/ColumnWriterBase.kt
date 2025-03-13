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
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ColumnWriter
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.impl.StatusManager.Companion.create
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.column.statistics.SizeStatistics
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter
import org.apache.parquet.io.ParquetEncodingException
import org.apache.parquet.io.api.Binary
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Base implementation for [ColumnWriter] to be extended to specialize for V1 and V2 pages.
 */
internal abstract class ColumnWriterBase(
    val path: ColumnDescriptor,
    @JvmField val pageWriter: PageWriter,
    bloomFilterWriter: BloomFilterWriter?,
    props: ParquetProperties,
) : ColumnWriter {
    private val repetitionLevelColumn = createRLWriter(props, path)
    private val definitionLevelColumn = createDLWriter(props, path)
    private val dataColumn = props.newValuesWriter(path)
    var valueCount: Int = 0
        private set

    var rowsWrittenSoFar: Long = 0
        private set
    private var pageRowCount = 0
    private var statusManager = create()

    private val collector = ColumnValueCollector(path, bloomFilterWriter, props)

    override val bufferedSizeInMemory: Long
        get() = repetitionLevelColumn.bufferedSize + definitionLevelColumn.bufferedSize + dataColumn.bufferedSize + pageWriter.memSize

    /**
     * Used to decide when to write a page
     *
     * @return the number of bytes of memory used to buffer the current data
     */
    val currentPageBufferedSize: Long
        get() = repetitionLevelColumn.bufferedSize + definitionLevelColumn.bufferedSize + dataColumn.bufferedSize

    /**
     * Used to decide when to write a page or row group
     *
     * @return the number of bytes of memory used to buffer the current data and the previously written pages
     */
    val totalBufferedSize: Long
        get() = repetitionLevelColumn.bufferedSize + definitionLevelColumn.bufferedSize + dataColumn.bufferedSize + pageWriter.memSize

    constructor(path: ColumnDescriptor, pageWriter: PageWriter, props: ParquetProperties) :
            this(path, pageWriter, null, props)

    fun initStatusManager(statusManager: StatusManager) {
        this.statusManager = statusManager
    }

    abstract fun createRLWriter(props: ParquetProperties, path: ColumnDescriptor): ValuesWriter

    abstract fun createDLWriter(props: ParquetProperties, path: ColumnDescriptor): ValuesWriter

    private fun log(value: Any?, r: Int, d: Int) {
        LOG.debug("{} {} r:{} d:{}", path, value, r, d)
    }

    private fun definitionLevel(definitionLevel: Int) {
        definitionLevelColumn.writeInteger(definitionLevel)
    }

    private fun repetitionLevel(repetitionLevel: Int) {
        repetitionLevelColumn.writeInteger(repetitionLevel)
        assert(if (pageRowCount == 0) repetitionLevel == 0 else true) { "Every page shall start on record boundaries" }
        if (repetitionLevel == 0) {
            ++pageRowCount
        }
    }

    /**
     * Writes the current null value
     *
     * @param repetitionLevel
     * @param definitionLevel
     */
    override fun writeNull(repetitionLevel: Int, definitionLevel: Int) {
        if (DEBUG) log(null, repetitionLevel, definitionLevel)
        try {
            repetitionLevel(repetitionLevel)
            definitionLevel(definitionLevel)
            collector.writeNull(repetitionLevel, definitionLevel)
            ++valueCount
        } catch (e: Throwable) {
            statusManager.abort()
            throw e
        }
    }

    override fun close() {
        // Close the Values writers.
        repetitionLevelColumn.close()
        definitionLevelColumn.close()
        dataColumn.close()
    }

    /**
     * Writes the current value
     *
     * @param value
     * @param repetitionLevel
     * @param definitionLevel
     */
    override fun write(value: Double, repetitionLevel: Int, definitionLevel: Int) {
        if (DEBUG) log(value, repetitionLevel, definitionLevel)
        try {
            repetitionLevel(repetitionLevel)
            definitionLevel(definitionLevel)
            dataColumn.writeDouble(value)
            collector.write(value, repetitionLevel, definitionLevel)
            ++valueCount
        } catch (e: Throwable) {
            statusManager.abort()
            throw e
        }
    }

    /**
     * Writes the current value
     *
     * @param value
     * @param repetitionLevel
     * @param definitionLevel
     */
    override fun write(value: Float, repetitionLevel: Int, definitionLevel: Int) {
        if (DEBUG) log(value, repetitionLevel, definitionLevel)
        try {
            repetitionLevel(repetitionLevel)
            definitionLevel(definitionLevel)
            dataColumn.writeFloat(value)
            collector.write(value, repetitionLevel, definitionLevel)
            ++valueCount
        } catch (e: Throwable) {
            statusManager.abort()
            throw e
        }
    }

    /**
     * Writes the current value
     *
     * @param value
     * @param repetitionLevel
     * @param definitionLevel
     */
    override fun write(value: Binary, repetitionLevel: Int, definitionLevel: Int) {
        if (DEBUG) log(value, repetitionLevel, definitionLevel)
        try {
            repetitionLevel(repetitionLevel)
            definitionLevel(definitionLevel)
            dataColumn.writeBytes(value)
            collector.write(value, repetitionLevel, definitionLevel)
            ++valueCount
        } catch (e: Throwable) {
            statusManager.abort()
            throw e
        }
    }

    /**
     * Writes the current value
     *
     * @param value
     * @param repetitionLevel
     * @param definitionLevel
     */
    override fun write(value: Boolean, repetitionLevel: Int, definitionLevel: Int) {
        if (DEBUG) log(value, repetitionLevel, definitionLevel)
        try {
            repetitionLevel(repetitionLevel)
            definitionLevel(definitionLevel)
            dataColumn.writeBoolean(value)
            collector.write(value, repetitionLevel, definitionLevel)
            ++valueCount
        } catch (e: Throwable) {
            statusManager.abort()
            throw e
        }
    }

    /**
     * Writes the current value
     *
     * @param value
     * @param repetitionLevel
     * @param definitionLevel
     */
    override fun write(value: Int, repetitionLevel: Int, definitionLevel: Int) {
        if (DEBUG) log(value, repetitionLevel, definitionLevel)
        try {
            repetitionLevel(repetitionLevel)
            definitionLevel(definitionLevel)
            dataColumn.writeInteger(value)
            collector.write(value, repetitionLevel, definitionLevel)
            ++valueCount
        } catch (e: Throwable) {
            statusManager.abort()
            throw e
        }
    }

    /**
     * Writes the current value
     *
     * @param value
     * @param repetitionLevel
     * @param definitionLevel
     */
    override fun write(value: Long, repetitionLevel: Int, definitionLevel: Int) {
        if (DEBUG) log(value, repetitionLevel, definitionLevel)
        try {
            repetitionLevel(repetitionLevel)
            definitionLevel(definitionLevel)
            dataColumn.writeLong(value)
            collector.write(value, repetitionLevel, definitionLevel)
            ++valueCount
        } catch (e: Throwable) {
            statusManager.abort()
            throw e
        }
    }

    /**
     * Finalizes the Column chunk. Possibly adding extra pages if needed (dictionary, ...)
     * Is called right after writePage
     */
    fun finalizeColumnChunk() {
        if (statusManager.isAborted) {
            // We are aborting -> nothing to be done
            return
        }
        try {
            val dictionaryPage = dataColumn.toDictPageAndClose()
            if (dictionaryPage != null) {
                if (DEBUG) LOG.debug("write dictionary")
                try {
                    pageWriter.writeDictionaryPage(dictionaryPage)
                } catch (e: IOException) {
                    throw ParquetEncodingException("could not write dictionary page for $path", e)
                }
                dataColumn.resetDictionary()
            }

            collector.finalizeColumnChunk()
        } catch (t: Throwable) {
            statusManager.abort()
            throw t
        }
    }

    /**
     * @return actual memory used
     */
    fun allocatedSize(): Long {
        return repetitionLevelColumn.allocatedSize + definitionLevelColumn.allocatedSize + dataColumn.allocatedSize + pageWriter.allocatedSize()
    }

    /**
     * @param indent a prefix to format lines
     * @return a formatted string showing how memory is used
     */
    fun memUsageString(indent: String) = buildString {
        append(indent).append(path).appendLine(" {")
        append(indent)
            .append(" r:")
            .append(repetitionLevelColumn.allocatedSize)
            .appendLine(" bytes")
        append(indent)
            .append(" d:")
            .append(definitionLevelColumn.allocatedSize)
            .appendLine(" bytes")
        appendLine(dataColumn.memUsageString("$indent  data:"))
        appendLine(pageWriter.memUsageString("$indent  pages:"))
        append(indent)
            .appendLine("  total: %,d/%,d".format(totalBufferedSize, allocatedSize()))
        append(indent).appendLine('}')
    }

    /**
     * Writes the current data to a new page in the page store
     */
    fun writePage() {
        if (valueCount == 0) {
            throw ParquetEncodingException("writing empty page")
        }
        if (statusManager.isAborted) {
            // We are aborting -> nothing to be done
            return
        }
        try {
            rowsWrittenSoFar += pageRowCount.toLong()
            if (DEBUG) LOG.debug("write page")
            try {
                writePage(
                    pageRowCount,
                    valueCount,
                    collector.statistics,
                    collector.sizeStatistics,
                    repetitionLevelColumn,
                    definitionLevelColumn,
                    dataColumn,
                )
            } catch (e: IOException) {
                throw ParquetEncodingException("could not write page for $path", e)
            }
            repetitionLevelColumn.reset()
            definitionLevelColumn.reset()
            dataColumn.reset()
            valueCount = 0
            collector.resetPageStatistics()
            pageRowCount = 0
        } catch (t: Throwable) {
            statusManager.abort()
            throw t
        }
    }

    @Throws(IOException::class)
    abstract fun writePage(
        rowCount: Int,
        valueCount: Int,
        statistics: Statistics<*>,
        sizeStatistics: SizeStatistics,
        repetitionLevels: ValuesWriter,
        definitionLevels: ValuesWriter,
        values: ValuesWriter,
    )

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(ColumnWriterBase::class.java)

        // By default: Debugging disabled this way (using the "if (DEBUG)" IN the methods) to allow
        // the java compiler (not the JIT) to remove the unused statements during build time.
        private const val DEBUG = false
    }
}
