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
import org.apache.parquet.column.ColumnWriteStore
import org.apache.parquet.column.ColumnWriter
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.impl.StatusManager.Companion.create
import org.apache.parquet.column.page.PageWriteStore
import org.apache.parquet.column.page.PageWriter
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter
import org.apache.parquet.schema.MessageType
import java.util.TreeMap

/**
 * Base implementation for [ColumnWriteStore] to be extended to specialize for V1 and V2 pages.
 */
sealed class ColumnWriteStoreBase(
    private var rowCountForNextSizeCheck: Long,
    private val props: ParquetProperties,
) : ColumnWriteStore {
    // Used to support the deprecated workflow of ColumnWriteStoreV1 (lazy init of ColumnWriters)
    private fun interface ColumnWriterProvider {
        fun getColumnWriter(path: ColumnDescriptor): ColumnWriter?
    }

    private lateinit var columns: Map<ColumnDescriptor, ColumnWriterBase>
    private lateinit var columnWriterProvider: ColumnWriterProvider
    private val thresholdTolerance = (props.pageSizeThreshold * THRESHOLD_TOLERANCE_RATIO).toLong()
    private var rowCount: Long = 0
    private val statusManager = create()

    override val allocatedSize: Long
        get() = columns.values.sumOf { it.allocatedSize().toLong() }

    override val bufferedSize: Long
        get() = columns.values.sumOf { it.totalBufferedSize }

    override val isColumnFlushNeeded: Boolean
        get() = rowCount + 1 >= rowCountForNextSizeCheck

    val columnDescriptors: Set<ColumnDescriptor>
        get() = columns.keys

    // To be used by the deprecated constructor of ColumnWriteStoreV1
    @Deprecated("")
    constructor(pageWriteStore: PageWriteStore, props: ParquetProperties) : this(
        rowCountForNextSizeCheck = minOf(props.minRowCountForPageSizeCheck, props.pageRowCountLimit).toLong(),
        props = props,
    ) {
        val columns = TreeMap<ColumnDescriptor, ColumnWriterBase>()
        this.columns = columns
        columnWriterProvider = ColumnWriterProvider {
            columns.getOrPut(it) {
                createColumnWriterBase(it, pageWriteStore.getPageWriter(it), null, props)
            }
        }
    }

    constructor(schema: MessageType, pageWriteStore: PageWriteStore, props: ParquetProperties) : this(
        rowCountForNextSizeCheck = minOf(props.minRowCountForPageSizeCheck, props.pageRowCountLimit).toLong(),
        props = props,
    ) {
        columns = schema.columns.associateBy({ it }, {
            val pageWriter = pageWriteStore.getPageWriter(it)
            createColumnWriterBase(it, pageWriter, null, props)
        })

        columnWriterProvider = ColumnWriterProvider { columns[it] }
    }

    // The Bloom filter is written to a specified bitset instead of pages, so it needs a separate write store abstract.
    constructor(
        schema: MessageType,
        pageWriteStore: PageWriteStore,
        bloomFilterWriteStore: BloomFilterWriteStore,
        props: ParquetProperties,
    ) : this(
        rowCountForNextSizeCheck = props.minRowCountForPageSizeCheck.toLong(),
        props = props,
    ) {
        columns = schema.columns.associateBy({ it }, {
            val pageWriter = pageWriteStore.getPageWriter(it)
            if (props.isBloomFilterEnabled(it)) {
                val bloomFilterWriter = bloomFilterWriteStore.getBloomFilterWriter(it)
                createColumnWriterBase(it, pageWriter, bloomFilterWriter, props)
            } else {
                createColumnWriterBase(it, pageWriter, null, props)
            }
        })

        columnWriterProvider = ColumnWriterProvider { columns[it] }
    }

    private fun createColumnWriterBase(
        path: ColumnDescriptor,
        pageWriter: PageWriter,
        bloomFilterWriter: BloomFilterWriter?,
        props: ParquetProperties,
    ): ColumnWriterBase {
        return createColumnWriter(path, pageWriter, bloomFilterWriter, props).also {
            it.initStatusManager(statusManager)
        }
    }

    internal abstract fun createColumnWriter(
        path: ColumnDescriptor,
        pageWriter: PageWriter,
        bloomFilterWriter: BloomFilterWriter?,
        props: ParquetProperties,
    ): ColumnWriterBase

    override fun getColumnWriter(path: ColumnDescriptor): ColumnWriter {
        return columnWriterProvider.getColumnWriter(path)!!
    }

    override fun toString() = buildString {
        for (entry in columns.entries) {
            append(entry.key.path.contentToString()).append(": ")
            append(entry.value.totalBufferedSize).appendLine(" bytes")
        }
    }

    override fun flush() {
        for (memColumn in columns.values) {
            val rows = rowCount - memColumn.rowsWrittenSoFar
            if (rows > 0) {
                memColumn.writePage()
            }
            memColumn.finalizeColumnChunk()
        }
    }

    override fun memUsageString() = buildString {
        appendLine("Store {")
        for (memColumn in columns.values) {
            append(memColumn.memUsageString(" "))
        }
        appendLine('}')
    }

    fun maxColMemSize(): Long {
        return columns.values.maxOf { it.bufferedSizeInMemory }
    }

    override fun close() {
        flush() // calling flush() here to keep it consistent with the behavior before merging with master
        for (memColumn in columns.values) {
            memColumn.close()
        }
    }

    override fun endRecord() {
        ++rowCount
        if (rowCount >= rowCountForNextSizeCheck) {
            sizeCheck()
        }
    }

    private fun sizeCheck() {
        var minRecordToWait = Long.MAX_VALUE
        val pageRowCountLimit = props.pageRowCountLimit
        var rowCountForNextRowCountCheck = rowCount + pageRowCountLimit
        for (writer in columns.values) {
            val usedMem = writer.currentPageBufferedSize
            val rows = rowCount - writer.rowsWrittenSoFar
            var remainingMem = props.pageSizeThreshold - usedMem
            if (remainingMem <= thresholdTolerance || rows >= pageRowCountLimit || writer.valueCount >= props.pageValueCountThreshold) {
                writer.writePage()
                remainingMem = props.pageSizeThreshold.toLong()
            } else {
                rowCountForNextRowCountCheck = minOf(rowCountForNextRowCountCheck, writer.rowsWrittenSoFar + pageRowCountLimit)
            }
            // estimate remaining row count by previous input for next row count check
            val rowsToFillPage = if (usedMem == 0L) props.maxRowCountForPageSizeCheck.toLong() else rows * remainingMem / usedMem
            if (rowsToFillPage < minRecordToWait) {
                minRecordToWait = rowsToFillPage
            }
        }
        if (minRecordToWait == Long.Companion.MAX_VALUE) {
            minRecordToWait = props.minRowCountForPageSizeCheck.toLong()
        }

        if (props.estimateNextSizeCheck()) {
            // will check again halfway if between min and max
            rowCountForNextSizeCheck = rowCount + minOf(
                maxOf(minRecordToWait / 2, props.minRowCountForPageSizeCheck.toLong()),
                props.maxRowCountForPageSizeCheck.toLong())
        } else {
            rowCountForNextSizeCheck = rowCount + props.minRowCountForPageSizeCheck
        }

        // Do the check earlier if required to keep the row count limit
        if (rowCountForNextRowCountCheck < rowCountForNextSizeCheck) {
            rowCountForNextSizeCheck = rowCountForNextRowCountCheck
        }
    }

    companion object {
        // will flush even if size bellow the threshold by this much to facilitate page alignment
        private const val THRESHOLD_TOLERANCE_RATIO = 0.1f // 10 %
    }
}
