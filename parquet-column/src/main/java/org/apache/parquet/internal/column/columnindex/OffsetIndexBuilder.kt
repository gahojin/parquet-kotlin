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
package org.apache.parquet.internal.column.columnindex

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.ints.IntList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.longs.LongList
import java.util.Formatter
import java.util.Optional
import java.util.OptionalLong

/**
 * Builder implementation to create [OffsetIndex] objects during writing a parquet file.
 */
open class OffsetIndexBuilder private constructor() {
    private class OffsetIndexImpl(
        var offsets: LongArray,
        var compressedPageSizes: IntArray,
        var firstRowIndexes: LongArray,
        var unencodedByteArrayDataBytes: LongArray?,
    ) : OffsetIndex {
        override val pageCount: Int
            get() = offsets.size

        override fun getOffset(pageIndex: Int): Long {
            return offsets[pageIndex]
        }

        override fun getCompressedPageSize(pageIndex: Int): Int {
            return compressedPageSizes[pageIndex]
        }

        override fun getFirstRowIndex(pageIndex: Int): Long {
            return firstRowIndexes[pageIndex]
        }

        override fun getPageOrdinal(pageIndex: Int): Int {
            return pageIndex
        }

        override fun getUnencodedByteArrayDataBytes(pageIndex: Int): Optional<Long> {
            if (unencodedByteArrayDataBytes == null || unencodedByteArrayDataBytes!!.isEmpty()) {
                return Optional.empty<Long>()
            }
            return Optional.of(unencodedByteArrayDataBytes!![pageIndex])
        }

        override fun toString(): String {
            Formatter().use { formatter ->
                formatter.format(
                    "%-10s  %20s  %20s  %20s  %20s\n",
                    "", "offset", "compressed size", "first row index", "unencoded bytes"
                )
                var i = 0
                val n = offsets.size
                while (i < n) {
                    val unencodedBytes = unencodedByteArrayDataBytes?.let {
                        if (it.isEmpty()) null else it[i].toString()
                    } ?: "-"
                    formatter.format(
                        "page-%-5d  %20d  %20d  %20d  %20s\n",
                        i, offsets[i], compressedPageSizes[i], firstRowIndexes[i], unencodedBytes
                    )
                    ++i
                }
                return formatter.toString()
            }
        }
    }

    private val offsets: LongList = LongArrayList()
    private val compressedPageSizes: IntList = IntArrayList()
    private val firstRowIndexes: LongList = LongArrayList()
    private val unencodedDataBytes: LongList = LongArrayList()
    private var previousOffset: Long = 0
    private var previousPageSize = 0
    private var previousRowIndex: Long = 0
    private var previousRowCount: Long = 0

    /**
     * Adds the specified parameters to this builder. Used by the writers to building up [OffsetIndex] objects to be
     * written to the Parquet file.
     *
     * @param compressedPageSize the size of the page (including header)
     * @param rowCount           the number of rows in the page
     */
    open fun add(compressedPageSize: Int, rowCount: Long) {
        add(compressedPageSize, rowCount, OptionalLong.empty())
    }

    /**
     * Adds the specified parameters to this builder. Used by the writers to building up [OffsetIndex] objects to be
     * written to the Parquet file.
     *
     * @param compressedPageSize the size of the page (including header)
     * @param rowCount the number of rows in the page
     * @param unencodedDataBytes the number of bytes of unencoded data of BYTE_ARRAY type
     */
    fun add(compressedPageSize: Int, rowCount: Long, unencodedDataBytes: OptionalLong) {
        add(previousOffset + previousPageSize, compressedPageSize, previousRowIndex + previousRowCount, unencodedDataBytes)
        previousRowCount = rowCount
    }

    /**
     * Adds the specified parameters to this builder. Used by the metadata converter to building up [OffsetIndex]
     * objects read from the Parquet file.
     *
     * @param offset             the offset of the page in the file
     * @param compressedPageSize the size of the page (including header)
     * @param firstRowIndex      the index of the first row in the page (within the row group)
     */
    open fun add(offset: Long, compressedPageSize: Int, firstRowIndex: Long) {
        add(offset, compressedPageSize, firstRowIndex, OptionalLong.empty())
    }

    /**
     * Adds the specified parameters to this builder. Used by the metadata converter to building up [OffsetIndex]
     * objects read from the Parquet file.
     *
     * @param offset
     * the offset of the page in the file
     * @param compressedPageSize
     * the size of the page (including header)
     * @param firstRowIndex
     * the index of the first row in the page (within the row group)
     * @param unencodedDataBytes
     * the number of bytes of unencoded data of BYTE_ARRAY type
     */
    fun add(offset: Long, compressedPageSize: Int, firstRowIndex: Long, unencodedDataBytes: OptionalLong) {
        previousOffset = offset
        offsets.add(offset)
        previousPageSize = compressedPageSize
        compressedPageSizes.add(compressedPageSize)
        previousRowIndex = firstRowIndex
        firstRowIndexes.add(firstRowIndex)
        if (unencodedDataBytes.isPresent) {
            this.unencodedDataBytes.add(unencodedDataBytes.asLong)
        }
    }

    /**
     * Builds the offset index. Used by the metadata converter to building up [OffsetIndex]
     * objects read from the Parquet file.
     *
     * @return the newly created offset index or `null` if the [OffsetIndex] object would be empty
     */
    open fun build(): OffsetIndex? {
        return build(0)
    }

    fun fromOffsetIndex(offsetIndex: OffsetIndex) = apply {
        assert(offsetIndex is OffsetIndexImpl)
        val offsetIndexImpl = offsetIndex as OffsetIndexImpl
        offsets.addAll(LongArrayList(offsetIndexImpl.offsets))
        compressedPageSizes.addAll(IntArrayList(offsetIndexImpl.compressedPageSizes))
        firstRowIndexes.addAll(LongArrayList(offsetIndexImpl.firstRowIndexes))
        if (offsetIndexImpl.unencodedByteArrayDataBytes != null) {
            this.unencodedDataBytes.addAll(LongArrayList(offsetIndexImpl.unencodedByteArrayDataBytes))
        }
        previousOffset = 0
        previousPageSize = 0
        previousRowIndex = 0
        previousRowCount = 0
    }

    /**
     * Builds the offset index. Used by the writers to building up [OffsetIndex] objects to be
     * written to the Parquet file.
     *
     * @param shift how much to be shifted away
     * @return the newly created offset index or `null` if the [OffsetIndex] object would be empty
     */
    open fun build(shift: Long): OffsetIndex? {
        if (compressedPageSizes.isEmpty()) {
            return null
        }
        val offsets = this.offsets.toLongArray()
        if (shift != 0L) {
            var i = 0
            val n = offsets.size
            while (i < n) {
                offsets[i] += shift
                ++i
            }
        }
        return OffsetIndexImpl(
            offsets = offsets,
            compressedPageSizes = compressedPageSizes.toIntArray(),
            firstRowIndexes = firstRowIndexes.toLongArray(),
            unencodedByteArrayDataBytes = if (unencodedDataBytes.isEmpty()) null else {
                check(unencodedDataBytes.size == this.offsets.size) { "unencodedDataBytes does not have the same size as offsets" }
                unencodedDataBytes.toLongArray()
            },
        )
    }

    companion object {
        /**
         * @return a no-op builder that does not collect values and therefore returns `null` at [.build]
         */
        @JvmStatic
        val noOpBuilder: OffsetIndexBuilder = object : OffsetIndexBuilder() {
            override fun add(compressedPageSize: Int, rowCount: Long) = Unit
            override fun add(offset: Long, compressedPageSize: Int, rowCount: Long) = Unit
            override fun build(): OffsetIndex? = null
            override fun build(shift: Long): OffsetIndex? = null
        }

        /**
         * @return an [OffsetIndexBuilder] instance to build an [OffsetIndex] object
         */
        @JvmStatic
        fun getBuilder(): OffsetIndexBuilder {
            return OffsetIndexBuilder()
        }
    }
}
