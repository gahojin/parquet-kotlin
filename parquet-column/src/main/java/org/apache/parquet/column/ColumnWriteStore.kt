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

/**
 * Container which can construct writers for multiple columns to be stored together.
 */
interface ColumnWriteStore : AutoCloseable {
    /**
     * used for information
     *
     * @return approximate size used in memory
     */
    val allocatedSize: Long

    /**
     * used to flush row groups to disk
     *
     * @return approximate size of the buffered encoded binary data
     */
    val bufferedSize: Long

    /**
     * Returns whether flushing the possibly cached values (or nulls) to the underlying column writers is necessary,
     * because the pages might be closed after the next invocation of [.endRecord].
     *
     * @return `true` if all the values shall be written to the underlying column writers before calling [.endRecord]
     */
    val isColumnFlushNeeded: Boolean
        get() = false

    /**
     * @param path the column for which to create a writer
     * @return the column writer for the given column
     */
    fun getColumnWriter(path: ColumnDescriptor): ColumnWriter

    /**
     * when we are done writing to flush to the underlying storage
     */
    fun flush()

    /**
     * called to notify of record boundaries
     */
    fun endRecord()

    /**
     * used for debugging purpose
     *
     * @return a formated string representing memory usage per column
     */
    fun memUsageString(): String?

    /**
     * Close the related output stream and release any resources
     */
    override fun close()
}
