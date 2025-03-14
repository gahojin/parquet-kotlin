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
package org.apache.parquet.filter

/**
 * Filter which will only materialize a page worth of results.
 */
class PagedRecordFilter private constructor(
    private val startPos: Long,
    pageSize: Long,
) : RecordFilter {
    private val endPos: Long = startPos + pageSize
    private var currentPos: Long = 0L

    /**
     * Keeps track of how many times it is called. Only returns matches when the
     * record number is in the range.
     */
    override val isMatch: Boolean
        get() {
            currentPos++
            return ((currentPos >= startPos) && (currentPos < endPos))
        }

    companion object {
        /**
         * Returns builder for creating a paged query.
         *
         * @param startPos The record to start from, numbering starts at 1.
         * @param pageSize The size of the page.
         * @return a paged record filter
         */
        @JvmStatic
        fun page(startPos: Long, pageSize: Long): UnboundRecordFilter {
            return UnboundRecordFilter {
                PagedRecordFilter(startPos, pageSize)
            }
        }
    }
}
