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

import org.apache.parquet.column.ColumnReader

/**
 * Record filter which applies the supplied predicate to the specified column.
 */
class ColumnRecordFilter private constructor(
    private val filterOnColumn: ColumnReader,
    private val filterPredicate: ColumnPredicates.Predicate,
) : RecordFilter {
    /**
     * @return true if the current value for the column reader matches the predicate.
     */
    override val isMatch: Boolean
        get() = filterPredicate.apply(filterOnColumn)

    companion object {
        /**
         * Factory method for record filter which applies the supplied predicate to the specified column.
         * Note that if searching for a repeated sub-attribute it will only ever match against the
         * first instance of it in the object.
         *
         * @param columnPath Dot separated path specifier, e.g. "engine.capacity"
         * @param predicate  Should call getBinary etc. and check the value
         * @return a column filter
         */
        @JvmStatic
        fun column(
            columnPath: String,
            predicate: ColumnPredicates.Predicate,
        ): UnboundRecordFilter {
            val filterPath = columnPath.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

            return UnboundRecordFilter {
                for (reader in it) {
                    if (reader.descriptor.path.contentEquals(filterPath)) {
                        return@UnboundRecordFilter ColumnRecordFilter(reader, predicate)
                    }
                }
                throw IllegalArgumentException("Column $columnPath does not exist.")
            }
        }
    }
}
