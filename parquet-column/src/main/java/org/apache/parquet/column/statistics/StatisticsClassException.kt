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
package org.apache.parquet.column.statistics

import org.apache.parquet.ParquetRuntimeException

/**
 * Thrown if the two Statistics objects have mismatching types
 */
class StatisticsClassException private constructor(msg: String) : ParquetRuntimeException(msg) {
    constructor(className1: String, className2: String) : this("Statistics classes mismatched: $className1 vs. $className2")

    companion object {
        private const val serialVersionUID = 1L

        @JvmStatic
        fun create(stats1: Statistics<*>, stats2: Statistics<*>): StatisticsClassException {
            if (stats1.javaClass != stats2.javaClass) {
                return StatisticsClassException(stats1.javaClass.toString(), stats2.javaClass.toString()
                )
            }
            return StatisticsClassException(
                "Statistics comparator mismatched: ${stats1.comparator()} vs. ${stats2.comparator()}"
            )
        }
    }
}
