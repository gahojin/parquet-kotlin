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

import org.apache.parquet.schema.PrimitiveComparator

/**
 * This class calculates the max and min values of an iterable collection.
 */
class MinMax<T>(comparator: PrimitiveComparator<T>, iterable: Iterable<T>) {
    var min: T? = null
        private set
    var max: T? = null
        private set

    init {
        getMinAndMax(comparator, iterable)
    }

    private fun getMinAndMax(comparator: PrimitiveComparator<T>, iterable: Iterable<T>) {
        var max: T? = null
        var min: T? = null
        iterable.forEach { element ->
            if (max == null) {
                max = element
            } else if (element != null && comparator.compare(max, element) < 0) {
                max = element
            }
            if (min == null) {
                min = element
            } else if (element != null && comparator.compare(min, element) > 0) {
                min = element
            }
        }
        this.max = max
        this.min = min
    }
}
