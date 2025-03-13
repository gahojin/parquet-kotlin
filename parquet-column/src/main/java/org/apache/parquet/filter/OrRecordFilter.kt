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
 * Provides ability to chain two filters together.
 */
class OrRecordFilter private constructor(
    private val boundFilter1: RecordFilter,
    private val boundFilter2: RecordFilter,
) : RecordFilter {
    override val isMatch: Boolean
        get() = boundFilter1.isMatch || boundFilter2.isMatch

    companion object {
        /**
         * Returns builder for creating an and filter.
         *
         * @param filter1 The first filter to check.
         * @param filter2 The second filter to check.
         * @return an or record filter
         */
        @JvmStatic
        fun or(filter1: UnboundRecordFilter, filter2: UnboundRecordFilter): UnboundRecordFilter {
            return UnboundRecordFilter {
                OrRecordFilter(filter1.bind(it), filter2.bind(it))
            }
        }
    }
}
