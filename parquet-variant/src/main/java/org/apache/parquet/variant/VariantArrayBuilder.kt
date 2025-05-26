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
package org.apache.parquet.variant

/**
 * Builder for creating Variant arrays, used by VariantBuilder.
 */
open class VariantArrayBuilder internal constructor(
    metadata: Metadata,
) : VariantBuilder(metadata) {
    /** The offsets of the elements in this array.  */
    private val offsets: ArrayList<Int> = ArrayList()

    /** The number of values appended to this array.  */
    var numValues: Long = 0L
        private set

    /**
     * @return the list of element offsets in this array
     */
    fun validateAndGetOffsets(): ArrayList<Int> {
        check(offsets.size.toLong() == numValues) {
            "Number of offsets (%d) do not match the number of values (%d).".format(offsets.size, numValues)
        }
        checkMultipleNested("Cannot call endArray() while a nested object/array is still open.")
        return offsets
    }

    override fun onAppend() {
        checkAppendWhileNested()
        offsets.add(writePos)
        numValues++
    }

    override fun onStartNested() {
        checkMultipleNested("Cannot call startObject()/startArray() without calling endObject()/endArray() first.")
        offsets.add(writePos)
        numValues++
    }
}
