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

import org.apache.parquet.bytes.BytesUtils.booleanToBytes
import org.apache.parquet.bytes.BytesUtils.bytesToBool
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types

class BooleanStatistics : Statistics<Boolean> {
    var max: Boolean = false
        private set
    var min: Boolean = false
        private set

    override val maxBytes: ByteArray
        get() = booleanToBytes(max)

    override val minBytes: ByteArray
        get() = booleanToBytes(min)

    @Deprecated("will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead")
    constructor() : this(DEFAULT_FAKE_TYPE)

    internal constructor(type: PrimitiveType) : super(type)

    private constructor(other: BooleanStatistics) : super(other.type()) {
        if (other.hasNonNullValue()) {
            initializeStats(other.min, other.max)
        }
        numNulls = other.numNulls
    }

    override fun updateStats(value: Boolean) {
        if (hasNonNullValue()) {
            updateStats(value, value)
        } else {
            initializeStats(value, value)
        }
    }

    override fun mergeStatisticsMinMax(stats: Statistics<*>) {
        val boolStats = stats as BooleanStatistics
        if (hasNonNullValue()) {
            updateStats(boolStats.min, boolStats.max)
        } else {
            initializeStats(boolStats.min, boolStats.max)
        }
    }

    @Deprecated("Deprecated in Java")
    override fun setMinMaxFromBytes(minBytes: ByteArray, maxBytes: ByteArray) {
        max = bytesToBool(maxBytes)
        min = bytesToBool(minBytes)
        markAsNotEmpty()
    }

    override fun stringify(value: Boolean): String {
        return stringifier.stringify(value)
    }

    override fun isSmallerThan(size: Long): Boolean {
        return !hasNonNullValue() || (2 < size)
    }

    fun updateStats(minValue: Boolean, maxValue: Boolean) {
        if (comparator().compare(min, minValue) > 0) {
            min = minValue
        }
        if (comparator().compare(max, maxValue) < 0) {
            max = maxValue
        }
    }

    fun initializeStats(minValue: Boolean, maxValue: Boolean) {
        min = minValue
        max = maxValue
        markAsNotEmpty()
    }

    override fun genericGetMin(): Boolean {
        return min
    }

    override fun genericGetMax(): Boolean {
        return max
    }

    override fun compareMinToValue(value: Boolean): Int {
        return comparator().compare(min, value)
    }

    override fun compareMaxToValue(value: Boolean): Int {
        return comparator().compare(max, value)
    }

    fun setMinMax(min: Boolean, max: Boolean) {
        this.max = max
        this.min = min
        markAsNotEmpty()
    }

    override fun copy(): BooleanStatistics {
        return BooleanStatistics(this)
    }

    companion object {
        // A fake type object to be used to generate the proper comparator
        private val DEFAULT_FAKE_TYPE: PrimitiveType =
            Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("fake_boolean_type")
    }
}
