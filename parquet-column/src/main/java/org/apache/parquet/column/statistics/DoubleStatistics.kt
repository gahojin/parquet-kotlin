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

import org.apache.parquet.bytes.BytesUtils.bytesToLong
import org.apache.parquet.bytes.BytesUtils.longToBytes
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types

class DoubleStatistics : Statistics<Double> {
    var max: Double = 0.0
        private set
    var min: Double = 0.0
        private set

    override val maxBytes: ByteArray
        get() = longToBytes(java.lang.Double.doubleToLongBits(max))

    override val minBytes: ByteArray
        get() = longToBytes(java.lang.Double.doubleToLongBits(min))

    @Deprecated("will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead")
    constructor() : this(DEFAULT_FAKE_TYPE)

    internal constructor(type: PrimitiveType) : super(type)

    private constructor(other: DoubleStatistics) : super(other.type()) {
        if (other.hasNonNullValue()) {
            initializeStats(other.min, other.max)
        }
        numNulls = other.numNulls
    }

    override fun updateStats(value: Double) {
        if (hasNonNullValue()) {
            updateStats(value, value)
        } else {
            initializeStats(value, value)
        }
    }

    override fun mergeStatisticsMinMax(stats: Statistics<*>) {
        val doubleStats = stats as DoubleStatistics
        if (hasNonNullValue()) {
            updateStats(doubleStats.min, doubleStats.max)
        } else {
            initializeStats(doubleStats.min, doubleStats.max)
        }
    }

    @Deprecated("Deprecated in Java")
    override fun setMinMaxFromBytes(minBytes: ByteArray, maxBytes: ByteArray) {
        max = java.lang.Double.longBitsToDouble(bytesToLong(maxBytes))
        min = java.lang.Double.longBitsToDouble(bytesToLong(minBytes))
        markAsNotEmpty()
    }

    override fun stringify(value: Double): String {
        return stringifier.stringify(value)
    }

    override fun isSmallerThan(size: Long): Boolean {
        return !hasNonNullValue() || (16 < size)
    }

    fun updateStats(minValue: Double, maxValue: Double) {
        if (comparator().compare(min, minValue) > 0) {
            min = minValue
        }
        if (comparator().compare(max, maxValue) < 0) {
            max = maxValue
        }
    }

    fun initializeStats(minValue: Double, maxValue: Double) {
        min = minValue
        max = maxValue
        markAsNotEmpty()
    }

    override fun genericGetMin(): Double {
        return min
    }

    override fun genericGetMax(): Double {
        return max
    }

    override fun compareMinToValue(value: Double): Int {
        return comparator().compare(min, value)
    }

    override fun compareMaxToValue(value: Double): Int {
        return comparator().compare(max, value)
    }

    fun setMinMax(min: Double, max: Double) {
        this.max = max
        this.min = min
        markAsNotEmpty()
    }

    override fun copy(): DoubleStatistics {
        return DoubleStatistics(this)
    }

    companion object {
        // A fake type object to be used to generate the proper comparator
        private val DEFAULT_FAKE_TYPE: PrimitiveType =
            Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named("fake_double_type")
    }
}
