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

class LongStatistics : Statistics<Long> {
    var max: Long = 0L
        private set
    var min: Long = 0L
        private set

    override val maxBytes: ByteArray
        get() = longToBytes(max)

    override val minBytes: ByteArray
        get() = longToBytes(min)

    @Deprecated("will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead")
    constructor() : this(DEFAULT_FAKE_TYPE)

    internal constructor(type: PrimitiveType) : super(type)

    private constructor(other: LongStatistics) : super(other.type()) {
        if (other.hasNonNullValue()) {
            initializeStats(other.min, other.max)
        }
        numNulls = other.numNulls
    }

    override fun updateStats(value: Long) {
        if (hasNonNullValue()) {
            updateStats(value, value)
        } else {
            initializeStats(value, value)
        }
    }

    public override fun mergeStatisticsMinMax(stats: Statistics<*>) {
        val longStats = stats as LongStatistics
        if (hasNonNullValue()) {
            updateStats(longStats.min, longStats.max)
        } else {
            initializeStats(longStats.min, longStats.max)
        }
    }

    @Deprecated("Deprecated in Java")
    override fun setMinMaxFromBytes(minBytes: ByteArray, maxBytes: ByteArray) {
        max = bytesToLong(maxBytes)
        min = bytesToLong(minBytes)
        markAsNotEmpty()
    }

    override fun stringify(value: Long): String {
        return stringifier.stringify(value)
    }

    override fun isSmallerThan(size: Long): Boolean {
        return !hasNonNullValue() || (16 < size)
    }

    fun updateStats(minValue: Long, maxValue: Long) {
        if (comparator().compare(min, minValue) > 0) {
            min = minValue
        }
        if (comparator().compare(max, maxValue) < 0) {
            max = maxValue
        }
    }

    fun initializeStats(minValue: Long, maxValue: Long) {
        min = minValue
        max = maxValue
        markAsNotEmpty()
    }

    override fun genericGetMin(): Long {
        return min
    }

    override fun genericGetMax(): Long {
        return max
    }

    override fun compareMinToValue(value: Long): Int {
        return comparator().compare(min, value)
    }

    override fun compareMaxToValue(value: Long): Int {
        return comparator().compare(max, value)
    }

    fun setMinMax(min: Long, max: Long) {
        this.max = max
        this.min = min
        markAsNotEmpty()
    }

    override fun copy(): LongStatistics {
        return LongStatistics(this)
    }

    companion object {
        // A fake type object to be used to generate the proper comparator
        private val DEFAULT_FAKE_TYPE: PrimitiveType =
            Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("fake_int64_type")
    }
}
