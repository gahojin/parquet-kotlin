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

import org.apache.parquet.bytes.BytesUtils.bytesToInt
import org.apache.parquet.bytes.BytesUtils.intToBytes
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types

class FloatStatistics : Statistics<Float> {
    var max: Float = 0f
        private set
    var min: Float = 0f
        private set

    override val maxBytes: ByteArray
        get() = intToBytes(java.lang.Float.floatToIntBits(max))

    override val minBytes: ByteArray
        get() = intToBytes(java.lang.Float.floatToIntBits(min))

    @Deprecated("will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead")
    constructor() : this(DEFAULT_FAKE_TYPE)

    internal constructor(type: PrimitiveType) : super(type)

    private constructor(other: FloatStatistics) : super(other.type()) {
        if (other.hasNonNullValue()) {
            initializeStats(other.min, other.max)
        }
        numNulls = other.numNulls
    }

    override fun updateStats(value: Float) {
        if (hasNonNullValue()) {
            updateStats(value, value)
        } else {
            initializeStats(value, value)
        }
    }

    public override fun mergeStatisticsMinMax(stats: Statistics<*>) {
        val floatStats = stats as FloatStatistics
        if (hasNonNullValue()) {
            updateStats(floatStats.min, floatStats.max)
        } else {
            initializeStats(floatStats.min, floatStats.max)
        }
    }

    @Deprecated("Deprecated in Java")
    override fun setMinMaxFromBytes(minBytes: ByteArray, maxBytes: ByteArray) {
        max = java.lang.Float.intBitsToFloat(bytesToInt(maxBytes))
        min = java.lang.Float.intBitsToFloat(bytesToInt(minBytes))
        markAsNotEmpty()
    }

    override fun stringify(value: Float): String {
        return stringifier.stringify(value)
    }

    override fun isSmallerThan(size: Long): Boolean {
        return !hasNonNullValue() || (8 < size)
    }

    fun updateStats(minValue: Float, maxValue: Float) {
        if (comparator().compare(min, minValue) > 0) {
            min = minValue
        }
        if (comparator().compare(max, maxValue) < 0) {
            max = maxValue
        }
    }

    fun initializeStats(minValue: Float, maxValue: Float) {
        min = minValue
        max = maxValue
        markAsNotEmpty()
    }

    override fun genericGetMin(): Float {
        return min
    }

    override fun genericGetMax(): Float {
        return max
    }

    override fun compareMinToValue(value: Float): Int {
        return comparator().compare(min, value)
    }

    override fun compareMaxToValue(value: Float): Int {
        return comparator().compare(max, value)
    }

    fun setMinMax(min: Float, max: Float) {
        this.max = max
        this.min = min
        markAsNotEmpty()
    }

    override fun copy(): FloatStatistics {
        return FloatStatistics(this)
    }

    companion object {
        // A fake type object to be used to generate the proper comparator
        private val DEFAULT_FAKE_TYPE: PrimitiveType =
            Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named("fake_float_type")
    }
}
