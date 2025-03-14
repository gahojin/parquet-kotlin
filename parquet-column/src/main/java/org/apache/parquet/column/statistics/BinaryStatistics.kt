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

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Binary.Companion.fromConstantByteArray
import org.apache.parquet.io.api.Binary.Companion.fromReusedByteArray
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types

class BinaryStatistics : Statistics<Binary> {
    /**
     * @return the max binary
     */
    @get:Deprecated("use {@link #genericGetMax()}, will be removed in 2.0.0")
    var max: Binary = EMPTY
        private set

    /**
     * @return the min binary
     */
    @get:Deprecated("use {@link #genericGetMin()}, will be removed in 2.0.0")
    var min: Binary = EMPTY
        private set

    override val maxBytes: ByteArray
        get() = max.bytes

    override val minBytes: ByteArray
        get() = min.bytes

    @Deprecated("will be removed in 2.0.0. Use {@link Statistics#createStats(org.apache.parquet.schema.Type)} instead")
    constructor() : this(DEFAULT_FAKE_TYPE)

    internal constructor(type: PrimitiveType) : super(type)

    private constructor(other: BinaryStatistics) : super(other.type()) {
        if (other.hasNonNullValue()) {
            initializeStats(other.min, other.max)
        }
        numNulls = other.numNulls
    }

    override fun updateStats(value: Binary) {
        if (!hasNonNullValue()) {
            min = value.copy()
            max = value.copy()
            markAsNotEmpty()
        } else if (comparator().compare(min, value) > 0) {
            min = value.copy()
        } else if (comparator().compare(max, value) < 0) {
            max = value.copy()
        }
    }

    override fun mergeStatisticsMinMax(stats: Statistics<*>) {
        val binaryStats = stats as BinaryStatistics
        if (hasNonNullValue()) {
            updateStats(binaryStats.min, binaryStats.max)
        } else {
            initializeStats(binaryStats.min, binaryStats.max)
        }
    }

    /**
     * Sets min and max values, re-uses the byte[] passed in.
     * Any changes made to byte[] will be reflected in min and max values as well.
     *
     * @param minBytes byte array to set the min value to
     * @param maxBytes byte array to set the max value to
     */
    @Deprecated("Deprecated in Java")
    override fun setMinMaxFromBytes(minBytes: ByteArray, maxBytes: ByteArray) {
        max = fromReusedByteArray(maxBytes)
        min = fromReusedByteArray(minBytes)
        markAsNotEmpty()
    }

    override fun stringify(value: Binary): String {
        return stringifier.stringify(value)
    }

    override fun isSmallerThan(size: Long): Boolean {
        return !hasNonNullValue() || ((min.length() + max.length()) < size)
    }

    fun isSmallerThanWithTruncation(size: Long, truncationLength: Int): Boolean {
        if (!hasNonNullValue()) {
            return true
        }

        val minTruncateLength = minOf(min.length(), truncationLength)
        val maxTruncateLength = minOf(max.length(), truncationLength)

        return minTruncateLength + maxTruncateLength < size
    }

    /**
     * @param minValue a min binary
     * @param maxValue a max binary
     */
    @Deprecated("use {@link #updateStats(Binary)}, will be removed in 2.0.0")
    fun updateStats(minValue: Binary, maxValue: Binary) {
        if (comparator().compare(min, minValue) > 0) {
            min = minValue.copy()
        }
        if (comparator().compare(max, maxValue) < 0) {
            max = maxValue.copy()
        }
    }

    /**
     * @param minValue a min binary
     * @param maxValue a max binary
     */
    @Deprecated("use {@link #updateStats(Binary)}, will be removed in 2.0.0")
    fun initializeStats(minValue: Binary, maxValue: Binary) {
        min = minValue.copy()
        max = maxValue.copy()
        markAsNotEmpty()
    }

    override fun genericGetMin(): Binary {
        return min
    }

    override fun genericGetMax(): Binary {
        return max
    }

    /**
     * @param min a min binary
     * @param max a max binary
     */
    @Deprecated("use {@link #updateStats(Binary)}, will be removed in 2.0.0")
    fun setMinMax(min: Binary, max: Binary) {
        this.max = max
        this.min = min
        markAsNotEmpty()
    }

    override fun copy(): BinaryStatistics {
        return BinaryStatistics(this)
    }

    companion object {
        // A fake type object to be used to generate the proper comparator
        private val DEFAULT_FAKE_TYPE: PrimitiveType =
            Types.optional(PrimitiveType.PrimitiveTypeName.BINARY).named("fake_binary_type")

        private val EMPTY: Binary = fromConstantByteArray(ByteArray(0))
    }
}
