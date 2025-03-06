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
import org.apache.parquet.schema.PrimitiveType

/**
 * A noop statistics which always return empty.
 */
internal class NoopStatistics<T : Comparable<T>>(
    type: PrimitiveType,
) : Statistics<T>(type) {
    override fun updateStats(value: Int) = Unit

    override fun updateStats(value: Long) = Unit

    override fun updateStats(value: Float) = Unit

    override fun updateStats(value: Double) = Unit

    override fun updateStats(value: Boolean) = Unit

    override fun updateStats(value: Binary?) = Unit

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is Statistics<*>) return false
        return type().equals(other.type())
    }

    override fun hashCode(): Int {
        return 31 * type().hashCode()
    }

    override fun mergeStatisticsMinMax(stats: Statistics<*>?) = Unit

    @Deprecated("will be removed in 2.0.0. Use {@link #getBuilderForReading(PrimitiveType)} instead.")
    override fun setMinMaxFromBytes(minBytes: ByteArray, maxBytes: ByteArray) = Unit

    override fun genericGetMin(): T {
        throw UnsupportedOperationException("genericGetMin is not supported by ${javaClass.name}")
    }

    override fun genericGetMax(): T? {
        throw UnsupportedOperationException("genericGetMax is not supported by ${javaClass.name}")
    }

    override fun getMaxBytes(): ByteArray? {
        throw UnsupportedOperationException("getMaxBytes is not supported by ${javaClass.name}")
    }

    override fun getMinBytes(): ByteArray? {
        throw UnsupportedOperationException("getMinBytes is not supported by ${javaClass.name}")
    }

    override fun stringify(value: T?): String? {
        throw UnsupportedOperationException("stringify is not supported by ${javaClass.name}")
    }

    override fun isSmallerThan(size: Long): Boolean {
        throw UnsupportedOperationException("isSmallerThan is not supported by ${javaClass.name}")
    }

    override fun getNumNulls(): Long = -1L

    override fun isEmpty(): Boolean = true

    override fun hasNonNullValue(): Boolean = false

    override fun isNumNullsSet(): Boolean = false

    override fun copy(): Statistics<T> {
        return NoopStatistics<T>(type())
    }
}
