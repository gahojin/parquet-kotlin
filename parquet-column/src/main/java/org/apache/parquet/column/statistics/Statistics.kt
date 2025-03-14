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

import org.apache.parquet.column.UnknownColumnTypeException
import org.apache.parquet.column.statistics.StatisticsClassException.Companion.create
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Binary.Companion.fromConstantByteArray
import org.apache.parquet.schema.Float16.isNaN
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveComparator
import org.apache.parquet.schema.PrimitiveStringifier
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type

/**
 * Statistics class to keep track of statistics in parquet pages and column chunks
 *
 * @param <T> the Java type described by this Statistics instance
 */
abstract class Statistics<T : Comparable<T>> internal constructor(
    private val type: PrimitiveType,
) {
    /**
     * Builder class to build Statistics objects. Used to read the statistics from the Parquet file.
     */
    open class Builder internal constructor(private val type: PrimitiveType) {
        private var min: ByteArray? = null
        private var max: ByteArray? = null
        private var numNulls: Long = -1L

        fun withMin(min: ByteArray?): Builder = apply {
            this.min = min
        }

        fun withMax(max: ByteArray?): Builder = apply {
            this.max = max
        }

        fun withNumNulls(numNulls: Long): Builder = apply {
            this.numNulls = numNulls
        }

        open fun build(): Statistics<*> {
            val stats: Statistics<*> = createStats(type)
            val min = min
            val max = max
            if (min != null && max != null) {
                stats.setMinMaxFromBytes(min, max)
            }
            stats.numNulls = numNulls
            return stats
        }
    }

    // Builder for FLOAT type to handle special cases of min/max values like NaN, -0.0, and 0.0
    private class FloatBuilder(type: PrimitiveType) : Builder(type) {
        init {
            assert(type.primitiveTypeName === PrimitiveTypeName.FLOAT)
        }

        override fun build(): Statistics<*> {
            val stats = super.build() as FloatStatistics
            if (stats.hasNonNullValue()) {
                var min = stats.genericGetMin()
                var max = stats.genericGetMax()
                // Drop min/max values in case of NaN as the sorting order of values is undefined for this case
                if (min.isNaN() || max.isNaN()) {
                    stats.setMinMax(0.0f, 0.0f)
                    (stats as Statistics<*>).hasNonNullValue = false
                } else {
                    // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
                    if (min.compareTo(0.0f) == 0) {
                        min = -0.0f
                        stats.setMinMax(min, max)
                    }
                    if (max.compareTo(-0.0f) == 0) {
                        max = 0.0f
                        stats.setMinMax(min, max)
                    }
                }
            }
            return stats
        }
    }

    // Builder for DOUBLE type to handle special cases of min/max values like NaN, -0.0, and 0.0
    private class DoubleBuilder(type: PrimitiveType) : Builder(type) {
        init {
            assert(type.primitiveTypeName === PrimitiveTypeName.DOUBLE)
        }

        override fun build(): Statistics<*> {
            val stats = super.build() as DoubleStatistics
            if (stats.hasNonNullValue()) {
                var min = stats.genericGetMin()
                var max = stats.genericGetMax()
                // Drop min/max values in case of NaN as the sorting order of values is undefined for this case
                if (min.isNaN() || max.isNaN()) {
                    stats.setMinMax(0.0, 0.0)
                    (stats as Statistics<*>).hasNonNullValue = false
                } else {
                    // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
                    if (min.compareTo(0.0) == 0) {
                        min = -0.0
                        stats.setMinMax(min, max)
                    }
                    if (max.compareTo(-0.0) == 0) {
                        max = 0.0
                        stats.setMinMax(min, max)
                    }
                }
            }
            return stats
        }
    }

    // Builder for FLOAT16 type to handle special cases of min/max values like NaN, -0.0, and 0.0
    private class Float16Builder(type: PrimitiveType) : Builder(type) {
        init {
            assert(type.primitiveTypeName === PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            assert(type.typeLength == 2)
        }

        override fun build(): Statistics<*> {
            val stats = super.build() as BinaryStatistics
            if (stats.hasNonNullValue()) {
                val bMin = stats.genericGetMin()
                val bMax = stats.genericGetMax()
                val min = bMin.get2BytesLittleEndian()
                val max = bMax.get2BytesLittleEndian()
                // Drop min/max values in case of NaN as the sorting order of values is undefined for this case
                if (isNaN(min) || isNaN(max)) {
                    stats.setMinMax(POSITIVE_ZERO_LITTLE_ENDIAN, NEGATIVE_ZERO_LITTLE_ENDIAN)
                    (stats as Statistics<*>).hasNonNullValue = false
                } else {
                    // Updating min to -0.0 and max to +0.0 to ensure that no 0.0 values would be skipped
                    if (min == 0x0000.toShort()) {
                        stats.setMinMax(NEGATIVE_ZERO_LITTLE_ENDIAN, bMax)
                    }
                    if (max == 0x8000.toShort()) {
                        stats.setMinMax(bMin, POSITIVE_ZERO_LITTLE_ENDIAN)
                    }
                }
            }
            return stats
        }

        companion object {
            private val POSITIVE_ZERO_LITTLE_ENDIAN = fromConstantByteArray(byteArrayOf(0x00, 0x00))
            private val NEGATIVE_ZERO_LITTLE_ENDIAN = fromConstantByteArray(byteArrayOf(0x00, 0x80.toByte()))
        }
    }

    private val comparator: PrimitiveComparator<T> = type.comparator()
    private var hasNonNullValue = false

    /**
     * null count or `-1` if the null count is not set
     */
    @Deprecated("will be removed in 2.0.0. Use {@link #getBuilderForReading(PrimitiveType)} instead.")
    open var numNulls: Long = 0

    /**
     * whether numNulls is set and can be used
     */
    open val isNumNullsSet: Boolean
        get() = numNulls >= 0

    val stringifier: PrimitiveStringifier = type.stringifier()

    /**
     * byte array corresponding to the max value
     */
    abstract val maxBytes: ByteArray

    /**
     * byte array corresponding to the min value
     */
    abstract val minBytes: ByteArray

    /**
     * Returns a boolean specifying if the Statistics object is empty,
     * i.e does not contain valid statistics for the page/column yet
     *
     * @return true if object is empty, false otherwise
     */
    open val isEmpty: Boolean
        get() = !hasNonNullValue && !isNumNullsSet

    /**
     * updates statistics min and max using the passed value
     *
     * @param value value to use to update min and max
     */
    open fun updateStats(value: Int) {
        throw UnsupportedOperationException()
    }

    /**
     * updates statistics min and max using the passed value
     *
     * @param value value to use to update min and max
     */
    open fun updateStats(value: Long) {
        throw UnsupportedOperationException()
    }

    /**
     * updates statistics min and max using the passed value
     *
     * @param value value to use to update min and max
     */
    open fun updateStats(value: Float) {
        throw UnsupportedOperationException()
    }

    /**
     * updates statistics min and max using the passed value
     *
     * @param value value to use to update min and max
     */
    open fun updateStats(value: Double) {
        throw UnsupportedOperationException()
    }

    /**
     * updates statistics min and max using the passed value
     *
     * @param value value to use to update min and max
     */
    open fun updateStats(value: Boolean) {
        throw UnsupportedOperationException()
    }

    /**
     * updates statistics min and max using the passed value
     *
     * @param value value to use to update min and max
     */
    open fun updateStats(value: Binary) {
        throw UnsupportedOperationException()
    }

    /**
     * Equality comparison method to compare two statistics objects.
     *
     * @param other Object to compare against
     * @return true if objects are equal, false otherwise
     */
    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is Statistics<*>) return false
        return type == other.type &&
                other.maxBytes.contentEquals(maxBytes) &&
                other.minBytes.contentEquals(minBytes) &&
                other.numNulls == numNulls
    }

    /**
     * Hash code for the statistics object
     *
     * @return hash code int
     */
    override fun hashCode(): Int {
        return (31 * hashCode() + 31 * maxBytes.contentHashCode() + 17 * minBytes.contentHashCode() + numNulls.hashCode())
    }

    /**
     * Method to merge this statistics object with the object passed
     * as parameter. Merging keeps the smallest of min values, largest of max
     * values and combines the number of null counts.
     *
     * @param stats Statistics object to merge with
     */
    open fun mergeStatistics(stats: Statistics<*>) {
        if (stats.isEmpty) return

        // Merge stats only if they have the same type
        if (type == stats.type) {
            incrementNumNulls(stats.numNulls)
            if (stats.hasNonNullValue()) {
                mergeStatisticsMinMax(stats)
                markAsNotEmpty()
            }
        } else {
            throw create(this, stats)
        }
    }

    /**
     * Abstract method to merge this statistics min and max with the values
     * of the parameter object. Does not do any checks, only called internally.
     *
     * @param stats Statistics object to merge with
     */
    protected abstract fun mergeStatisticsMinMax(stats: Statistics<*>)

    /**
     * Abstract method to set min and max values from byte arrays.
     *
     * @param minBytes byte array to set the min value to
     * @param maxBytes byte array to set the max value to
     */
    @Deprecated("will be removed in 2.0.0. Use {@link #getBuilderForReading(PrimitiveType)} instead.")
    abstract fun setMinMaxFromBytes(minBytes: ByteArray, maxBytes: ByteArray)

    /**
     * Returns the min value in the statistics. The java natural order of the returned type defined by [Comparable.compareTo] might not be the proper one.
     * For example, UINT_32 requires unsigned comparison instead of the natural signed one.
     * Use [.compareMinToValue] or the comparator returned by [.comparator] to always get the proper ordering.
     *
     * @return the min value
     */
    abstract fun genericGetMin(): T

    /**
     * Returns the max value in the statistics. The java natural order of the returned type defined by [Comparable.compareTo] might not be the proper one.
     * For example, UINT_32 requires unsigned comparison instead of the natural signed one.
     * Use [.compareMaxToValue] or the comparator returned by [.comparator] to always get the proper ordering.
     *
     * @return the max value
     */
    abstract fun genericGetMax(): T

    /**
     * Returns the [PrimitiveComparator] implementation to be used to compare two generic values in the proper way
     * (for example, unsigned comparison for UINT_32).
     *
     * @return the comparator for data described by this Statistics instance
     */
    fun comparator(): PrimitiveComparator<T> {
        return comparator
    }

    /**
     * Compares min to the specified value in the proper way. It does the same as invoking
     * `comparator().compare(genericGetMin(), value)`. The corresponding statistics implementations overload this
     * method so the one with the primitive argument shall be used to avoid boxing/unboxing.
     *
     * @param value the value which `min` is to be compared to
     * @return a negative integer, zero, or a positive integer as `min` is less than, equal to, or greater than
     * `value`.
     */
    open fun compareMinToValue(value: T): Int {
        return comparator.compare(genericGetMin(), value)
    }

    /**
     * Compares max to the specified value in the proper way. It does the same as invoking
     * `comparator().compare(genericGetMax(), value)`. The corresponding statistics implementations overload this
     * method so the one with the primitive argument shall be used to avoid boxing/unboxing.
     *
     * @param value the value which `max` is to be compared to
     * @return a negative integer, zero, or a positive integer as `max` is less than, equal to, or greater than
     * `value`.
     */
    open fun compareMaxToValue(value: T): Int {
        return comparator.compare(genericGetMax(), value)
    }

    /**
     * Returns the string representation of min for debugging/logging purposes.
     *
     * @return the min value as a string
     */
    fun minAsString(): String {
        return stringify(genericGetMin())
    }

    /**
     * Returns the string representation of max for debugging/logging purposes.
     *
     * @return the max value as a string
     */
    fun maxAsString(): String? {
        return stringify(genericGetMax())
    }

    abstract fun stringify(value: T): String

    /**
     * Abstract method to return whether the min and max values fit in the given
     * size.
     *
     * @param size a size in bytes
     * @return true iff the min and max values are less than size bytes
     */
    abstract fun isSmallerThan(size: Long): Boolean

    override fun toString(): String {
        return when {
            hasNonNullValue() -> {
                if (isNumNullsSet) {
                    "min: %s, max: %s, num_nulls: %d".format(minAsString(), maxAsString(), numNulls)
                } else {
                    "min: %s, max: %s, num_nulls not defined".format(minAsString(), maxAsString())
                }
            }
            isEmpty -> "no stats for this column"
            else -> "num_nulls: %d, min/max not defined".format(numNulls)
        }
    }

    /**
     * Increments the null count by one
     */
    fun incrementNumNulls() {
        numNulls++
    }

    /**
     * Increments the null count by the parameter value
     *
     * @param increment value to increment the null count by
     */
    fun incrementNumNulls(increment: Long) {
        numNulls += increment
    }

    /**
     * Returns whether there have been non-null values added to this statistics
     *
     * @return true if the values contained at least one non-null value
     */
    open fun hasNonNullValue(): Boolean {
        return hasNonNullValue
    }

    /**
     * Sets the page/column as having a valid non-null value
     * kind of misnomer here
     */
    protected fun markAsNotEmpty() {
        hasNonNullValue = true
    }

    /**
     * @return a new independent statistics instance of this class.
     */
    abstract fun copy(): Statistics<T>

    /**
     * @return the primitive type object which this statistics is created for
     */
    fun type(): PrimitiveType {
        return type
    }

    companion object {
        /**
         * Returns the typed statistics object based on the passed type parameter
         *
         * @param type PrimitiveTypeName type of the column
         * @return instance of a typed statistics class
         */
        @Deprecated("Use {@link #createStats(Type)} instead")
        fun getStatsBasedOnType(type: PrimitiveTypeName): Statistics<*> {
            return when (type) {
                PrimitiveTypeName.INT32 -> IntStatistics()
                PrimitiveTypeName.INT64 -> LongStatistics()
                PrimitiveTypeName.FLOAT -> FloatStatistics()
                PrimitiveTypeName.DOUBLE -> DoubleStatistics()
                PrimitiveTypeName.BOOLEAN -> BooleanStatistics()
                PrimitiveTypeName.BINARY -> BinaryStatistics()
                PrimitiveTypeName.INT96 -> BinaryStatistics()
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> BinaryStatistics()
            }
        }

        /**
         * Creates an empty `Statistics` instance for the specified type to be
         * used for reading/writing the new min/max statistics used in the V2 format.
         *
         * @param type type of the column
         * @return instance of a typed statistics class
         */
        @JvmStatic
        fun createStats(type: Type): Statistics<*> {
            val primitive = type.asPrimitiveType()
            return when (primitive.primitiveTypeName) {
                PrimitiveTypeName.INT32 -> IntStatistics(primitive)
                PrimitiveTypeName.INT64 -> LongStatistics(primitive)
                PrimitiveTypeName.FLOAT -> FloatStatistics(primitive)
                PrimitiveTypeName.DOUBLE -> DoubleStatistics(primitive)
                PrimitiveTypeName.BOOLEAN -> BooleanStatistics(primitive)
                PrimitiveTypeName.BINARY, PrimitiveTypeName.INT96, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> BinaryStatistics(primitive)
                else -> throw UnknownColumnTypeException(primitive.primitiveTypeName)
            }
        }

        /**
         * Creates a noop `NoopStatistics` statistics instance. This is only used when the user disables statistics for the specified column.
         * @param type type of the column
         * @return a noop statistics
         */
        @JvmStatic
        fun noopStats(type: Type): Statistics<*> {
            return NoopStatistics<Comparable<Any>>(type.asPrimitiveType())
        }

        /**
         * Returns a builder to create new statistics object. Used to read the statistics from the parquet file.
         *
         * @param type type of the column
         * @return builder to create new statistics object
         */
        @JvmStatic
        fun getBuilderForReading(type: PrimitiveType): Builder {
            return when (type.primitiveTypeName) {
                PrimitiveTypeName.FLOAT -> FloatBuilder(type)
                PrimitiveTypeName.DOUBLE -> DoubleBuilder(type)
                PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> {
                    val logicalTypeAnnotation = type.logicalTypeAnnotation
                    if (logicalTypeAnnotation is Float16LogicalTypeAnnotation) {
                        Float16Builder(type)
                    } else {
                        Builder(type)
                    }
                }
                else -> Builder(type)
            }
        }
    }
}
