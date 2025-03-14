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

import it.unimi.dsi.fastutil.longs.LongArrayList
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.PrimitiveType
import java.util.OptionalLong

/**
 * A structure for capturing metadata for estimating the unencoded,
 * uncompressed size of data written. This is useful for readers to estimate
 * how much memory is needed to reconstruct data in their memory model and for
 * fine-grained filter push down on nested structures (the histograms contained
 * in this structure can help determine the number of nulls at a particular
 * nesting level and maximum length of lists).
 */
class SizeStatistics(
  val type: PrimitiveType,
  private var unencodedByteArrayDataBytes: Long,
  repetitionLevelHistogram: MutableList<Long>?,
  definitionLevelHistogram: MutableList<Long>?,
) {
    private val repetitionLevelHistogram = repetitionLevelHistogram ?: mutableListOf<Long>()
    private val definitionLevelHistogram = definitionLevelHistogram ?: mutableListOf<Long>()

    /**
     * @return whether the statistics has valid value.
     */
    /**
     * Whether the statistics has valid value.
     *
     * It is true by default. Only set to false while it fails to merge statistics.
     */
    var isValid: Boolean = true
        private set

    /**
     * Builder to create a SizeStatistics.
     */
    open class Builder internal constructor(
        protected val type: PrimitiveType,
        maxRepetitionLevel: Int,
        maxDefinitionLevel: Int,
    ) {
        private var unencodedByteArrayDataBytes = 0L
        private val repetitionLevelHistogram: LongArray = if (maxRepetitionLevel > 0) {
            LongArray(maxRepetitionLevel + 1)
        } else {
            LongArray(0) // omitted
        }
        private val definitionLevelHistogram: LongArray = if (maxDefinitionLevel > 1) {
            LongArray(maxDefinitionLevel + 1)
        } else {
            LongArray(0) // omitted
        }

        /**
         * Add repetition and definition level of a value to the statistics.
         * It is called when value is null, or the column is not of BYTE_ARRAY type.
         *
         * @param repetitionLevel repetition level of the value
         * @param definitionLevel definition level of the value
         */
        open fun add(repetitionLevel: Int, definitionLevel: Int) {
            if (repetitionLevelHistogram.isNotEmpty()) {
                repetitionLevelHistogram[repetitionLevel]++
            }
            if (definitionLevelHistogram.isNotEmpty()) {
                definitionLevelHistogram[definitionLevel]++
            }
        }

        /**
         * Add repetition and definition level of a value to the statistics.
         * It is called when the column is of BYTE_ARRAY type.
         *
         * @param repetitionLevel repetition level of the value
         * @param definitionLevel definition level of the value
         * @param value value of to be added
         */
        open fun add(repetitionLevel: Int, definitionLevel: Int, value: Binary?) {
            add(repetitionLevel, definitionLevel)
            if (type.primitiveTypeName === PrimitiveType.PrimitiveTypeName.BINARY && value != null) {
                unencodedByteArrayDataBytes += value.length().toLong()
            }
        }

        /**
         * Build a SizeStatistics from the builder.
         */
        open fun build(): SizeStatistics {
            return SizeStatistics(
                type,
                unencodedByteArrayDataBytes,
                LongArrayList(repetitionLevelHistogram),
                LongArrayList(definitionLevelHistogram),
            )
        }
    }

    /**
     * Merge two SizeStatistics of the same column.
     * It is used to merge size statistics from all pages of the same column chunk.
     */
    fun mergeStatistics(other: SizeStatistics?) {
        if (!isValid) {
            return
        }

        // Stop merge if other is invalid.
        if (other == null || !other.isValid) {
            isValid = false
            unencodedByteArrayDataBytes = 0L
            repetitionLevelHistogram.clear()
            definitionLevelHistogram.clear()
            return
        }

        require(type == other.type) { "Cannot merge SizeStatistics of different types" }
        unencodedByteArrayDataBytes = Math.addExact(unencodedByteArrayDataBytes, other.unencodedByteArrayDataBytes)

        if (other.repetitionLevelHistogram.isEmpty()) {
            repetitionLevelHistogram.clear()
        } else {
            require(repetitionLevelHistogram.size == other.repetitionLevelHistogram.size) {
                "Cannot merge SizeStatistics with different repetition level histogram size"
            }
            for (i in repetitionLevelHistogram.indices) {
                repetitionLevelHistogram[i] = Math.addExact(repetitionLevelHistogram[i], other.repetitionLevelHistogram[i])
            }
        }

        if (other.definitionLevelHistogram.isEmpty()) {
            definitionLevelHistogram.clear()
        } else {
            require(definitionLevelHistogram.size == other.definitionLevelHistogram.size) {
                "Cannot merge SizeStatistics with different definition level histogram size"
            }
            for (i in definitionLevelHistogram.indices) {
                definitionLevelHistogram[i] = Math.addExact(definitionLevelHistogram[i], other.definitionLevelHistogram[i])
            }
        }
    }

    /**
     * The number of physical bytes stored for BYTE_ARRAY data values assuming
     * no encoding. This is exclusive of the bytes needed to store the length of
     * each byte array. In other words, this field is equivalent to the `(size
     * of PLAIN-ENCODING the byte array values) - (4 bytes * number of values
     * written)`. To determine unencoded sizes of other types readers can use
     * schema information multiplied by the number of non-null and null values.
     * The number of null/non-null values can be inferred from the histograms
     * below.
     *
     * For example, if a column chunk is dictionary-encoded with dictionary
     * ["a", "bc", "cde"], and a data page contains the indices [0, 0, 1, 2],
     * then this value for that data page should be 7 (1 + 1 + 2 + 3).
     *
     * This field should only be set for types that use BYTE_ARRAY as their
     * physical type.
     *
     * It represents the field `unencoded_byte_array_data_bytes` in the
     * [org.apache.parquet.format.SizeStatistics]
     *
     * @return unencoded and uncompressed byte size of the BYTE_ARRAY column,
     * or empty for other types.
     */
    fun getUnencodedByteArrayDataBytes(): OptionalLong {
        if (type.primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY) {
            return OptionalLong.of(unencodedByteArrayDataBytes)
        }
        return OptionalLong.empty()
    }

    /**
     * When present, there is expected to be one element corresponding to each
     * repetition (i.e. size=max repetition_level+1) where each element
     * represents the number of times the repetition level was observed in the
     * data.
     *
     * This field may be omitted if max_repetition_level is 0 without loss
     * of information.
     *
     * It represents the field `repetition_level_histogram` in the
     * [org.apache.parquet.format.SizeStatistics]
     *
     * @return repetition level histogram of all levels if not empty.
     */
    fun getRepetitionLevelHistogram(): List<Long> {
        return repetitionLevelHistogram.toList()
    }

    /**
     * Same as repetition_level_histogram except for definition levels.
     *
     * This field may be omitted if max_definition_level is 0 or 1 without
     * loss of information.
     *
     * It represents the field `definition_level_histogram` in the
     * [org.apache.parquet.format.SizeStatistics]
     *
     * @return definition level histogram of all levels if not empty.
     */
    fun getDefinitionLevelHistogram(): List<Long> {
        return definitionLevelHistogram.toList()
    }

    /**
     * @return a new independent statistics instance of this class.
     */
    fun copy(): SizeStatistics {
        return SizeStatistics(
            type,
            unencodedByteArrayDataBytes,
            LongArrayList(repetitionLevelHistogram),
            LongArrayList(definitionLevelHistogram),
        )
    }

    /**
     * Creates a no-op size statistics builder that collects no data.
     * Used when size statistics collection is disabled.
     */
    private class NoopBuilder(
        type: PrimitiveType,
        maxRepetitionLevel: Int,
        maxDefinitionLevel: Int,
    ) : Builder(type, maxRepetitionLevel, maxDefinitionLevel) {
        override fun add(repetitionLevel: Int, definitionLevel: Int) {
            // Do nothing
        }

        override fun add(repetitionLevel: Int, definitionLevel: Int, value: Binary?) {
            // Do nothing
        }

        override fun build(): SizeStatistics {
            val stats = SizeStatistics(type, 0L, mutableListOf<Long>(), mutableListOf<Long>())
            stats.isValid = false // Mark as invalid since this is a noop builder
            return stats
        }
    }

    companion object {
        /**
         * Create a builder to create a SizeStatistics.
         *
         * @param type physical type of the column associated with this statistics
         * @param maxRepetitionLevel maximum repetition level of the column
         * @param maxDefinitionLevel maximum definition level of the column
         */
        @JvmStatic
        fun newBuilder(type: PrimitiveType, maxRepetitionLevel: Int, maxDefinitionLevel: Int): Builder {
            return Builder(type, maxRepetitionLevel, maxDefinitionLevel)
        }

        /**
         * Creates a builder that doesn't collect any statistics.
         */
        fun noopBuilder(type: PrimitiveType, maxRepetitionLevel: Int, maxDefinitionLevel: Int): Builder {
            return NoopBuilder(type, maxRepetitionLevel, maxDefinitionLevel)
        }
    }
}
