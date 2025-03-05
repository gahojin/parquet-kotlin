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
package org.apache.parquet.internal.column.columnindex

import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder.ColumnIndexBase
import java.util.PrimitiveIterator
import java.util.function.IntPredicate
import java.util.function.IntUnaryOperator

/**
 * Enum for [org.apache.parquet.format.BoundaryOrder]. It also contains the implementations of searching for
 * matching page indexes for column index based filtering.
 */
enum class BoundaryOrder {
    UNORDERED {
        override fun eq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            return IndexIterator.filterTranslate(
                comparator.arrayLength(),
                IntPredicate { arrayIndex: Int ->
                    comparator.compareValueToMin(arrayIndex) >= 0
                            && comparator.compareValueToMax(arrayIndex) <= 0
                },
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun gt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            return IndexIterator.filterTranslate(
                comparator.arrayLength(),
                IntPredicate { arrayIndex: Int -> comparator.compareValueToMax(arrayIndex) < 0 },
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun gtEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            return IndexIterator.filterTranslate(
                comparator.arrayLength(),
                IntPredicate { arrayIndex: Int -> comparator.compareValueToMax(arrayIndex) <= 0 },
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun lt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            return IndexIterator.filterTranslate(
                comparator.arrayLength(),
                IntPredicate { arrayIndex: Int -> comparator.compareValueToMin(arrayIndex) > 0 },
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun ltEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            return IndexIterator.filterTranslate(
                comparator.arrayLength(),
                IntPredicate { arrayIndex: Int -> comparator.compareValueToMin(arrayIndex) >= 0 },
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun notEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            return IndexIterator.filterTranslate(comparator.arrayLength(), {
                comparator.compareValueToMin(it) != 0 || comparator.compareValueToMax(it) != 0
            }, { comparator.translate(it) })
        }
    },
    ASCENDING {
        override fun eq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val bounds = findBounds(comparator)
            if (bounds == null) {
                return IndexIterator.EMPTY
            }
            return IndexIterator.rangeTranslate(bounds.lower, bounds.upper) {
                comparator.translate(it)
            }
        }

        override fun gt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = 0
            var right = length
            do {
                val i: Int = BoundaryOrder.Companion.floorMid(left, right)
                if (comparator.compareValueToMax(i) >= 0) {
                    left = i + 1
                } else {
                    right = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                right,
                length - 1,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun gtEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = 0
            var right = length
            do {
                val i: Int = BoundaryOrder.Companion.floorMid(left, right)
                if (comparator.compareValueToMax(i) > 0) {
                    left = i + 1
                } else {
                    right = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                right,
                length - 1,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun lt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = -1
            var right = length - 1
            do {
                val i: Int = BoundaryOrder.Companion.ceilingMid(left, right)
                if (comparator.compareValueToMin(i) <= 0) {
                    right = i - 1
                } else {
                    left = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                0,
                left,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun ltEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = -1
            var right = length - 1
            do {
                val i: Int = BoundaryOrder.Companion.ceilingMid(left, right)
                if (comparator.compareValueToMin(i) < 0) {
                    right = i - 1
                } else {
                    left = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                0,
                left,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun notEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val bounds = findBounds(comparator)
            val length: Int = comparator.arrayLength()
            if (bounds == null) {
                return IndexIterator.all(comparator)
            }
            return IndexIterator.filterTranslate(
                length,
                IntPredicate { i: Int ->
                    i < bounds.lower || i > bounds.upper || comparator.compareValueToMin(i) != 0 || comparator.compareValueToMax(
                        i
                    ) != 0
                },
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        private fun findBounds(comparator: ColumnIndexBase<*>.ValueComparator): Bounds? {
            val length: Int = comparator.arrayLength()
            var lowerLeft = 0
            var upperLeft = 0
            var lowerRight = length - 1
            var upperRight = length - 1
            do {
                if (lowerLeft > lowerRight) {
                    return null
                }
                val i: Int = BoundaryOrder.Companion.floorMid(lowerLeft, lowerRight)
                if (comparator.compareValueToMin(i) < 0) {
                    upperRight = i - 1
                    lowerRight = upperRight
                } else if (comparator.compareValueToMax(i) > 0) {
                    upperLeft = i + 1
                    lowerLeft = upperLeft
                } else {
                    upperLeft = i
                    lowerRight = upperLeft
                }
            } while (lowerLeft != lowerRight)
            do {
                if (upperLeft > upperRight) {
                    return null
                }
                val i: Int = BoundaryOrder.Companion.ceilingMid(upperLeft, upperRight)
                if (comparator.compareValueToMin(i) < 0) {
                    upperRight = i - 1
                } else if (comparator.compareValueToMax(i) > 0) {
                    upperLeft = i + 1
                } else {
                    upperLeft = i
                }
            } while (upperLeft != upperRight)
            return Bounds(lowerLeft, upperRight)
        }
    },
    DESCENDING {
        override fun eq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val bounds = findBounds(comparator)
            if (bounds == null) {
                return IndexIterator.EMPTY
            }
            return IndexIterator.rangeTranslate(bounds.lower, bounds.upper,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun gt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = -1
            var right = length - 1
            do {
                val i: Int = BoundaryOrder.Companion.ceilingMid(left, right)
                if (comparator.compareValueToMax(i) >= 0) {
                    right = i - 1
                } else {
                    left = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                0,
                left,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun gtEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = -1
            var right = length - 1
            do {
                val i: Int = BoundaryOrder.Companion.ceilingMid(left, right)
                if (comparator.compareValueToMax(i) > 0) {
                    right = i - 1
                } else {
                    left = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                0,
                left,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun lt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = 0
            var right = length
            do {
                val i: Int = BoundaryOrder.Companion.floorMid(left, right)
                if (comparator.compareValueToMin(i) <= 0) {
                    left = i + 1
                } else {
                    right = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                right,
                length - 1,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun ltEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val length: Int = comparator.arrayLength()
            if (length == 0) {
                // No matching rows if the column index contains null pages only
                return IndexIterator.EMPTY
            }
            var left = 0
            var right = length
            do {
                val i: Int = BoundaryOrder.Companion.floorMid(left, right)
                if (comparator.compareValueToMin(i) < 0) {
                    left = i + 1
                } else {
                    right = i
                }
            } while (left < right)
            return IndexIterator.rangeTranslate(
                right,
                length - 1,
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        override fun notEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt {
            val bounds = findBounds(comparator)
            val length: Int = comparator.arrayLength()
            if (bounds == null) {
                return IndexIterator.all(comparator)
            }
            return IndexIterator.filterTranslate(
                length,
                IntPredicate { i: Int ->
                    i < bounds.lower || i > bounds.upper || comparator.compareValueToMin(i) != 0 || comparator.compareValueToMax(
                        i
                    ) != 0
                },
                IntUnaryOperator { arrayIndex: Int -> comparator.translate(arrayIndex) })
        }

        private fun findBounds(comparator: ColumnIndexBase<*>.ValueComparator): Bounds? {
            val length: Int = comparator.arrayLength()
            var lowerLeft = 0
            var upperLeft = 0
            var lowerRight = length - 1
            var upperRight = length - 1
            do {
                if (lowerLeft > lowerRight) {
                    return null
                }
                val i: Int = BoundaryOrder.Companion.floorMid(lowerLeft, lowerRight)
                if (comparator.compareValueToMax(i) > 0) {
                    upperRight = i - 1
                    lowerRight = upperRight
                } else if (comparator.compareValueToMin(i) < 0) {
                    upperLeft = i + 1
                    lowerLeft = upperLeft
                } else {
                    upperLeft = i
                    lowerRight = upperLeft
                }
            } while (lowerLeft != lowerRight)
            do {
                if (upperLeft > upperRight) {
                    return null
                }
                val i: Int = BoundaryOrder.Companion.ceilingMid(upperLeft, upperRight)
                if (comparator.compareValueToMax(i) > 0) {
                    upperRight = i - 1
                } else if (comparator.compareValueToMin(i) < 0) {
                    upperLeft = i + 1
                } else {
                    upperLeft = i
                }
            } while (upperLeft != upperRight)
            return Bounds(lowerLeft, upperRight)
        }
    };

    private class Bounds(lower: Int, upper: Int) {
        val lower: Int
        val upper: Int

        init {
            assert(lower <= upper)
            this.lower = lower
            this.upper = upper
        }
    }

    internal abstract fun eq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt

    internal abstract fun gt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt

    internal abstract fun gtEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt

    internal abstract fun lt(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt

    internal abstract fun ltEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt

    internal abstract fun notEq(comparator: ColumnIndexBase<*>.ValueComparator): PrimitiveIterator.OfInt

    companion object {
        private fun floorMid(left: Int, right: Int): Int {
            // Avoid the possible overflow might happen in case of (left + right) / 2
            return left + ((right - left) / 2)
        }

        private fun ceilingMid(left: Int, right: Int): Int {
            // Avoid the possible overflow might happen in case of (left + right + 1) / 2
            return left + ((right - left + 1) / 2)
        }
    }
}
