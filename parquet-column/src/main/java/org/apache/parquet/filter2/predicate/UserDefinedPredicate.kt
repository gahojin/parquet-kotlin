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
package org.apache.parquet.filter2.predicate

/**
 * A UserDefinedPredicate decides whether a record should be kept or dropped, first by
 * inspecting meta data about a group of records to see if the entire group can be dropped,
 * then by inspecting actual values of a single column. These predicates can be combined into
 * a complex boolean expression via the [FilterApi].
 *
 * @param <T> The type of the column this predicate is applied to.
 */
// TODO: consider avoiding autoboxing and adding the specialized methods for each type
// TODO: downside is that's fairly unwieldy for users
abstract class UserDefinedPredicate<T : Comparable<T>> {
    /**
     * Returns whether this predicate accepts `null` values.
     *
     * @return `true` if this predicate accepts `null` values, `false` otherwise
     */
    fun acceptsNullValue(): Boolean {
        return try {
            keep(null)
        } catch (_: NullPointerException) {
            // The implementor might not be prepared to handle null values;
            // in this case this predicate obviously does not accept nulls
            false
        }
    }

    /**
     * Return true to keep the record with this value, false to drop it.
     *
     * This method shall handle `null` values returning whether this user defined predicate accepts `null`
     * values or not.
     *
     * @param value a value (might be `null`)
     * @return true to keep the record with the value, false to drop it
     */
    abstract fun keep(value: T?): Boolean

    /**
     * Given information about a group of records (eg, the min and max value)
     * Return true to drop all the records in this group, false to keep them for further
     * inspection. Returning false here will cause the records to be loaded and each value
     * will be passed to [.keep] to make the final decision.
     *
     * It is safe to always return false here, if you simply want to visit each record via the [.keep] method,
     * though it is much more efficient to drop entire chunks of records here if you can.
     *
     * @param statistics statistics for the column
     * @return true if none of the values described by statistics can match the predicate
     */
    abstract fun canDrop(statistics: Statistics<T>): Boolean

    /**
     * Same as [.canDrop] except this method describes the logical inverse
     * behavior of this predicate. If this predicate is passed to the not() operator, then
     * this method will be called instead of [.canDrop]
     *
     * It is safe to always return false here, if you simply want to visit each record via the [.keep] method,
     * though it is much more efficient to drop entire chunks of records here if you can.
     *
     * It may be valid to simply return !canDrop(statistics) but that is not always the case.
     * To illustrate, look at this re-implementation of a UDP that checks for values greater than 7:
     *
     * ```
     * // This is just an example, you should use the built in {@link FilterApi#gt} operator instead of
     * // implementing your own like this.
     *
     * public class IntGreaterThan7UDP extends UserDefinedPredicate&lt;Integer&gt; {
     *   public boolean keep(Integer value) {
     *     // here we just check if the value is greater than 7.
     *     // here, parquet knows that if the predicate not(columnX, IntGreaterThan7UDP) is being evaluated,
     *     // it is safe to simply use !IntEquals7UDP.keep(value)
     *     return value &gt; 7;
     *   }
     *
     *   public boolean canDrop(Statistics&lt;Integer&gt; statistics) {
     *     // here we drop a group of records if they are all less than or equal to 7,
     *     // (there can't possibly be any values greater than 7 in this group of records)
     *     return statistics.getMax() &lt;= 7;
     *   }
     *
     *   public boolean inverseCanDrop(Statistics&lt;Integer&gt; statistics) {
     *     // here the predicate not(columnX, IntGreaterThan7UDP) is being evaluated, which means we want
     *     // to keep all records whose value is is not greater than 7, or, rephrased, whose value is less than or equal to 7.
     *     // notice what would happen if parquet just tried to evaluate !IntGreaterThan7UDP.canDrop():
     *     // !IntGreaterThan7UDP.canDrop(stats) == !(stats.getMax() &lt;= 7) == (stats.getMax() &lt; 7)
     *     // it would drop the following group of records: [100, 1, 2, 3], even though this group of records contains values
     *     // less than than or equal to 7.
     *
     *     // what we actually want to do is drop groups of records where the *min* is greater than 7, (not the max)
     *     // for example: the group of records: [100, 8, 9, 10] has a min of 8, so there's no way there are going
     *     // to be records with a value
     *     // less than or equal to 7 in this group.
     *     return statistics.getMin() &gt; 7;
     *   }
     * }
     * ```
     *
     * @param statistics statistics for the column
     * @return false if none of the values described by statistics can match the predicate
     */
    abstract fun inverseCanDrop(statistics: Statistics<T>): Boolean
}
