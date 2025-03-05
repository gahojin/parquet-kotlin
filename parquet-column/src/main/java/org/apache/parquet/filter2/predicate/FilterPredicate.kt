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

import org.apache.parquet.filter2.predicate.Operators.GtEq
import org.apache.parquet.filter2.predicate.Operators.In
import org.apache.parquet.filter2.predicate.Operators.LogicalNotUserDefined
import org.apache.parquet.filter2.predicate.Operators.LtEq
import org.apache.parquet.filter2.predicate.Operators.NotEq
import org.apache.parquet.filter2.predicate.Operators.NotIn
import org.apache.parquet.filter2.predicate.Operators.UserDefined

/**
 * A FilterPredicate is an expression tree describing the criteria for which records to keep when loading data from
 * a parquet file. These predicates are applied in multiple places. Currently, they are applied to all row groups at
 * job submission time to see if we can potentially drop entire row groups, and then they are applied during column
 * assembly to drop individual records that are not wanted.
 *
 * FilterPredicates do not contain closures or instances of anonymous classes, rather they are expressed as
 * an expression tree of operators.
 *
 * FilterPredicates are implemented in terms of the visitor pattern.
 *
 * See [Operators] for the implementation of the operator tokens,
 * and [FilterApi] for the dsl functions for constructing an expression tree.
 */
interface FilterPredicate {
    /**
     * A FilterPredicate must accept a Visitor, per the visitor pattern.
     *
     * @param visitor a visitor
     * @param <R>     return type of the visitor
     * @return the return value of Visitor#visit(this)
     */
    fun <R> accept(visitor: Visitor<R>): R

    /**
     * A FilterPredicate Visitor must visit all the operators in a FilterPredicate expression tree,
     * and must handle recursion itself, per the visitor pattern.
     *
     * @param <R> return type of the visitor
     */
    interface Visitor<R> {
        fun <T : Comparable<@JvmSuppressWildcards T>> visit(eq: Operators.Eq<T>): R

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(notEq: NotEq<T>): R

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(lt: Operators.Lt<T>): R

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(ltEq: LtEq<T>): R

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(gt: Operators.Gt<T>): R

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(gtEq: GtEq<T>): R

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(`in`: In<T>): R {
            throw UnsupportedOperationException("visit in is not supported.")
        }

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(notIn: NotIn<T>): R {
            throw UnsupportedOperationException("visit NotIn is not supported.")
        }

        fun <T : Comparable<@JvmSuppressWildcards T>> visit(contains: Operators.Contains<T>): R {
            throw UnsupportedOperationException("visit Contains is not supported.")
        }

        fun visit(and: Operators.And): R

        fun visit(or: Operators.Or): R

        fun visit(not: Operators.Not): R

        fun <T : Comparable<@JvmSuppressWildcards T>, U : UserDefinedPredicate<T>> visit(udp: UserDefined<T, U>): R

        fun <T : Comparable<@JvmSuppressWildcards T>, U : UserDefinedPredicate<T>> visit(udp: LogicalNotUserDefined<T, U>): R
    }
}
