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
 * Recursively removes all use of the not() operator in a predicate
 * by replacing all instances of not(x) with the inverse(x),
 * eg: not(and(eq(), not(eq(y))) -&gt; or(notEq(), eq(y))
 *
 * The returned predicate should have the same meaning as the original, but
 * without the use of the not() operator.
 *
 * See also [LogicalInverter], which is used
 * to do the inversion.
 */
class LogicalInverseRewriter private constructor() : FilterPredicate.Visitor<FilterPredicate> {
    override fun <T : Comparable<T>> visit(eq: Operators.Eq<T>): FilterPredicate = eq

    override fun <T : Comparable<T>> visit(notEq: NotEq<T>): FilterPredicate = notEq

    override fun <T : Comparable<T>> visit(lt: Operators.Lt<T>): FilterPredicate = lt

    override fun <T : Comparable<T>> visit(ltEq: LtEq<T>): FilterPredicate = ltEq

    override fun <T : Comparable<T>> visit(gt: Operators.Gt<T>): FilterPredicate = gt

    override fun <T : Comparable<T>> visit(gtEq: GtEq<T>): FilterPredicate = gtEq

    override fun <T : Comparable<T>> visit(`in`: In<T>): FilterPredicate = `in`

    override fun <T : Comparable<T>> visit(notIn: NotIn<T>): FilterPredicate = notIn

    override fun <T : Comparable<T>> visit(contains: Operators.Contains<T>): FilterPredicate = contains

    override fun visit(and: Operators.And): FilterPredicate {
        return FilterApi.and(and.left.accept(this), and.right.accept(this))
    }

    override fun visit(or: Operators.Or): FilterPredicate {
        return FilterApi.or(or.left.accept(this), or.right.accept(this))
    }

    override fun visit(not: Operators.Not): FilterPredicate {
        return LogicalInverter.invert(not.predicate.accept(this))
    }

    override fun <T : Comparable<T>, U : UserDefinedPredicate<T>> visit(udp: UserDefined<T, U>): FilterPredicate = udp

    override fun <T : Comparable<T>, U : UserDefinedPredicate<T>> visit(udp: LogicalNotUserDefined<T, U>): FilterPredicate = udp

    companion object {
        private val INSTANCE = LogicalInverseRewriter()

        @JvmStatic
        fun rewrite(pred: FilterPredicate): FilterPredicate {
            return pred.accept(INSTANCE)
        }
    }
}
