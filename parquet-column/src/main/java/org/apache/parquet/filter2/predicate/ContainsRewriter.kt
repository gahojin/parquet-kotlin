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
 * Recursively rewrites Contains predicates composed using And or Or into a single Contains predicate
 * containing all predicate assertions.
 *
 * This is a performance optimization, as all composed Contains sub-predicates must share the same column, and
 * can therefore be applied efficiently as a single predicate pass.
 */
class ContainsRewriter private constructor() : FilterPredicate.Visitor<FilterPredicate> {
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
        val left = if (and.left is Operators.And) {
            visit(and.left as Operators.And)
        } else if (and.left is Operators.Or) {
            visit(and.left as Operators.Or)
        } else and.left as? Operators.Contains<*> ?: and.left

        val right = if (and.right is Operators.And) {
            visit(and.right as Operators.And)
        } else if (and.right is Operators.Or) {
            visit(and.right as Operators.Or)
        } else and.right as? Operators.Contains<*> ?: and.right

        // If two Contains predicates refer to the same column, optimize by combining into a single predicate
        return if ((left is Operators.Contains<*> && right is Operators.Contains<*>)
            && left.getColumn()
                .columnPath
                .equals(right.getColumn().columnPath)
        ) {
            left.and(right)
        } else if (left !== and.left || right !== and.right) {
            Operators.And(left, right)
        } else {
            and
        }
    }

    override fun visit(or: Operators.Or): FilterPredicate {
        val left = if (or.left is Operators.And) {
            visit(or.left as Operators.And)
        } else if (or.left is Operators.Or) {
            visit(or.left as Operators.Or)
        } else or.left as? Operators.Contains<*> ?: or.left

        val right = if (or.right is Operators.And) {
            visit(or.right as Operators.And)
        } else if (or.right is Operators.Or) {
            visit(or.right as Operators.Or)
        } else or.right as? Operators.Contains<*> ?: or.right

        // If two Contains predicates refer to the same column, optimize by combining into a single predicate
        return if ((left is Operators.Contains<*> && right is Operators.Contains<*>)
            && left.getColumn()
                .columnPath
                .equals(right.getColumn().columnPath)
        ) {
            left.or(right)
        } else if (left !== or.left || right !== or.right) {
            Operators.Or(left, right)
        } else {
            or
        }
    }

    override fun visit(not: Operators.Not): FilterPredicate {
        error("Not predicate should be rewritten before being evaluated by ContainsRewriter")
    }

    override fun <T : Comparable<T>, U : UserDefinedPredicate<T>> visit(udp: UserDefined<T, U>): FilterPredicate = udp

    override fun <T : Comparable<T>, U : UserDefinedPredicate<T>> visit(udp: LogicalNotUserDefined<T, U>): FilterPredicate = udp

    companion object {
        private val INSTANCE = ContainsRewriter()

        @JvmStatic
        fun rewrite(pred: FilterPredicate): FilterPredicate {
            return pred.accept(INSTANCE)
        }
    }
}
