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
 * Converts a [FilterPredicate] to its logical inverse.
 * The returned predicate should be equivalent to not(p), but without
 * the use of a not() operator.
 *
 *
 * See also [LogicalInverseRewriter], which can remove the use
 * of all not() operators without inverting the overall predicate.
 */
class LogicalInverter private constructor() : FilterPredicate.Visitor<FilterPredicate> {
    override fun <T : Comparable<T>> visit(eq: Operators.Eq<T>): FilterPredicate {
        return NotEq(eq.getColumn(), eq.getValue())
    }

    override fun <T : Comparable<T>> visit(notEq: NotEq<T>): FilterPredicate {
        return Operators.Eq(notEq.getColumn(), notEq.getValue())
    }

    override fun <T : Comparable<T>> visit(lt: Operators.Lt<T>): FilterPredicate {
        return GtEq(lt.getColumn(), lt.getValue())
    }

    override fun <T : Comparable<T>> visit(ltEq: LtEq<T>): FilterPredicate {
        return Operators.Gt(ltEq.getColumn(), ltEq.getValue())
    }

    override fun <T : Comparable<T>> visit(gt: Operators.Gt<T>): FilterPredicate {
        return LtEq(gt.getColumn(), gt.getValue())
    }

    override fun <T : Comparable<T>> visit(gtEq: GtEq<T>): FilterPredicate {
        return Operators.Lt(gtEq.getColumn(), gtEq.getValue())
    }

    override fun <T : Comparable<T>> visit(`in`: In<T>): FilterPredicate {
        return NotIn(`in`.getColumn(), `in`.getValues())
    }

    override fun <T : Comparable<T>> visit(notIn: NotIn<T>): FilterPredicate {
        return In(notIn.getColumn(), notIn.getValues())
    }

    override fun <T : Comparable<T>> visit(contains: Operators.Contains<T>): FilterPredicate {
        return contains.not()
    }

    override fun visit(and: Operators.And): FilterPredicate {
        return Operators.Or(and.left.accept(this), and.right.accept(this))
    }

    override fun visit(or: Operators.Or): FilterPredicate {
        return Operators.And(or.left.accept(this), or.right.accept(this))
    }

    override fun visit(not: Operators.Not): FilterPredicate {
        return not.predicate
    }

    override fun <T : Comparable<T>, U : UserDefinedPredicate<T>> visit(udp: UserDefined<T, U>): FilterPredicate {
        return LogicalNotUserDefined<T?, U?>(udp)
    }

    override fun <T : Comparable<T>, U : UserDefinedPredicate<T>> visit(udp: LogicalNotUserDefined<T, U>): FilterPredicate {
        return udp.getUserDefined()
    }

    companion object {
        private val INSTANCE = LogicalInverter()

        @JvmStatic
        fun invert(pred: FilterPredicate): FilterPredicate {
            return pred.accept(INSTANCE)
        }
    }
}
