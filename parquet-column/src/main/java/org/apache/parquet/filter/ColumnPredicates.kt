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
package org.apache.parquet.filter

import org.apache.parquet.column.ColumnReader
import org.apache.parquet.io.api.Binary

/**
 * ColumnPredicates class provides checks for column values. Factory methods
 * are provided for standard predicates which wrap the job of getting the
 * correct value from the column.
 */
object ColumnPredicates {
    @JvmStatic
    fun equalTo(target: String): Predicate {
        return Predicate { target == it.binary.toStringUsingUTF8() }
    }

    @JvmStatic
    fun applyFunctionToString(fn: PredicateFunction<String>): Predicate {
        return Predicate { fn.functionToApply(it.binary.toStringUsingUTF8()) }
    }

    @JvmStatic
    fun equalTo(target: Int): Predicate {
        return Predicate { it.integer == target }
    }

    @JvmStatic
    fun applyFunctionToInteger(fn: IntegerPredicateFunction): Predicate {
        return Predicate { fn.functionToApply(it.integer) }
    }

    @JvmStatic
    fun equalTo(target: Long): Predicate {
        return Predicate { it.long == target }
    }

    @JvmStatic
    fun applyFunctionToLong(fn: LongPredicateFunction): Predicate {
        return Predicate { fn.functionToApply(it.long) }
    }

    @JvmStatic
    fun equalTo(target: Float): Predicate {
        return Predicate { it.float == target }
    }

    @JvmStatic
    fun applyFunctionToFloat(fn: FloatPredicateFunction): Predicate {
        return Predicate { fn.functionToApply(it.float) }
    }

    @JvmStatic
    fun equalTo(target: Double): Predicate {
        return Predicate { it.double == target }
    }

    @JvmStatic
    fun applyFunctionToDouble(fn: DoublePredicateFunction): Predicate {
        return Predicate { fn.functionToApply(it.double) }
    }

    @JvmStatic
    fun equalTo(target: Boolean): Predicate {
        return Predicate { it.boolean == target }
    }

    @JvmStatic
    fun applyFunctionToBoolean(fn: BooleanPredicateFunction): Predicate {
        return Predicate { fn.functionToApply(it.boolean) }
    }

    @JvmStatic
    fun <E : Enum<*>> equalTo(target: E): Predicate {
        return Predicate { target.name == it.binary.toStringUsingUTF8() }
    }

    @JvmStatic
    fun applyFunctionToBinary(fn: PredicateFunction<Binary>): Predicate {
        return Predicate { fn.functionToApply(it.binary) }
    }

    fun interface Predicate {
        fun apply(input: ColumnReader): Boolean
    }

    fun interface PredicateFunction<T> {
        fun functionToApply(input: T): Boolean
    }

    /* provide the following to avoid boxing primitives */
    fun interface IntegerPredicateFunction {
        fun functionToApply(input: Int): Boolean
    }

    fun interface LongPredicateFunction {
        fun functionToApply(input: Long): Boolean
    }

    fun interface FloatPredicateFunction {
        fun functionToApply(input: Float): Boolean
    }

    fun interface DoublePredicateFunction {
        fun functionToApply(input: Double): Boolean
    }

    fun interface BooleanPredicateFunction {
        fun functionToApply(input: Boolean): Boolean
    }
}
