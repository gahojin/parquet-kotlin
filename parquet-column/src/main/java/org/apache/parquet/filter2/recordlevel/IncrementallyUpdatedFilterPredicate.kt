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
package org.apache.parquet.filter2.recordlevel

import org.apache.parquet.io.api.Binary

/**
 * A rewritten version of a [org.apache.parquet.filter2.predicate.FilterPredicate] which receives
 * the values for a record's columns one by one and internally tracks whether the predicate is
 * satisfied, unsatisfied, or unknown.
 *
 * This is used to apply a predicate during record assembly, without assembling a second copy of
 * a record, and without building a stack of update events.
 *
 * IncrementallyUpdatedFilterPredicate is implemented via the visitor pattern, as is
 * [org.apache.parquet.filter2.predicate.FilterPredicate]
 */
interface IncrementallyUpdatedFilterPredicate {
    /**
     * A Visitor for an [IncrementallyUpdatedFilterPredicate], per the visitor pattern.
     */
    interface Visitor {
        fun visit(p: ValueInspector): Boolean

        fun visit(and: And): Boolean

        fun visit(or: Or): Boolean
    }

    /**
     * A [IncrementallyUpdatedFilterPredicate] must accept a [Visitor], per the visitor pattern.
     *
     * @param visitor a Visitor
     * @return the result of this predicate
     */
    fun accept(visitor: Visitor): Boolean

    /**
     * This is the leaf node of a filter predicate. It receives the value for the primitive column it represents,
     * and decides whether or not the predicate represented by this node is satisfied.
     *
     * It is stateful, and needs to be rest after use.
     */
    open class ValueInspector internal constructor() : IncrementallyUpdatedFilterPredicate {
        internal var result = false

        /**
         * Return true if this inspector has received a value yet, false otherwise.
         *
         * @return true if the value of this predicate has been determined
         */
        var isKnown: Boolean = false
            private set

        // these methods signal what the value is
        open fun updateNull() {
            throw UnsupportedOperationException()
        }

        open fun update(value: Int) {
            throw UnsupportedOperationException()
        }

        open fun update(value: Long) {
            throw UnsupportedOperationException()
        }

        open fun update(value: Double) {
            throw UnsupportedOperationException()
        }

        open fun update(value: Float) {
            throw UnsupportedOperationException()
        }

        open fun update(value: Boolean) {
            throw UnsupportedOperationException()
        }

        open fun update(value: Binary) {
            throw UnsupportedOperationException()
        }

        /**
         * Reset to clear state and begin evaluating the next record.
         */
        open fun reset() {
            isKnown = false
            result = false
        }

        /**
         * Subclasses should call this method to signal that the result of this predicate is known.
         *
         * @param result the result of this predicate, when it is determined
         */
        protected fun setResult(result: Boolean) {
            check(!isKnown) {
                "setResult() called on a ValueInspector whose result is already known! Did you forget to call reset()?"
            }
            this.result = result
            isKnown = true
        }

        /**
         * Should only be called if [.isKnown] return true.
         *
         * @return the result of this predicate
         */
        fun getResult(): Boolean {
            check(isKnown) { "getResult() called on a ValueInspector whose result is not yet known!" }
            return result
        }

        override fun accept(visitor: Visitor): Boolean {
            return visitor.visit(this)
        }
    }

    /**
     * A ValueInspector implementation that keeps state for one or more delegate inspectors.
     */
    abstract class DelegatingValueInspector internal constructor(vararg delegates: ValueInspector) : ValueInspector() {
        val delegates: Iterable<ValueInspector> = delegates.asList()

        /**
         * Hook called after every value update. Can update state and set result on this ValueInspector.
         */
        abstract fun onUpdate()

        /**
         * Hook called after updateNull(), if no values have been recorded for the delegate inspectors.
         */
        abstract fun onNull()

        override fun updateNull() {
            for (delegate in delegates) {
                if (!delegate.isKnown) {
                    delegate.updateNull()
                }
            }
            onNull()
        }

        override fun update(value: Int) {
            delegates.forEach { it.update(value) }
            onUpdate()
        }

        override fun update(value: Long) {
            delegates.forEach { it.update(value) }
            onUpdate()
        }

        override fun update(value: Boolean) {
            delegates.forEach { it.update(value) }
            onUpdate()
        }

        override fun update(value: Float) {
            delegates.forEach { it.update(value) }
            onUpdate()
        }

        override fun update(value: Double) {
            delegates.forEach { it.update(value) }
            onUpdate()
        }

        override fun update(value: Binary) {
            delegates.forEach { it.update(value) }
            onUpdate()
        }

        override fun reset() {
            delegates.forEach { it.reset() }
            super.reset()
        }
    }

    // base class for and / or
    abstract class BinaryLogical internal constructor(
        val left: IncrementallyUpdatedFilterPredicate,
        val right: IncrementallyUpdatedFilterPredicate,
    ) : IncrementallyUpdatedFilterPredicate

    class Or internal constructor(
        left: IncrementallyUpdatedFilterPredicate,
        right: IncrementallyUpdatedFilterPredicate,
    ) : BinaryLogical(left, right) {
        override fun accept(visitor: Visitor): Boolean {
            return visitor.visit(this)
        }
    }

    class And internal constructor(
        left: IncrementallyUpdatedFilterPredicate,
        right: IncrementallyUpdatedFilterPredicate,
    ) : BinaryLogical(left, right) {
        override fun accept(visitor: Visitor): Boolean {
            return visitor.visit(this)
        }
    }
}
