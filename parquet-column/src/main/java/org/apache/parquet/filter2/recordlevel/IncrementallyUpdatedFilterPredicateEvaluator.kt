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

import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector

/**
 * Determines whether an [IncrementallyUpdatedFilterPredicate] is satisfied or not.
 * This implementation makes the assumption that all [ValueInspector]s in an unknown state
 * represent columns with a null value, and updates them accordingly.
 *
 * TODO: We could also build an evaluator that detects if enough values are known to determine the outcome
 * TODO: of the predicate and quit the record assembly early. (https://issues.apache.org/jira/browse/PARQUET-37)
 */
class IncrementallyUpdatedFilterPredicateEvaluator private constructor() : IncrementallyUpdatedFilterPredicate.Visitor {
    override fun visit(p: ValueInspector): Boolean {
        if (!p.isKnown) {
            p.updateNull()
        }
        return p.getResult()
    }

    override fun visit(and: IncrementallyUpdatedFilterPredicate.And): Boolean {
        return and.left.accept(this) && and.right.accept(this)
    }

    override fun visit(or: IncrementallyUpdatedFilterPredicate.Or): Boolean {
        return or.left.accept(this) || or.right.accept(this)
    }

    companion object {
        private val INSTANCE = IncrementallyUpdatedFilterPredicateEvaluator()

        @JvmStatic
        fun evaluate(pred: IncrementallyUpdatedFilterPredicate): Boolean {
            return pred.accept(INSTANCE)
        }
    }
}
