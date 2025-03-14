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
 * Resets all the [ValueInspector]s in a [IncrementallyUpdatedFilterPredicate].
 */
class IncrementallyUpdatedFilterPredicateResetter private constructor() : IncrementallyUpdatedFilterPredicate.Visitor {
    override fun visit(p: ValueInspector): Boolean {
        p.reset()
        return false
    }

    override fun visit(and: IncrementallyUpdatedFilterPredicate.And): Boolean {
        and.left.accept(this)
        and.right.accept(this)
        return false
    }

    override fun visit(or: IncrementallyUpdatedFilterPredicate.Or): Boolean {
        or.left.accept(this)
        or.right.accept(this)
        return false
    }

    companion object {
        private val INSTANCE = IncrementallyUpdatedFilterPredicateResetter()

        @JvmStatic
        fun reset(pred: IncrementallyUpdatedFilterPredicate) {
            pred.accept(INSTANCE)
        }
    }
}
