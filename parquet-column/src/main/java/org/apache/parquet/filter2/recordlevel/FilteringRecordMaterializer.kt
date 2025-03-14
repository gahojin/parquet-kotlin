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
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateEvaluator.Companion.evaluate
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateResetter.Companion.reset
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.io.PrimitiveColumnIO
import org.apache.parquet.io.api.RecordMaterializer

/**
 * A pass-through proxy for a [RecordMaterializer] that updates a [IncrementallyUpdatedFilterPredicate]
 * as it receives concrete values for the current record. If, after the record assembly signals that
 * there are no more values, the predicate indicates that this record should be dropped, [.getCurrentRecord]
 * returns null to signal that this record is being skipped.
 * Otherwise, the record is retrieved from the delegate.
 *
 * @param delegate valueInspectorsByColumn
 * @param filterPredicate the predicate
 */
class FilteringRecordMaterializer<T>(
    private val delegate: RecordMaterializer<T>,
    columnIOs: List<PrimitiveColumnIO>,
    valueInspectorsByColumn: Map<ColumnPath, List<ValueInspector>>,
    private val filterPredicate: IncrementallyUpdatedFilterPredicate,
) : RecordMaterializer<T>() {
    // create a proxy for the delegate's root converter
    override val rootConverter = FilteringGroupConverter(
        delegate.rootConverter,
        emptyList(),
        valueInspectorsByColumn,
        // keep track of which path of indices leads to which primitive column
        columnIOs.associateBy { it.indexFieldPath.toList() },
    )

    override val currentRecord: T?
        get() {
            // find out if the predicate thinks we should keep this record
            val keep = evaluate(filterPredicate)

            // reset the stateful predicate no matter what
            reset(filterPredicate)

            // null - signals a skip
            return if (keep) delegate.currentRecord else null
        }

    override fun skipCurrentRecord() {
        delegate.skipCurrentRecord()
    }

    companion object {
        // The following two methods are kept for backward compatibility
        @Deprecated("")
        fun getIndexFieldPathList(c: PrimitiveColumnIO): List<Int> {
            return intArrayToList(c.indexFieldPath)
        }

        @Deprecated("")
        fun intArrayToList(arr: IntArray): List<Int> {
            return arr.toList()
        }
    }
}
