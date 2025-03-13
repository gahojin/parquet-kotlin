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
import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.ColumnPath.Companion.get
import org.apache.parquet.io.PrimitiveColumnIO
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter

/**
 * See [FilteringRecordMaterializer]
 *
 * @param delegate the real converter
 * @param indexFieldPath the path, from the root of the schema, to this converter
 *                       used ultimately by the primitive converter proxy to figure
 *                       out which column it represents.
 * @param valueInspectorsByColumn for a given column, which nodes in the filter expression need to be notified of this column's value
 * @param columnIOsByIndexFieldPath used to go from our indexFieldPath to the PrimitiveColumnIO for that column
 */
class FilteringGroupConverter(
    private val delegate: GroupConverter,
    private val indexFieldPath: List<Int>,
    private val valueInspectorsByColumn: Map<ColumnPath, List<ValueInspector>>,
    private val columnIOsByIndexFieldPath: Map<List<Int>, PrimitiveColumnIO>,
) : GroupConverter() {

    // When a converter is asked for, we get the real one from the delegate, then wrap it
    // in a filtering pass-through proxy.
    // TODO: making the assumption that getConverter(i) is only called once, is that valid?
    override fun getConverter(fieldIndex: Int): Converter {
        // get the real converter from the delegate
        val delegateConverter = requireNotNull(delegate.getConverter(fieldIndex)) { "delegate converter cannot be null" }

        // determine the indexFieldPath for the converter proxy we're about to make, which is
        // this converter's path + the requested fieldIndex
        val newIndexFieldPath = indexFieldPath + fieldIndex

        return if (delegateConverter.isPrimitive) {
            val columnIO = getColumnIO(newIndexFieldPath)
            val columnPath = get(*columnIO.columnDescriptor.path)
            val valueInspectors = getValueInspectors(columnPath)
            FilteringPrimitiveConverter(delegateConverter.asPrimitiveConverter(), valueInspectors)
        } else {
            FilteringGroupConverter(
                delegateConverter.asGroupConverter(),
                newIndexFieldPath,
                valueInspectorsByColumn,
                columnIOsByIndexFieldPath,
            )
        }
    }

    private fun getColumnIO(indexFieldPath: List<Int>): PrimitiveColumnIO {
        val found = columnIOsByIndexFieldPath[indexFieldPath]
        return checkNotNull(found) { "Did not find PrimitiveColumnIO for index field path $indexFieldPath" }
    }

    private fun getValueInspectors(columnPath: ColumnPath): Array<ValueInspector> {
        val inspectorsList = valueInspectorsByColumn[columnPath]
        return inspectorsList?.toTypedArray<ValueInspector>() ?: emptyArray()
    }

    override fun start() {
        delegate.start()
    }

    override fun end() {
        delegate.end()
    }
}
