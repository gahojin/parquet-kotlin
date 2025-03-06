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
package org.apache.parquet.io

import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.Type.Repetition

/**
 * Group level of the IO structure
 */
open class GroupColumnIO internal constructor(
    groupType: GroupType,
    parent: GroupColumnIO?,
    index: Int,
) : ColumnIO(groupType, parent, index) {
    private val childrenByName = hashMapOf<String, ColumnIO>()
    private val children = arrayListOf<ColumnIO>()
    var childrenCount: Int = 0
        private set

    override val columnNames: List<Array<String>>
        get() = children.flatMap { it.columnNames }

    override val last: PrimitiveColumnIO
        get() = children[children.size - 1].last

    override val first: PrimitiveColumnIO
        get() = children[0].first

    fun add(child: ColumnIO) {
        children.add(child)
        childrenByName.put(child.type.name, child)
        ++childrenCount
    }

    override fun setLevels(
        r: Int,
        d: Int,
        fieldPath: Array<String>,
        indexFieldPath: IntArray,
        repetition: List<ColumnIO>,
        path: List<ColumnIO>,
    ) {
        super.setLevels(r, d, fieldPath, indexFieldPath, repetition, path)
        for (child in children) {
            val newFieldPath = fieldPath.copyOf(fieldPath.size + 1)
            val newIndexFieldPath = indexFieldPath.copyOf(indexFieldPath.size + 1)
            newFieldPath[fieldPath.size] = child.type.name
            newIndexFieldPath[indexFieldPath.size] = child.index
            val newRepetition: List<ColumnIO>
            if (child.type.isRepetition(Repetition.REPEATED)) {
                newRepetition = ArrayList<ColumnIO>(repetition)
                newRepetition.add(child)
            } else {
                newRepetition = repetition
            }
            val newPath = ArrayList<ColumnIO>(path)
            newPath.add(child)
            child.setLevels(
                // the type repetition level increases whenever there's a possible repetition
                if (child.type.isRepetition(Repetition.REPEATED)) r + 1 else r,
                // the type definition level increases whenever a field can be missing (not required)
                if (!child.type.isRepetition(Repetition.REQUIRED)) d + 1 else d,
                newFieldPath.requireNoNulls(),
                newIndexFieldPath,
                newRepetition,
                newPath,
            )
        }
    }

    fun getChild(name: String): ColumnIO? {
        return childrenByName[name]
    }

    fun getChild(fieldIndex: Int): ColumnIO {
        try {
            return children[fieldIndex]
        } catch (e: IndexOutOfBoundsException) {
            throw InvalidRecordException("could not get child $fieldIndex from $children", e)
        }
    }
}
