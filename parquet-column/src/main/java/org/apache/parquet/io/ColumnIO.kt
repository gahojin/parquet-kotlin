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

import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Type.Repetition

/**
 * a structure used to serialize deserialize records
 */
abstract class ColumnIO internal constructor(
    open val type: Type,
    val parent: GroupColumnIO?,
    val index: Int,
) {
    val name: String = type.name

    /** the maximum repetition level for this column */
    var repetitionLevel: Int = 0

    /** the maximum definition level for this column */
    var definitionLevel: Int = 0

    lateinit var fieldPath: Array<String>
        private set
    lateinit var indexFieldPath: IntArray
        private set

    abstract val columnNames: List<Array<String>>

    abstract val last: PrimitiveColumnIO

    abstract val first: PrimitiveColumnIO

    fun getFieldPath(level: Int): String? {
        return fieldPath[level]
    }

    fun getIndexFieldPath(level: Int): Int {
        return indexFieldPath[level]
    }

    fun setFieldPath(fieldPath: Array<String>, indexFieldPath: IntArray) {
        this.fieldPath = fieldPath
        this.indexFieldPath = indexFieldPath
    }

    open fun setLevels(
        r: Int,
        d: Int,
        fieldPath: Array<String>,
        indexFieldPath: IntArray,
        repetition: List<ColumnIO>,
        path: List<ColumnIO>,
    ) {
        repetitionLevel = r
        definitionLevel = d
        setFieldPath(fieldPath, indexFieldPath)
    }

    fun getParent(r: Int): ColumnIO {
        return if (repetitionLevel == r && this.type.isRepetition(Repetition.REPEATED)) {
            this
        } else if (parent != null && parent.definitionLevel >= r) {
            parent.getParent(r)
        } else {
            throw InvalidRecordException("no parent(" + r + ") for " + this.fieldPath.contentToString())
        }
    }

    override fun toString(): String {
        return "${javaClass.getSimpleName()} ${type.name} r:${repetitionLevel} d:${definitionLevel} ${fieldPath.contentToString()}"
    }
}
