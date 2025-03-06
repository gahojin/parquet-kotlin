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

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import java.util.*

/**
 * Primitive level of the IO structure
 */
class PrimitiveColumnIO internal constructor(
    type: Type,
    parent: GroupColumnIO,
    index: Int,
    val id: Int,
) : ColumnIO(type, parent, index) {
    //  private static final Logger logger = Logger.getLogger(PrimitiveColumnIO.class.getName());
    var path: Array<ColumnIO> = emptyArray()
        private set
    lateinit var columnDescriptor: ColumnDescriptor
        private set
    val primitive: PrimitiveType.PrimitiveTypeName
        get() = type.asPrimitiveType().primitiveTypeName

    override val columnNames: List<Array<String>>
        get() = listOf(fieldPath)

    override val last: PrimitiveColumnIO = this

    override val first: PrimitiveColumnIO = this

    override fun setLevels(
        r: Int,
        d: Int,
        fieldPath: Array<String>,
        fieldIndexPath: IntArray,
        repetition: List<ColumnIO>,
        path: List<ColumnIO>,
    ) {
        super.setLevels(r, d, fieldPath, fieldIndexPath, repetition, path)
        val type = type.asPrimitiveType()
        columnDescriptor = ColumnDescriptor(fieldPath, type, repetitionLevel, definitionLevel)
        this.path = path.toTypedArray()
    }

    fun isFirst(r: Int): Boolean = getFirst(r) === this

    fun isLast(r: Int): Boolean = getLast(r) === this

    private fun getLast(r: Int): PrimitiveColumnIO {
        val parent = getParent(r)

        val last = parent.last
        return last
    }

    private fun getFirst(r: Int): PrimitiveColumnIO {
        val parent = getParent(r)
        return parent.first
    }
}
