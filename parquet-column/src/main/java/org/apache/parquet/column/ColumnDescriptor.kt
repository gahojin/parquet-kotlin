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
package org.apache.parquet.column

import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type

/**
 * Describes a column's type as well as its position in its containing schema.
 *
 * @property path   the path to the leaf field in the schema
 * @property primitiveType   the type of the field
 * @property maxRepetitionLevel the maximum repetition level for that path
 * @property maxDefinitionLevel the maximum definition level for that path
 */
class ColumnDescriptor(
    val path: Array<String>,
    val primitiveType: PrimitiveType,
    val maxRepetitionLevel: Int,
    val maxDefinitionLevel: Int,
) : Comparable<ColumnDescriptor> {
    /**
     * @return the size of the type
     */
    @get:Deprecated("will removed in 2.0.0. Use {@link #getPrimitiveType()} instead.")
    val typeLength: Int
        get() = primitiveType.typeLength

    /**
     * @param path   the path to the leaf field in the schema
     * @param type   the type of the field
     * @param maxRep the maximum repetition level for that path
     * @param maxDef the maximum definition level for that path
     */
    @Deprecated("will be removed in 2.0.0; Use {@link #ColumnDescriptor(String[], PrimitiveType, int, int)}")
    constructor(path: Array<String>, type: PrimitiveTypeName, maxRep: Int, maxDef: Int) : this(
        path = path,
        type = type,
        typeLength = 0,
        maxRep = maxRep,
        maxDef = maxDef,
    )

    /**
     * @param path       the path to the leaf field in the schema
     * @param type       the type of the field
     * @param typeLength the length of the type, if type is a fixed-length byte array
     * @param maxRep     the maximum repetition level for that path
     * @param maxDef     the maximum definition level for that path
     */
    @Deprecated("will be removed in 2.0.0; Use {@link #ColumnDescriptor(String[], PrimitiveType, int, int)}")
    constructor(path: Array<String>, type: PrimitiveTypeName, typeLength: Int, maxRep: Int, maxDef: Int) : this(
        path = path,
        primitiveType = PrimitiveType(Type.Repetition.OPTIONAL, type, typeLength, ""),
        maxRepetitionLevel = maxRep,
        maxDefinitionLevel = maxDef,
    )

    /**
     * @return the type of that column
     */
    @Deprecated("will removed in 2.0.0. Use {@link #getPrimitiveType()} instead.")
    fun getType(): PrimitiveTypeName {
        return primitiveType.primitiveTypeName
    }

    override fun hashCode(): Int {
        return path.contentHashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is ColumnDescriptor) return false
        val descriptor = other
        return path.contentEquals(descriptor.path)
    }

    override fun compareTo(o: ColumnDescriptor): Int {
        val length = if (path.size < o.path.size) path.size else o.path.size
        for (i in 0..<length) {
            val compareTo = path[i].compareTo(o.path[i])
            if (compareTo != 0) {
                return compareTo
            }
        }
        return path.size - o.path.size
    }

    override fun toString(): String {
        return "${path.contentToString()} $primitiveType"
    }
}
