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
package org.apache.parquet.filter2.predicate

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

/**
 * Contains all valid mappings from class -&gt; parquet type (and vice versa) for use in
 * [FilterPredicate]s
 *
 * This is a bit ugly, but it allows us to provide good error messages at runtime
 * when there are type mismatches.
 *
 * TODO: this has some overlap with [PrimitiveTypeName.javaType]
 * TODO: (https://issues.apache.org/jira/browse/PARQUET-30)
 */
object ValidTypeMap {
    // classToParquetType and parquetTypeToClass are used as a bi-directional map
    private val classToParquetType: MutableMap<Class<*>, MutableSet<PrimitiveTypeName>> = hashMapOf()
    private val parquetTypeToClass: MutableMap<PrimitiveTypeName, MutableSet<Class<*>>> = hashMapOf()

    init {
        for (t in PrimitiveTypeName.entries) {
            var c = t.javaType
            if (c.isPrimitive) {
                c = PrimitiveToBoxedClass.get(c)
            }
            add(c, t)
        }
    }

    // set up the mapping in both directions
    private fun add(c: Class<*>, p: PrimitiveTypeName) {
        var descriptors = classToParquetType.getOrPut(c) { hashSetOf() }
        descriptors.add(p)

        var classes = parquetTypeToClass.getOrPut(p) { hashSetOf() }
        classes.add(c)
    }

    /**
     * Asserts that foundColumn was declared as a type that is compatible with the type for this column found
     * in the schema of the parquet file.
     *
     * @param foundColumn   the column as declared by the user
     * @param primitiveType the primitive type according to the schema
     * @param <T>           the java Type of values in the column, must be Comparable
     * @throws IllegalArgumentException if the types do not align
     */
    @JvmStatic
    fun <T : Comparable<T>> assertTypeValid(
        foundColumn: Operators.Column<T>,
        primitiveType: PrimitiveTypeName,
    ) {
        val foundColumnType = foundColumn.getColumnType()
        val columnPath = foundColumn.columnPath

        val validTypeDescriptors = classToParquetType[foundColumnType] ?: run {
            val message = buildString {
                append("Column ")
                append(columnPath.toDotString())
                append(" was declared as type: ")
                append(foundColumnType.getName())
                append(" which is not supported in FilterPredicates.")

                parquetTypeToClass[primitiveType]?.also {
                    append(" Supported types for this column are: ").append(it)
                } ?: run {
                    append(" There are no supported types for columns of ").append(primitiveType)
                }
            }
            throw IllegalArgumentException(message)
        }

        if (!validTypeDescriptors.contains(primitiveType)) {
            val message = buildString {
                append("FilterPredicate column: ")
                append(columnPath.toDotString())
                append("'s declared type (")
                append(foundColumnType.getName())
                append(") does not match the schema found in file metadata. Column ")
                append(columnPath.toDotString())
                append(" is of type: ")
                append(primitiveType)
                append("\nValid types for this column are: ")
                append(parquetTypeToClass[primitiveType])
            }
            throw IllegalArgumentException(message)
        }
    }
}
