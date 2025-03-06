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
package org.apache.parquet.schema

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.InvalidRecordException

/**
 * The root of a schema
 */
class MessageType : GroupType {
    val paths: List<Array<String>>
        get() = getPaths(0)

    val columns: List<ColumnDescriptor>
        get() = getPaths(0).map {
            // TODO: optimize this
            val primitiveType = getType(*it).asPrimitiveType()
            ColumnDescriptor(it, primitiveType, getMaxRepetitionLevel(*it), getMaxDefinitionLevel(*it))
        }

    /**
     * @param name   the name of the type
     * @param fields the fields contained by this message
     */
    constructor(name: String, vararg fields: Type) : super(Repetition.REPEATED, name, fields.toList())

    /**
     * @param name   the name of the type
     * @param fields the fields contained by this message
     */
    constructor(name: String, fields: List<Type>) : super(Repetition.REPEATED, name, fields)

    override fun accept(visitor: TypeVisitor) {
        visitor.visit(this)
    }

    override fun writeToStringBuilder(sb: StringBuilder, indent: String) {
        sb.apply {
            append("message ")
            append(name)
            if (logicalTypeAnnotation != null) {
                " ($logicalTypeAnnotation)"
            }
            append(" {\n")
            membersDisplayString(this, "  ")
            append("}\n")
        }
    }

    /**
     * @param path an array of strings representing the name path in this type
     * @return the max repetition level that might be needed to encode the
     * type at 'path'.
     */
    fun getMaxRepetitionLevel(vararg path: String): Int {
        return getMaxRepetitionLevel(path, 0) - 1
    }

    /**
     * @param path an array of strings representing the name path in this type
     * @return the max repetition level that might be needed to encode the
     * type at 'path'.
     */
    fun getMaxDefinitionLevel(vararg path: String): Int {
        return getMaxDefinitionLevel(path, 0) - 1
    }

    fun getType(vararg path: String): Type {
        return getType(path, 0)
    }

    fun getColumnDescription(path: Array<String>): ColumnDescriptor {
        val maxRep = getMaxRepetitionLevel(*path)
        val maxDef = getMaxDefinitionLevel(*path)
        val type = getType(*path).asPrimitiveType()
        return ColumnDescriptor(path, type, maxRep, maxDef)
    }

    override fun checkContains(subType: Type) {
        if (subType !is MessageType) {
            throw InvalidRecordException("$subType found: expected $this")
        }
        checkGroupContains(subType)
    }

    fun <T> convertWith(converter: TypeConverter<T>): T {
        val path = ArrayList<GroupType>()
        path.add(this)
        return converter.convertMessageType(this, convertChildren<T>(path, converter))
    }

    fun containsPath(path: Array<String>): Boolean {
        return containsPath(path, 0)
    }

    @JvmOverloads
    fun union(toMerge: MessageType, strict: Boolean = true): MessageType {
        return MessageType(this.name, mergeFields(toMerge, strict))
    }
}
