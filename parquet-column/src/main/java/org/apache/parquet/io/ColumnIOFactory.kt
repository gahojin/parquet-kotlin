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
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.TypeVisitor

/**
 * Factory constructing the ColumnIO structure from the schema
 *
 * validation is off by default
 *
 * @param createdBy createdBy string for readers
 */
class ColumnIOFactory @JvmOverloads constructor(
    private val createdBy: String? = null,
    private val validating: Boolean = false,
) {
    private class ColumnIOCreatorVisitor(
        private val validating: Boolean,
        private val requestedSchema: MessageType,
        private val createdBy: String?,
        private val strictTypeChecking: Boolean
    ) : TypeVisitor {
        lateinit var columnIO: MessageColumnIO
            private set
        private var current: GroupColumnIO? = null
        private val leaves = ArrayList<PrimitiveColumnIO>()
        private var currentRequestedIndex = 0
        private lateinit var currentRequestedType: Type

        override fun visit(messageType: MessageType) {
            columnIO = MessageColumnIO(requestedSchema, validating, createdBy).also {
                visitChildren(it, messageType, requestedSchema)
                it.setLevels()
                it.leaves = leaves
            }
        }

        override fun visit(groupType: GroupType) {
            if (currentRequestedType.isPrimitive) {
                incompatibleSchema(groupType, currentRequestedType)
            }
            val newIO = GroupColumnIO(groupType, current, currentRequestedIndex)
            current!!.add(newIO)
            visitChildren(newIO, groupType, currentRequestedType.asGroupType())
        }

        fun visitChildren(newIO: GroupColumnIO, groupType: GroupType, requestedGroupType: GroupType) {
            val oldIO = current
            current = newIO
            for (type in groupType.fields) {
                // if the file schema does not contain the field it will just stay null
                if (requestedGroupType.containsField(type.name)) {
                    currentRequestedIndex = requestedGroupType.getFieldIndex(type.name)
                    currentRequestedType = requestedGroupType.getType(currentRequestedIndex).also {
                        if (it.repetition.isMoreRestrictiveThan(type.repetition)) {
                            incompatibleSchema(type, it)
                        }
                    }
                    type.accept(this)
                }
            }
            current = oldIO
        }

        override fun visit(primitiveType: PrimitiveType) {
            if (!currentRequestedType.isPrimitive
                || (this.strictTypeChecking
                        && (currentRequestedType.asPrimitiveType().primitiveTypeName
                        !== primitiveType.primitiveTypeName))
            ) {
                incompatibleSchema(primitiveType, currentRequestedType)
            }
            val newIO = PrimitiveColumnIO(primitiveType, current!!, currentRequestedIndex, leaves.size)
            current!!.add(newIO)
            leaves.add(newIO)
        }

        fun incompatibleSchema(fileType: Type, requestedType: Type) {
            throw ParquetDecodingException(
                "The requested schema is not compatible with the file schema. incompatible types: $requestedType != $fileType")
        }
    }

    /**
     * @param validating to turn validation on
     */
    constructor(validating: Boolean) : this(null, validating)

    /**
     * @param requestedSchema the requestedSchema we want to read/write
     * @param fileSchema      the file schema (when reading it can be different from the requested schema)
     * @return the corresponding serializing/deserializing structure
     */
    fun getColumnIO(requestedSchema: MessageType, fileSchema: MessageType): MessageColumnIO {
        return getColumnIO(requestedSchema, fileSchema, true)
    }

    /**
     * @param requestedSchema the requestedSchema we want to read/write
     * @param fileSchema      the file schema (when reading it can be different from the requested schema)
     * @param strict          should file type and requested primitive types match
     * @return the corresponding serializing/deserializing structure
     */
    fun getColumnIO(requestedSchema: MessageType, fileSchema: MessageType, strict: Boolean): MessageColumnIO {
        val visitor = ColumnIOCreatorVisitor(validating, requestedSchema, createdBy, strict)
        fileSchema.accept(visitor)
        return visitor.columnIO
    }

    /**
     * @param schema the schema we want to read/write
     * @return the corresponding serializing/deserializing structure
     */
    fun getColumnIO(schema: MessageType): MessageColumnIO {
        return this.getColumnIO(schema, schema)
    }
}
