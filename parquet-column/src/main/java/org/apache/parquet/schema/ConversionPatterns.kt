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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.schema

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

/**
 * Utility functions to convert from Java-like map and list types
 * to equivalent Parquet types.
 */
object ConversionPatterns {
    const val MAP_REPEATED_NAME = "key_value"
    private const val ELEMENT_NAME = "element"

    /**
     * to preserve the difference between empty list and null when optional
     *
     * @param repetition            repetition for the list or map
     * @param alias                 name of the field
     * @param logicalTypeAnnotation logical type for the list or map
     * @param nested                the nested repeated field
     * @return a group type
     */
    private fun listWrapper(
        repetition: Repetition,
        alias: String,
        logicalTypeAnnotation: LogicalTypeAnnotation,
        nested: Type,
    ): GroupType {
        require(nested.isRepetition(Repetition.REPEATED)) { "Nested type should be repeated: $nested" }
        return GroupType(repetition, alias, logicalTypeAnnotation, nested)
    }

    fun mapType(
        repetition: Repetition,
        alias: String,
        keyType: Type,
        valueType: Type?,
    ): GroupType {
        return mapType(repetition, alias, MAP_REPEATED_NAME, keyType, valueType)
    }

    fun stringKeyMapType(
        repetition: Repetition,
        alias: String,
        mapAlias: String,
        valueType: Type?,
    ): GroupType {
        return mapType(
            repetition,
            alias,
            mapAlias,
            PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "key", LogicalTypeAnnotation.stringType()),
            valueType,
        )
    }

    fun stringKeyMapType(
        repetition: Repetition,
        alias: String,
        valueType: Type,
    ): GroupType {
        return stringKeyMapType(repetition, alias, MAP_REPEATED_NAME, valueType)
    }

    fun mapType(
        repetition: Repetition,
        alias: String,
        mapAlias: String,
        keyType: Type,
        valueType: Type?,
    ): GroupType {
        // support projection only on key of a map
        return valueType?.let {
            if (it.name != "value") {
                throw RuntimeException("${it.name} should be value")
            }
            listWrapper(
                repetition,
                alias,
                LogicalTypeAnnotation.mapType(),
                GroupType(
                    Repetition.REPEATED,
                    mapAlias,
                    LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance(),
                    keyType,
                    it,
                )
            )
        } ?: run {
            listWrapper(
                repetition,
                alias,
                LogicalTypeAnnotation.mapType(),
                GroupType(
                    Repetition.REPEATED,
                    mapAlias,
                    LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance(),
                    keyType,
                )
            )
        }
    }

    /**
     * @param repetition repetition for the list
     * @param alias      name of the field
     * @param nestedType type of elements in the list
     * @return a group representing the list using a 2-level representation
     */
    @Deprecated("use listOfElements instead")
    fun listType(
        repetition: Repetition,
        alias: String,
        nestedType: Type,
    ): GroupType {
        return listWrapper(repetition, alias, LogicalTypeAnnotation.listType(), nestedType)
    }

    /**
     * Creates a 3-level list structure annotated with LIST with elements of the
     * given elementType. The repeated level is inserted automatically and the
     * elementType's repetition should be the correct repetition of the elements,
     * required for non-null and optional for nullable.
     *
     * @param listRepetition the repetition of the entire list structure
     * @param name           the name of the list structure type
     * @param elementType    the type of elements contained by the list
     * @return a GroupType that represents the list
     */
    fun listOfElements(
        listRepetition: Repetition,
        name: String,
        elementType: Type,
    ): GroupType {
        require(elementType.name == ELEMENT_NAME) { "List element type must be named 'element'" }
        return listWrapper(
            listRepetition,
            name,
            LogicalTypeAnnotation.listType(),
            GroupType(Repetition.REPEATED, "list", elementType)
        )
    }
}
