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

/**
 * Class representing the column order with all the related parameters.
 */
class ColumnOrder private constructor(
    val columnOrderName: ColumnOrderName,
) {
    /**
     * The enum type of the column order.
     */
    enum class ColumnOrderName {
        /**
         * Representing the case when the defined column order is undefined (e.g. the file is written by a later API and the
         * current one does not support the related column order). No statistics will be written/read in this case.
         */
        UNDEFINED,

        /**
         * Type defined order meaning that the comparison order of the elements are based on its type.
         */
        TYPE_DEFINED_ORDER,
    }

    override fun equals(obj: Any?): Boolean {
        if (obj is ColumnOrder) {
            return columnOrderName == obj.columnOrderName
        }
        return false
    }

    override fun hashCode(): Int {
        return columnOrderName.hashCode()
    }

    override fun toString(): String {
        return columnOrderName.toString()
    }

    companion object {
        private val UNDEFINED_COLUMN_ORDER = ColumnOrder(ColumnOrderName.UNDEFINED)
        private val TYPE_DEFINED_COLUMN_ORDER = ColumnOrder(ColumnOrderName.TYPE_DEFINED_ORDER)

        /**
         * @return a [ColumnOrder] instance representing an undefined order
         * @see ColumnOrderName.UNDEFINED
         */
        @JvmStatic
        fun undefined(): ColumnOrder {
            return UNDEFINED_COLUMN_ORDER
        }

        /**
         * @return a [ColumnOrder] instance representing a type defined order
         * @see ColumnOrderName.TYPE_DEFINED_ORDER
         */
        @JvmStatic
        fun typeDefined(): ColumnOrder {
            return TYPE_DEFINED_COLUMN_ORDER
        }
    }
}
