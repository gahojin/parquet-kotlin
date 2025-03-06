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
package org.apache.parquet.example.data

import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.GroupType

abstract class GroupValueSource {
    abstract val type: GroupType

    open fun getFieldRepetitionCount(field: String): Int {
        return getFieldRepetitionCount(type.getFieldIndex(field))
    }

    open fun getGroup(field: String, index: Int): GroupValueSource {
        return getGroup(type.getFieldIndex(field), index)
    }

    open fun getString(field: String, index: Int): String? {
        return getString(type.getFieldIndex(field), index)
    }

    open fun getInteger(field: String, index: Int): Int {
        return getInteger(type.getFieldIndex(field), index)
    }

    open fun getLong(field: String, index: Int): Long {
        return getLong(type.getFieldIndex(field), index)
    }

    open fun getDouble(field: String, index: Int): Double {
        return getDouble(type.getFieldIndex(field), index)
    }

    open fun getFloat(field: String, index: Int): Float {
        return getFloat(type.getFieldIndex(field), index)
    }

    open fun getBoolean(field: String, index: Int): Boolean {
        return getBoolean(type.getFieldIndex(field), index)
    }

    open fun getBinary(field: String, index: Int): Binary {
        return getBinary(type.getFieldIndex(field), index)
    }

    open fun getInt96(field: String, index: Int): Binary {
        return getInt96(type.getFieldIndex(field), index)
    }

    abstract fun getFieldRepetitionCount(fieldIndex: Int): Int

    abstract fun getGroup(fieldIndex: Int, index: Int): GroupValueSource

    abstract fun getString(fieldIndex: Int, index: Int): String

    abstract fun getInteger(fieldIndex: Int, index: Int): Int

    abstract fun getLong(fieldIndex: Int, index: Int): Long

    abstract fun getDouble(fieldIndex: Int, index: Int): Double

    abstract fun getFloat(fieldIndex: Int, index: Int): Float

    abstract fun getBoolean(fieldIndex: Int, index: Int): Boolean

    abstract fun getBinary(fieldIndex: Int, index: Int): Binary

    abstract fun getInt96(fieldIndex: Int, index: Int): Binary

    abstract fun getValueToString(fieldIndex: Int, index: Int): String
}
