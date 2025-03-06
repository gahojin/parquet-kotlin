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
package org.apache.parquet.example.data.simple.convert

import org.apache.parquet.example.data.Group
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.schema.GroupType

internal open class SimpleGroupConverter(
    private val parent: SimpleGroupConverter?,
    private val index: Int,
    schema: GroupType,
) : GroupConverter() {
    lateinit var currentRecord: Group
        protected set
    private var converters = Array<Converter>(schema.fieldCount) {
        val type = schema.getType(it)
        if (type.isPrimitive) {
            SimplePrimitiveConverter(this, it)
        } else {
            SimpleGroupConverter(this, it, type.asGroupType())
        }
    }

    override fun start() {
        currentRecord = requireNotNull(parent).currentRecord.addGroup(index)
    }

    override fun getConverter(fieldIndex: Int): Converter {
        return converters[fieldIndex]
    }

    override fun end() = Unit
}
