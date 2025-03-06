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

import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.GroupType

class GroupWriter(
    private val recordConsumer: RecordConsumer,
    private val schema: GroupType,
) {
    fun write(group: Group) {
        recordConsumer.startMessage()
        writeGroup(group, schema)
        recordConsumer.endMessage()
    }

    private fun writeGroup(group: Group, type: GroupType) {
        val fieldCount = type.fieldCount
        for (field in 0..<fieldCount) {
            val valueCount = group.getFieldRepetitionCount(field)
            if (valueCount > 0) {
                val fieldType = type.getType(field)
                val fieldName = fieldType.name
                recordConsumer.startField(fieldName, field)
                for (index in 0..<valueCount) {
                    if (fieldType.isPrimitive) {
                        group.writeValue(field, index, recordConsumer)
                    } else {
                        recordConsumer.startGroup()
                        writeGroup(group.getGroup(field, index), fieldType.asGroupType())
                        recordConsumer.endGroup()
                    }
                }
                recordConsumer.endField(fieldName, field)
            }
        }
    }
}
