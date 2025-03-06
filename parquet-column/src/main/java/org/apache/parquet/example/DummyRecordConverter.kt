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
package org.apache.parquet.example

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Converter
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.PrimitiveConverter
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.TypeConverter

/**
 * Dummy implementation for perf tests
 */
class DummyRecordConverter(schema: MessageType) : RecordMaterializer<Any?>() {
    override var currentRecord: Any? = null
        private set
    override val rootConverter: GroupConverter = schema.convertWith<Converter>(object : TypeConverter<Converter> {
        override fun convertPrimitiveType(
            path: List<GroupType>,
            primitiveType: PrimitiveType,
        ): Converter {
            return object : PrimitiveConverter() {
                override fun addBinary(value: Binary) {
                    this@DummyRecordConverter.currentRecord = value
                }

                override fun addBoolean(value: Boolean) {
                    this@DummyRecordConverter.currentRecord = value
                }

                override fun addDouble(value: Double) {
                    this@DummyRecordConverter.currentRecord = value
                }

                override fun addFloat(value: Float) {
                    this@DummyRecordConverter.currentRecord = value
                }

                override fun addInt(value: Int) {
                    this@DummyRecordConverter.currentRecord = value
                }

                override fun addLong(value: Long) {
                    this@DummyRecordConverter.currentRecord = value
                }
            }
        }

        override fun convertGroupType(
            path: List<GroupType>?,
            groupType: GroupType,
            converters: List<Converter>,
        ): Converter {
            return object : GroupConverter() {
                override fun getConverter(fieldIndex: Int): Converter {
                    return converters[fieldIndex]
                }

                override fun start() {
                    this@DummyRecordConverter.currentRecord = "start()"
                }

                override fun end() {
                    this@DummyRecordConverter.currentRecord = "end()"
                }
            }
        }

        override fun convertMessageType(
            messageType: MessageType,
            children: List<Converter>,
        ): Converter {
            return convertGroupType(null, messageType, children)
        }
    }) as GroupConverter
}
