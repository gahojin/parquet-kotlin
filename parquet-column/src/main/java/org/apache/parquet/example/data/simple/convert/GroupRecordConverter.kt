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
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.io.api.GroupConverter
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType

class GroupRecordConverter(schema: MessageType) : RecordMaterializer<Group>() {
    private val simpleGroupFactory: SimpleGroupFactory = SimpleGroupFactory(schema)

    private val root: SimpleGroupConverter = object : SimpleGroupConverter(null, 0, schema) {
        override fun start() {
            currentRecord = simpleGroupFactory.newGroup()
        }
        override fun end() = Unit
    }

    override val currentRecord: Group?
        get() = root.currentRecord

    override val rootConverter: GroupConverter
        get() = root
}
