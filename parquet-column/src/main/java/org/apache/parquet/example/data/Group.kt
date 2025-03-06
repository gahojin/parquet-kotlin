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

import org.apache.parquet.example.data.simple.NanoTime
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Binary.Companion.fromString
import org.apache.parquet.io.api.RecordConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

abstract class Group : GroupValueSource() {
    fun add(field: String, value: Int) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: Long) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: Float) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: Double) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: String) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: NanoTime) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: Boolean) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: Binary) {
        add(type.getFieldIndex(field), value)
    }

    fun add(field: String, value: Group) {
        add(type.getFieldIndex(field), value)
    }

    fun addGroup(field: String): Group {
        if (LOG.isDebugEnabled) {
            LOG.debug("add group {} to {}", field, type.name)
        }
        return addGroup(type.getFieldIndex(field))
    }

    override fun getGroup(field: String, index: Int): Group {
        return getGroup(type.getFieldIndex(field), index)
    }

    abstract fun add(fieldIndex: Int, value: Int)

    abstract fun add(fieldIndex: Int, value: Long)

    abstract fun add(fieldIndex: Int, value: String)

    abstract fun add(fieldIndex: Int, value: Boolean)

    abstract fun add(fieldIndex: Int, value: NanoTime)

    abstract fun add(fieldIndex: Int, value: Binary)

    abstract fun add(fieldIndex: Int, value: Float)

    abstract fun add(fieldIndex: Int, value: Double)

    abstract fun add(fieldIndex: Int, value: Group)

    abstract fun addGroup(fieldIndex: Int): Group

    abstract override fun getGroup(fieldIndex: Int, index: Int): Group

    open fun asGroup() = this

    fun append(fieldName: String, value: Int) = apply {
        add(fieldName, value)
    }

    fun append(fieldName: String, value: Float) = apply {
        add(fieldName, value)
    }

    fun append(fieldName: String, value: Double) = apply {
        add(fieldName, value)
    }

    fun append(fieldName: String, value: Long) = apply {
        add(fieldName, value)
    }

    fun append(fieldName: String, value: NanoTime) = apply {
        add(fieldName, value)
    }

    fun append(fieldName: String, value: String) = apply {
        add(fieldName, fromString(value))
    }

    fun append(fieldName: String, value: Boolean) = apply {
        add(fieldName, value)
    }

    fun append(fieldName: String, value: Binary) = apply {
        add(fieldName, value)
    }

    abstract fun writeValue(field: Int, index: Int, recordConsumer: RecordConsumer)

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(Group::class.java)
    }
}
