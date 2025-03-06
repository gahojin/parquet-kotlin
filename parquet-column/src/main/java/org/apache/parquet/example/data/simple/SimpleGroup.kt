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
package org.apache.parquet.example.data.simple

import org.apache.parquet.example.data.Group
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type

class SimpleGroup(private val schema: GroupType) : Group() {
    private val data = Array(schema.fieldCount) { mutableListOf<Any?>() }

    override val type: GroupType = schema

    private fun appendToString(builder: StringBuilder, indent: String): StringBuilder {
        var i = 0
        for (field in schema.fields) {
            val name = field.name
            val values = data[i]
            ++i
            if (!values.isEmpty()) {
                for (value in values) {
                    builder.append(indent).append(name)
                    if (value == null) {
                        builder.append(": NULL\n")
                    } else if (value is SimpleGroup) {
                        builder.append('\n')
                        value.appendToString(builder, "$indent  ")
                    } else {
                        builder.append(": ").append(value).append('\n')
                    }
                }
            }
        }
        return builder
    }

    override fun toString(): String {
        return toString("")
    }

    fun toString(indent: String) = buildString {
        appendToString(this, indent)
    }

    override fun addGroup(fieldIndex: Int): Group {
        val g = SimpleGroup(schema.getType(fieldIndex).asGroupType())
        add(fieldIndex, g)
        return g
    }

    fun getObject(field: String?, index: Int): Any? {
        return getObject(schema.getFieldIndex(field), index)
    }

    fun getObject(fieldIndex: Int, index: Int): Any? {
        val wrapped = getValue(fieldIndex, index)
        // Unwrap to Java standard object, if possible
        return when (wrapped) {
            is BooleanValue -> wrapped.boolean
            is IntegerValue -> wrapped.integer
            is LongValue -> wrapped.long
            is Int96Value -> wrapped.int96
            is FloatValue -> wrapped.float
            is DoubleValue -> wrapped.double
            is BinaryValue -> wrapped.binary
            else -> wrapped
        }
    }

    override fun getGroup(fieldIndex: Int, index: Int): Group {
        return getValue(fieldIndex, index) as Group
    }

    private fun getValue(fieldIndex: Int, index: Int): Any? {
        val list: List<Any?>
        try {
            list = data[fieldIndex]
        } catch (_: IndexOutOfBoundsException) {
            throw RuntimeException(
                "not found $fieldIndex(${schema.getFieldName(fieldIndex)}) in group:\n$this")
        }
        try {
            return list[index]
        } catch (_: IndexOutOfBoundsException) {
            throw RuntimeException(
                "not found $fieldIndex(${schema.getFieldName(fieldIndex)}) element number $index in group:\n$this")
        }
    }

    private fun add(fieldIndex: Int, value: Primitive?) {
        val type = schema.getType(fieldIndex)
        val list = data[fieldIndex]
        check(type.isRepetition(Type.Repetition.REPEATED) || list.isEmpty()) {
            "field $fieldIndex (${type.name}) can not have more than one value: $list"
        }
        list.add(value)
    }

    override fun getFieldRepetitionCount(fieldIndex: Int): Int {
        val list = data[fieldIndex]
        return list.size
    }

    override fun getValueToString(fieldIndex: Int, index: Int): String {
        return getValue(fieldIndex, index).toString()
    }

    override fun getString(fieldIndex: Int, index: Int): String {
        return (getValue(fieldIndex, index) as BinaryValue).string
    }

    override fun getInteger(fieldIndex: Int, index: Int): Int {
        return (getValue(fieldIndex, index) as IntegerValue).integer
    }

    override fun getLong(fieldIndex: Int, index: Int): Long {
        return (getValue(fieldIndex, index) as LongValue).long
    }

    override fun getDouble(fieldIndex: Int, index: Int): Double {
        return (getValue(fieldIndex, index) as DoubleValue).double
    }

    override fun getFloat(fieldIndex: Int, index: Int): Float {
        return (getValue(fieldIndex, index) as FloatValue).float
    }

    override fun getBoolean(fieldIndex: Int, index: Int): Boolean {
        return (getValue(fieldIndex, index) as BooleanValue).boolean
    }

    override fun getBinary(fieldIndex: Int, index: Int): Binary {
        return (getValue(fieldIndex, index) as BinaryValue).binary
    }

    fun getTimeNanos(fieldIndex: Int, index: Int): NanoTime {
        return NanoTime.fromInt96((getValue(fieldIndex, index) as Int96Value))
    }

    override fun getInt96(fieldIndex: Int, index: Int): Binary {
        return (getValue(fieldIndex, index) as Int96Value).int96
    }

    override fun add(fieldIndex: Int, value: Int) {
        add(fieldIndex, IntegerValue(value))
    }

    override fun add(fieldIndex: Int, value: Long) {
        add(fieldIndex, LongValue(value))
    }

    override fun add(fieldIndex: Int, value: String) {
        add(fieldIndex, BinaryValue(Binary.fromString(value)))
    }

    override fun add(fieldIndex: Int, value: NanoTime) {
        add(fieldIndex, value.toInt96())
    }

    override fun add(fieldIndex: Int, value: Boolean) {
        add(fieldIndex, BooleanValue(value))
    }

    override fun add(fieldIndex: Int, value: Binary) {
        when (type.getType(fieldIndex).asPrimitiveType().primitiveTypeName) {
            PrimitiveTypeName.BINARY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY -> add(fieldIndex, BinaryValue(value))
            PrimitiveTypeName.INT96 -> add(fieldIndex, Int96Value(value))
            else -> throw UnsupportedOperationException("${type.asPrimitiveType().name} not supported for Binary")
        }
    }

    override fun add(fieldIndex: Int, value: Float) {
        add(fieldIndex, FloatValue(value))
    }

    override fun add(fieldIndex: Int, value: Double) {
        add(fieldIndex, DoubleValue(value))
    }

    override fun add(fieldIndex: Int, value: Group) {
        data[fieldIndex].add(value)
    }

    override fun writeValue(field: Int, index: Int, recordConsumer: RecordConsumer) {
        (getValue(field, index) as Primitive).writeValue(recordConsumer)
    }
}
