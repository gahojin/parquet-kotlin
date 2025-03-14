/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.io

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Type.Repetition
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Wraps a record consumer
 * Validates the record written against the schema and pass down the event to the wrapped consumer
 *
 * @param delegate the consumer to pass down the event to
 * @param schema   the schema to validate against
 */
class ValidatingRecordConsumer(
    private val delegate: RecordConsumer,
    schema: MessageType,
) : RecordConsumer() {
    private val types = ArrayDeque<Type>()
    private val fields = ArrayDeque<Int>()
    private val previousField = ArrayDeque<Int>()
    private val fieldValueCount = ArrayDeque<Int>()

    init {
        types.addFirst(schema)
    }

    override fun startMessage() {
        previousField.addFirst(-1)
        delegate.startMessage()
    }

    override fun endMessage() {
        delegate.endMessage()
        validateMissingFields(types.first().asGroupType().fieldCount)
        previousField.removeFirst()
    }

    override fun startField(field: String, index: Int) {
        if (index <= previousField.first()) {
            throw InvalidRecordException("fields must be added in order $field index $index is before previous field ${previousField.first()}")
        }
        validateMissingFields(index)
        fields.addFirst(index)
        fieldValueCount.addFirst(0)
        delegate.startField(field, index)
    }

    private fun validateMissingFields(index: Int) {
        for (i in previousField.first() + 1..<index) {
            val type = types.first().asGroupType().getType(i)
            if (type.isRepetition(Repetition.REQUIRED)) {
                throw InvalidRecordException("required field is missing $type")
            }
        }
    }

    override fun endField(field: String, index: Int) {
        delegate.endField(field, index)
        fieldValueCount.removeFirst()
        previousField.addFirst(fields.removeFirst())
    }

    override fun startGroup() {
        previousField.addFirst(-1)
        types.addFirst(types.first().asGroupType().getType(fields.first()))
        delegate.startGroup()
    }

    override fun endGroup() {
        delegate.endGroup()
        validateMissingFields(types.first().asGroupType().fieldCount)
        types.removeFirst()
        previousField.removeFirst()
    }

    override fun flush() {
        delegate.flush()
    }

    private fun validate(p: PrimitiveTypeName) {
        val currentType = types.first().asGroupType().getType(fields.first())
        val c = fieldValueCount.removeFirst() + 1
        fieldValueCount.addFirst(c)
        LOG.debug("validate {} for {}", p, currentType.name)
        when (currentType.repetition) {
            Repetition.OPTIONAL, Repetition.REQUIRED -> if (c > 1) {
                throw InvalidRecordException("repeated value when the type is not repeated in $currentType")
            }
            Repetition.REPEATED -> Unit
        }
        if (!currentType.isPrimitive || currentType.asPrimitiveType().primitiveTypeName !== p) {
            throw InvalidRecordException("expected type $p but got $currentType")
        }
    }

    private fun validate(vararg ptypes: PrimitiveTypeName) {
        val currentType = types.first().asGroupType().getType(fields.first())
        val c = fieldValueCount.removeFirst() + 1
        fieldValueCount.addFirst(c)
        if (LOG.isDebugEnabled) LOG.debug("validate ${ptypes.contentToString()} for ${currentType.name}")
        when (currentType.repetition) {
            Repetition.OPTIONAL, Repetition.REQUIRED -> if (c > 1) {
                throw InvalidRecordException("repeated value when the type is not repeated in $currentType")
            }
            Repetition.REPEATED -> Unit
        }
        if (!currentType.isPrimitive) {
            throw InvalidRecordException("expected type in ${ptypes.contentToString()} but got $currentType")
        }
        for (p in ptypes) {
            if (currentType.asPrimitiveType().primitiveTypeName === p) {
                return  // type is valid
            }
        }
        throw InvalidRecordException("expected type in ${ptypes.contentToString()} but got $currentType")
    }

    override fun addInteger(value: Int) {
        validate(PrimitiveTypeName.INT32)
        delegate.addInteger(value)
    }

    override fun addLong(value: Long) {
        validate(PrimitiveTypeName.INT64)
        delegate.addLong(value)
    }

    override fun addBoolean(value: Boolean) {
        validate(PrimitiveTypeName.BOOLEAN)
        delegate.addBoolean(value)
    }

    override fun addBinary(value: Binary) {
        validate(PrimitiveTypeName.BINARY, PrimitiveTypeName.INT96, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        delegate.addBinary(value)
    }

    override fun addFloat(value: Float) {
        validate(PrimitiveTypeName.FLOAT)
        delegate.addFloat(value)
    }

    override fun addDouble(value: Double) {
        validate(PrimitiveTypeName.DOUBLE)
        delegate.addDouble(value)
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(ValidatingRecordConsumer::class.java)
    }
}
