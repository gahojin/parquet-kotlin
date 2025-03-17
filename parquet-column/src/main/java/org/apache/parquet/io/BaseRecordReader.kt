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
package org.apache.parquet.io

import org.apache.parquet.column.ColumnReadStore
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.io.api.RecordMaterializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Base record reader class.
 */
@Deprecated("")
abstract class BaseRecordReader<T> : RecordReader<T>() {
    abstract var recordConsumer: RecordConsumer
    abstract var recordMaterializer: RecordMaterializer<T>
    abstract var columnStore: ColumnReadStore
    abstract var caseLookup: Array<RecordReaderImplementation.State>
    private var endField: String? = null
    private var endIndex = 0

    override fun read(): T? {
        readOneRecord()
        return recordMaterializer.currentRecord
    }

    protected abstract fun readOneRecord()

    protected fun currentLevel(currentLevel: Int) {
        LOG.debug("currentLevel: {}", currentLevel)
    }

    protected fun log(message: String?) {
        LOG.debug("bc: {}", message)
    }

    protected fun getCaseId(state: Int, currentLevel: Int, d: Int, nextR: Int): Int {
        return caseLookup[state].getCase(currentLevel, d, nextR).id
    }

    protected fun startMessage() {
        // reset state
        endField = null
        LOG.debug("startMessage()")
        recordConsumer.startMessage()
    }

    protected fun startGroup(field: String, index: Int) {
        startField(field, index)
        LOG.debug("startGroup()")
        recordConsumer.startGroup()
    }

    private fun startField(field: String, index: Int) {
        LOG.debug("startField({},{})", field, index)
        if (endField != null && index == endIndex) {
            // skip the close/open tag
            endField = null
        } else {
            endField?.also {
                // close the previous field
                recordConsumer.endField(it, endIndex)
                endField = null
            }
            recordConsumer.startField(field, index)
        }
    }

    protected fun addPrimitiveINT64(field: String, index: Int, value: Long) {
        startField(field, index)
        LOG.debug("addLong({})", value)
        recordConsumer.addLong(value)
        endField(field, index)
    }

    private fun endField(field: String, index: Int) {
        LOG.debug("endField({},{})", field, index)
        endField?.also {
            recordConsumer.endField(it, endIndex)
        }
        endField = field
        endIndex = index
    }

    protected fun addPrimitiveBINARY(field: String, index: Int, value: Binary) {
        startField(field, index)
        LOG.debug("addBinary({})", value)
        recordConsumer.addBinary(value)
        endField(field, index)
    }

    protected fun addPrimitiveINT32(field: String, index: Int, value: Int) {
        startField(field, index)
        LOG.debug("addInteger({})", value)
        recordConsumer.addInteger(value)
        endField(field, index)
    }

    protected fun endGroup(field: String, index: Int) {
        endField?.also {
            // close the previous field
            recordConsumer.endField(it, endIndex)
            endField = null
        }
        LOG.debug("endGroup()")
        recordConsumer.endGroup()
        endField(field, index)
    }

    protected fun endMessage() {
        endField?.also {
            // close the previous field
            recordConsumer.endField(it, endIndex)
            endField = null
        }
        LOG.debug("endMessage()")
        recordConsumer.endMessage()
    }

    protected fun error(message: String) {
        throw ParquetDecodingException(message)
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(BaseRecordReader::class.java)
    }
}
