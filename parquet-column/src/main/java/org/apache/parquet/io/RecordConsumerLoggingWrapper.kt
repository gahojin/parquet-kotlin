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
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This class can be used to wrap an actual RecordConsumer and log all calls.
 *
 * all calls a delegate to the wrapped delegate
 *
 * @param delegate a wrapped record consumer that does the real work
 */
class RecordConsumerLoggingWrapper(
    private val delegate: RecordConsumer,
) : RecordConsumer() {
    private var indent: Int = 0

    override fun startField(field: String, index: Int) {
        logOpen(field)
        delegate.startField(field, index)
    }

    private fun logOpen(field: String?) {
        log("<{}>", field)
    }

    private fun log(value: Any?, vararg parameters: Any?) {
        if (LOG.isDebugEnabled) {
            LOG.debug("  ".repeat(indent) + value, *parameters)
        }
    }

    override fun startGroup() {
        ++indent
        log("<!-- start group -->")
        delegate.startGroup()
    }

    override fun addInteger(value: Int) {
        log(value)
        delegate.addInteger(value)
    }

    override fun addLong(value: Long) {
        log(value)
        delegate.addLong(value)
    }

    override fun addBoolean(value: Boolean) {
        log(value)
        delegate.addBoolean(value)
    }

    override fun addBinary(value: Binary) {
        if (LOG.isDebugEnabled) log(value.bytesUnsafe.contentToString())
        delegate.addBinary(value)
    }

    override fun addFloat(value: Float) {
        log(value)
        delegate.addFloat(value)
    }

    override fun addDouble(value: Double) {
        log(value)
        delegate.addDouble(value)
    }

    override fun flush() {
        log("<!-- flush -->")
        delegate.flush()
    }

    override fun endGroup() {
        log("<!-- end group -->")
        --indent
        delegate.endGroup()
    }

    override fun endField(field: String, index: Int) {
        logClose(field)
        delegate.endField(field, index)
    }

    private fun logClose(field: String?) {
        log("</{}>", field)
    }

    override fun startMessage() {
        log("<!-- start message -->")
        delegate.startMessage()
    }

    override fun endMessage() {
        delegate.endMessage()
        log("<!-- end message -->")
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(RecordConsumerLoggingWrapper::class.java)
    }
}
