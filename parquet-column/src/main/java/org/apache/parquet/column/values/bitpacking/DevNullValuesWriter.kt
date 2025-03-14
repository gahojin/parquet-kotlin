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
package org.apache.parquet.column.values.bitpacking

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.Companion.empty
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.io.api.Binary

/**
 * This is a special writer that doesn't write anything. The idea being that
 * some columns will always be the same value, and this will capture that. An
 * example is the set of repetition levels for a schema with no repeated fields.
 */
object DevNullValuesWriter : ValuesWriter() {
    override val bufferedSize: Long = 0L

    override val bytes: BytesInput = empty()

    override val allocatedSize: Long = 0L

    override val encoding: Encoding = Encoding.BIT_PACKED

    override fun reset() = Unit

    override fun writeInteger(v: Int) = Unit

    override fun writeByte(value: Int) = Unit

    override fun writeBoolean(v: Boolean) = Unit

    override fun writeBytes(v: Binary) = Unit

    override fun writeLong(v: Long) = Unit

    override fun writeDouble(v: Double) = Unit

    override fun writeFloat(v: Float) = Unit

    override fun memUsageString(prefix: String): String = "${prefix}0"
}
