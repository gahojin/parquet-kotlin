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
package org.apache.parquet.column.values.plain

import okio.IOException
import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader
import org.apache.parquet.column.values.bitpacking.Packer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * encodes boolean for the plain encoding: one bit at a time (0 = false)
 */
class BooleanPlainValuesReader : ValuesReader() {
    private val `in` = ByteBitPackingValuesReader(1, Packer.LITTLE_ENDIAN)

    override fun readBoolean(): Boolean {
        return `in`.readInteger() != 0
    }

    override fun skip() {
        `in`.readInteger()
    }

    @Throws(IOException::class)
    override fun initFromPage(valueCount: Int, stream: ByteBufferInputStream) {
        LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available())
        `in`.initFromPage(valueCount, stream)
    }

    @Deprecated("")
    override fun getNextOffset(): Int {
        return `in`.getNextOffset()
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(BooleanPlainValuesReader::class.java)
    }
}
