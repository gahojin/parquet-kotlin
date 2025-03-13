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
import org.apache.parquet.bytes.BytesUtils
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.io.ParquetDecodingException
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.Binary.Companion.fromConstantByteBuffer
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class BinaryPlainValuesReader : ValuesReader() {
    private lateinit var `in`: ByteBufferInputStream

    override fun readBytes(): Binary {
        try {
            val length = BytesUtils.readIntLittleEndian(`in`)
            return fromConstantByteBuffer(`in`.slice(length))
        } catch (e: IOException) {
            throw ParquetDecodingException("could not read bytes at offset ${`in`.position()}", e)
        } catch (e: RuntimeException) {
            throw ParquetDecodingException("could not read bytes at offset ${`in`.position()}", e)
        }
    }

    override fun skip() {
        try {
            val length = BytesUtils.readIntLittleEndian(`in`)
            `in`.skipFully(length.toLong())
        } catch (e: IOException) {
            throw ParquetDecodingException("could not skip bytes at offset ${`in`.position()}", e)
        } catch (e: RuntimeException) {
            throw ParquetDecodingException("could not skip bytes at offset ${`in`.position()}", e)
        }
    }

    @Throws(IOException::class)
    override fun initFromPage(valueCount: Int, stream: ByteBufferInputStream) {
        LOG.debug(
            "init from page at offset {} for length {}",
            stream.position(),
            stream.available() - stream.position(),
        )
        `in` = stream.remainingStream()
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(BinaryPlainValuesReader::class.java)
    }
}
