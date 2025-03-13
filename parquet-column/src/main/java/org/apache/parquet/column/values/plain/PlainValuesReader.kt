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
import org.apache.parquet.bytes.LittleEndianDataInputStream
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.io.ParquetDecodingException
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Plain encoding for float, double, int, long
 */
abstract class PlainValuesReader : ValuesReader() {
    protected lateinit var `in`: LittleEndianDataInputStream

    @Throws(IOException::class)
    override fun initFromPage(valueCount: Int, stream: ByteBufferInputStream) {
        LOG.debug("init from page at offset {} for length {}", stream.position(), stream.available())
        `in` = LittleEndianDataInputStream(stream.remainingStream())
    }

    override fun skip() {
        skip(1)
    }

    @Throws(IOException::class)
    fun skipBytesFully(n: Int) {
        var skipped = 0
        while (skipped < n) {
            skipped += `in`.skipBytes(n - skipped)
        }
    }

    class DoublePlainValuesReader : PlainValuesReader() {
        override fun skip(n: Int) {
            try {
                skipBytesFully(n * 8)
            } catch (e: IOException) {
                throw ParquetDecodingException("could not skip $n double values", e)
            }
        }

        override fun readDouble(): Double {
            try {
                return `in`.readDouble()
            } catch (e: IOException) {
                throw ParquetDecodingException("could not read double", e)
            }
        }
    }

    class FloatPlainValuesReader : PlainValuesReader() {
        override fun skip(n: Int) {
            try {
                skipBytesFully(n * 4)
            } catch (e: IOException) {
                throw ParquetDecodingException("could not skip $n floats", e)
            }
        }

        override fun readFloat(): Float {
            try {
                return `in`.readFloat()
            } catch (e: IOException) {
                throw ParquetDecodingException("could not read float", e)
            }
        }
    }

    class IntegerPlainValuesReader : PlainValuesReader() {
        override fun skip(n: Int) {
            try {
                `in`.skipBytes(n * 4)
            } catch (e: IOException) {
                throw ParquetDecodingException("could not skip $n ints", e)
            }
        }

        override fun readInteger(): Int {
            try {
                return `in`.readInt()
            } catch (e: IOException) {
                throw ParquetDecodingException("could not read int", e)
            }
        }
    }

    class LongPlainValuesReader : PlainValuesReader() {
        override fun skip(n: Int) {
            try {
                `in`.skipBytes(n * 8)
            } catch (e: IOException) {
                throw ParquetDecodingException("could not skip $n longs", e)
            }
        }

        override fun readLong(): Long {
            try {
                return `in`.readLong()
            } catch (e: IOException) {
                throw ParquetDecodingException("could not read long", e)
            }
        }
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(PlainValuesReader::class.java)
    }
}
