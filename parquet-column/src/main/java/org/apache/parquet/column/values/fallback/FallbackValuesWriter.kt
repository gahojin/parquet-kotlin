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
package org.apache.parquet.column.values.fallback

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.column.Encoding
import org.apache.parquet.column.page.DictionaryPage
import org.apache.parquet.column.values.RequiresFallback
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.io.api.Binary

/**
 * @property initialWriter writer to start with
 * @property fallBackWriter fallback
 */
class FallbackValuesWriter<I, F : ValuesWriter>(
    @JvmField val initialWriter: I,
    @JvmField val fallBackWriter: F,
) : ValuesWriter() where I : ValuesWriter, I : RequiresFallback {
    private var fellBackAlready = false

    /** writer currently written to */
    private var currentWriter: ValuesWriter = initialWriter

    private var initialUsedAndHadDictionary = false

    // use raw data size to decide if we want to flush the page
    // so the actual size of the page written could be much more smaller
    // due to dictionary encoding. This prevents page being too big when fallback happens.
    /* size of raw data, even if dictionary is used, it will not have effect on raw data size, it is used to decide
       * if fall back to plain encoding is better by comparing rawDataByteSize with Encoded data size
       * It's also used in getBufferedSize, so the page will be written based on raw data size
       */
    override var bufferedSize: Long = 0L
        private set

    /**
     * indicates if this is the first page being processed
     */
    private var firstPage = true

    override val bytes: BytesInput
        get() {
            if (!fellBackAlready && firstPage) {
                // we use the first page to decide if we're going to use this encoding
                val bytes = initialWriter!!.bytes
                if (!initialWriter.isCompressionSatisfying(this.bufferedSize, bytes.size())) {
                    fallBack()
                } else {
                    return bytes
                }
            }
            return currentWriter.bytes
        }

    override val encoding: Encoding
        get() {
            val encoding = currentWriter.encoding
            if (!fellBackAlready && !initialUsedAndHadDictionary) {
                initialUsedAndHadDictionary = encoding.usesDictionary()
            }
            return encoding
        }

    override val allocatedSize: Long
        get() = currentWriter.allocatedSize

    override fun reset() {
        this.bufferedSize = 0
        firstPage = false
        currentWriter.reset()
    }

    override fun close() {
        initialWriter.close()
        fallBackWriter.close()
    }

    override fun toDictPageAndClose(): DictionaryPage? {
        return if (initialUsedAndHadDictionary) {
            initialWriter.toDictPageAndClose()
        } else {
            currentWriter.toDictPageAndClose()
        }
    }

    override fun resetDictionary() {
        if (initialUsedAndHadDictionary) {
            initialWriter.resetDictionary()
        } else {
            currentWriter.resetDictionary()
        }
        currentWriter = initialWriter
        fellBackAlready = false
        initialUsedAndHadDictionary = false
        firstPage = true
    }

    override fun memUsageString(prefix: String): String {
        return "%s FallbackValuesWriter{\n" + "%s\n" + "%s\n" + "%s}\n".format(
            prefix,
            initialWriter.memUsageString("$prefix initial:"),
            fallBackWriter.memUsageString("$prefix fallback:"),
            prefix
        )
    }

    private fun checkFallback() {
        if (!fellBackAlready && initialWriter.shouldFallBack()) {
            fallBack()
        }
    }

    private fun fallBack() {
        fellBackAlready = true
        initialWriter.fallBackAllValuesTo(fallBackWriter)
        currentWriter = fallBackWriter
    }

    // passthrough writing the value
    override fun writeByte(value: Int) {
        bufferedSize += 1
        currentWriter.writeByte(value)
        checkFallback()
    }

    override fun writeBytes(v: Binary) {
        // for rawdata, length(4 bytes int) is stored, followed by the binary content itself
        bufferedSize += (v.length() + 4).toLong()
        currentWriter.writeBytes(v)
        checkFallback()
    }

    override fun writeInteger(v: Int) {
        bufferedSize += 4
        currentWriter.writeInteger(v)
        checkFallback()
    }

    override fun writeLong(v: Long) {
        bufferedSize += 8
        currentWriter.writeLong(v)
        checkFallback()
    }

    override fun writeFloat(v: Float) {
        bufferedSize += 4
        currentWriter.writeFloat(v)
        checkFallback()
    }

    override fun writeDouble(v: Double) {
        bufferedSize += 8
        currentWriter.writeDouble(v)
        checkFallback()
    }

    companion object {
        @JvmStatic
        fun <I, F : ValuesWriter> of(
            initialWriter: I,
            fallBackWriter: F,
        ): FallbackValuesWriter<I, F> where I : ValuesWriter, I : RequiresFallback {
            return FallbackValuesWriter(initialWriter, fallBackWriter)
        }
    }
}
