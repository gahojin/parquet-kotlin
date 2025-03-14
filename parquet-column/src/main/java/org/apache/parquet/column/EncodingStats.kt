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
package org.apache.parquet.column

import java.util.concurrent.atomic.AtomicInteger

/**
 * EncodingStats track dictionary and data page encodings for a single column within a row group.
 * These are used when filtering row groups. For example, to filter a row group based on a column's
 * dictionary, all of the data pages in that column must be dictionary-encoded. This class provides
 * convenience methods for those checks, like [.hasNonDictionaryEncodedPages].
 */
class EncodingStats private constructor(
  @JvmField val dictStats: Map<Encoding, Number>,
  @JvmField val dataStats: Map<Encoding, Number>,
  private val usesV2Pages: Boolean
) {
    val dictionaryEncodings: Set<Encoding>
        get() = dictStats.keys

    val dataEncodings: Set<Encoding>
        get() = dataStats.keys

    fun getNumDictionaryPagesEncodedAs(enc: Encoding): Int {
        val pageCount = dictStats[enc]
        return pageCount?.toInt() ?: 0
    }

    fun getNumDataPagesEncodedAs(enc: Encoding): Int {
        val pageCount = dataStats[enc]
        return pageCount?.toInt() ?: 0
    }

    fun hasDictionaryPages(): Boolean {
        return !dictStats.isEmpty()
    }

    fun hasDictionaryEncodedPages(): Boolean {
        val encodings = dataStats.keys
        return (encodings.contains(Encoding.RLE_DICTIONARY) || encodings.contains(Encoding.PLAIN_DICTIONARY))
    }

    fun hasNonDictionaryEncodedPages(): Boolean {
        if (dataStats.isEmpty()) {
            return false // no pages
        }

        // this modifies the set, so copy it
        val encodings = dataStats.keys.toMutableSet()
        if (!encodings.remove(Encoding.RLE_DICTIONARY) && !encodings.remove(Encoding.PLAIN_DICTIONARY)) {
            return true // not dictionary encoded
        }

        if (encodings.isEmpty()) {
            return false
        }

        // at least one non-dictionary encoding is present
        return true
    }

    fun usesV2Pages(): Boolean {
        return usesV2Pages
    }

    /**
     * Used to build [EncodingStats] from metadata or to accumulate stats as pages are written.
     */
    class Builder {
        private val dictStats: MutableMap<Encoding, AtomicInteger> = linkedMapOf()
        private val dataStats: MutableMap<Encoding, AtomicInteger> = linkedMapOf()
        private var usesV2Pages = false

        fun clear(): Builder = apply {
            usesV2Pages = false
            dictStats.clear()
            dataStats.clear()
        }

        fun withV2Pages(): Builder = apply {
            usesV2Pages = true
        }

        @JvmOverloads
        fun addDictEncoding(encoding: Encoding, numPages: Int = 1): Builder = apply {
            dictStats.getOrPut(encoding) { AtomicInteger(0) }.addAndGet(numPages)
        }

        fun addDataEncodings(encodings: Iterable<Encoding>): Builder = apply {
            for (encoding in encodings) {
                addDataEncoding(encoding)
            }
        }

        @JvmOverloads
        fun addDataEncoding(encoding: Encoding, numPages: Int = 1): Builder = apply {
            dataStats.getOrPut(encoding) { AtomicInteger(0) }.addAndGet(numPages)
        }

        fun build(): EncodingStats {
            return EncodingStats(
                dictStats = dictStats.toMap(),
                dataStats = dataStats.toMap(),
                usesV2Pages = usesV2Pages,
            )
        }
    }
}
