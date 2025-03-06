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
package org.apache.parquet.column.page

import org.apache.parquet.bytes.BytesInput
import org.apache.parquet.bytes.BytesInput.Companion.copy
import org.apache.parquet.column.Encoding
import java.io.IOException

/**
 * Data for a dictionary page
 */
class DictionaryPage(
    val bytes: BytesInput,
    uncompressedSize: Int,
    val dictionarySize: Int,
    val encoding: Encoding,
) : Page(Math.toIntExact(bytes.size()), uncompressedSize) {
    /**
     * creates an uncompressed page
     *
     * @param bytes          the content of the page
     * @param dictionarySize the value count in the dictionary
     * @param encoding       the encoding used
     */
    constructor(bytes: BytesInput, dictionarySize: Int, encoding: Encoding) : this(
        bytes,
        bytes.size().toInt(),
        dictionarySize,
        encoding
    ) // TODO: fix sizes long or int

    @Throws(IOException::class)
    fun copy(): DictionaryPage {
        return DictionaryPage(copy(bytes), uncompressedSize, dictionarySize, encoding)
    }

    override fun toString(): String {
        return ("Page [bytes.size=${bytes.size()}, entryCount=$dictionarySize, uncompressedSize=$uncompressedSize, encoding=$encoding]")
    }
}
