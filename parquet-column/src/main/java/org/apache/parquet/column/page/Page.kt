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

import java.util.*

/**
 * one page in a chunk
 *
 * @property uncompressedSize the uncompressed size of the page when the bytes are compressed
 */
abstract class Page internal constructor(
    val compressedSize: Int,
    val uncompressedSize: Int
) {
    // Visible for testing
    // Note: the following field is only used for testing purposes and are NOT used in checksum
    // verification. There crc value here will merely be a copy of the actual crc field read in
    // ParquetFileReader.Chunk.readAllPages()
    var crc: OptionalInt = OptionalInt.empty()
        private set

    // Visible for testing
    fun setCrc(crc: Int) {
        this.crc = OptionalInt.of(crc)
    }
}
