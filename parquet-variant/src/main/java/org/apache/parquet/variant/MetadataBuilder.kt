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
package org.apache.parquet.variant

import org.apache.parquet.variant.VariantUtil.writeLong
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * Metadata that adds keys as needed.
 */
class MetadataBuilder : Metadata {
    /** The dictionary for mapping keys to monotonically increasing ids.  */
    private val dictionary = HashMap<String, Int>()

    /** The keys in the dictionary, in id order.  */
    private val dictionaryKeys = ArrayList<ByteArray>()

    override val encodedBuffer: ByteBuffer
        get() {
            val numKeys = dictionaryKeys.size
            // Use long to avoid overflow in accumulating lengths.
            var dictionaryTotalDataSize: Long = 0
            for (key in dictionaryKeys) {
                dictionaryTotalDataSize += key.size.toLong()
            }
            // Determine the number of bytes required per offset entry.
            // The largest offset is the one-past-the-end value, which is total data size. It's very
            // unlikely that the number of keys could be larger, but incorporate that into the calculation
            // in case of pathological data.
            val maxSize = max(dictionaryTotalDataSize, numKeys.toLong())
            val offsetSize: Int = VariantBuilder.getMinIntegerSize(maxSize.toInt())

            val offsetListOffset = 1 + offsetSize
            val dataOffset = offsetListOffset + (numKeys + 1) * offsetSize
            val metadataSize = dataOffset + dictionaryTotalDataSize

            val metadata = ByteArray(metadataSize.toInt())
            // Only unsorted dictionary keys are supported.
            // TODO: Support sorted dictionary keys.
            val headerByte = VariantUtil.VERSION or ((offsetSize - 1) shl 6)
            writeLong(metadata, 0, headerByte.toLong(), 1)
            writeLong(metadata, 1, numKeys.toLong(), offsetSize)
            var currentOffset = 0
            for (i in 0..<numKeys) {
                writeLong(metadata, offsetListOffset + i * offsetSize, currentOffset.toLong(), offsetSize)
                val key = dictionaryKeys.get(i)
                System.arraycopy(key, 0, metadata, dataOffset + currentOffset, key.size)
                currentOffset += key.size
            }
            writeLong(
                metadata,
                offsetListOffset + numKeys * offsetSize,
                currentOffset.toLong(),
                offsetSize,
            )
            return ByteBuffer.wrap(metadata)
        }

    override fun getOrInsert(key: String): Int {
        return dictionary.computeIfAbsent(key) { newKey ->
            dictionaryKeys.size.also {
                dictionaryKeys.add(newKey.toByteArray(java.nio.charset.StandardCharsets.UTF_8))
            }
        }
    }
}
