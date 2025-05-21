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

import org.junit.Assert
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.Arrays

object VariantTestUtil {
    private const val RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

    /** Random number generator for generating random strings  */
    private val random = SecureRandom(byteArrayOf(1, 2, 3, 4, 5))

    val EMPTY_METADATA: ByteBuffer = ByteBuffer.wrap(byteArrayOf(1))

    fun checkType(v: Variant?, expectedBasicType: Int, expectedType: Variant.Type?) {
        requireNotNull(v)
        Assert.assertEquals(
            expectedBasicType.toLong(),
            (v.value.get(v.value.position()).toInt() and VariantUtil.BASIC_TYPE_MASK).toLong()
        )
        Assert.assertEquals(expectedType, v.type)
    }

    fun randomString(len: Int) = buildString(len) {
        repeat(len) {
            append(RANDOM_CHARS[random.nextInt(RANDOM_CHARS.length)])
        }
    }

    fun testVariant(v: Variant, consumer: (Variant) -> Unit) {
        consumer(v)
        // Create new Variant with different byte offsets
        val newValue = ByteArray(v.value.limit() + 50)
        val newMetadata = ByteArray(v.metadata.limit() + 50)
        Arrays.fill(newValue, 0xFF.toByte())
        Arrays.fill(newMetadata, 0xFF.toByte())
        v.value.position(0)
        v.value.get(newValue, 25, v.value.limit())
        v.value.position(0)
        v.metadata.position(0)
        v.metadata.get(newMetadata, 25, v.metadata.limit())
        v.metadata.position(0)
        val v2 = Variant(
            ByteBuffer.wrap(newValue, 25, v.value.limit()),
            ByteBuffer.wrap(newMetadata, 25, v.metadata.limit())
        )
        consumer(v2)
    }

    fun primitiveHeader(type: Int): Byte {
        return (type shl 2).toByte()
    }

    fun metadataHeader(isSorted: Boolean, offsetSize: Int): Byte {
        return (((offsetSize - 1) shl 6) or (if (isSorted) 16 else 0) or 1).toByte()
    }

    fun constructString(value: String): ByteArray {
        return ByteBuffer.allocate(value.length + 5)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put(primitiveHeader(16))
            .putInt(value.length)
            .put(value.toByteArray(StandardCharsets.UTF_8))
            .array()
    }

    fun getMinIntegerSize(value: Int): Int {
        return if (value <= 0xFF) 1 else if (value <= 0xFFFF) 2 else if (value <= 0xFFFFFF) 3 else 4
    }

    fun writeVarlenInt(buffer: ByteBuffer, value: Int, valueSize: Int) {
        when (valueSize) {
            1 -> buffer.put(value.toByte())
            2 -> buffer.putShort(value.toShort())
            3 -> {
                buffer.put((value and 0xFF).toByte())
                buffer.put(((value shr 8) and 0xFF).toByte())
                buffer.put(((value shr 16) and 0xFF).toByte())
            }
            else -> buffer.putInt(value)
        }
    }
}
