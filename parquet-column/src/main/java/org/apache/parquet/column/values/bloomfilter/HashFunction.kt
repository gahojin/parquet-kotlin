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
package org.apache.parquet.column.values.bloomfilter

import java.nio.ByteBuffer

/**
 * A interface contains a set of hash functions used by Bloom filter.
 */
interface HashFunction {
    /**
     * compute the hash value for a byte array.
     *
     * @param input the input byte array
     * @return a result of long value.
     */
    fun hashBytes(input: ByteArray): Long

    /**
     * compute the hash value for a ByteBuffer.
     *
     * @param input the input ByteBuffer
     * @return a result of long value.
     */
    fun hashByteBuffer(input: ByteBuffer): Long
}
