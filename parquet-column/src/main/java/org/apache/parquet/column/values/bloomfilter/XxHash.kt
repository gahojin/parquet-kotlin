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

import net.openhft.hashing.LongHashFunction
import java.nio.ByteBuffer

/**
 * The implementation of HashFunction interface. The XxHash uses XXH64 version xxHash
 * with a seed of 0.
 */
class XxHash : HashFunction {
    override fun hashBytes(input: ByteArray): Long {
        return LongHashFunction.xx().hashBytes(input)
    }

    override fun hashByteBuffer(input: ByteBuffer): Long {
        return LongHashFunction.xx().hashBytes(input)
    }
}
