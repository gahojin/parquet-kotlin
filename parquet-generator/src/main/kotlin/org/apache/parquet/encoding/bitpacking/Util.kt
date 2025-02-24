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
package org.apache.parquet.encoding.bitpacking

const val MAX_BITS_FOR_INT: Int = 32
const val MAX_BITS_FOR_LONG: Int = 64

fun genMask(width: Int, isLong: Boolean = false): Long {
    val maxBitWidth = if (isLong) MAX_BITS_FOR_LONG else MAX_BITS_FOR_INT
    if (width >= maxBitWidth) {
        // -1 is always ones (11111...1111). It covers all it can possibly can.
        return -1L
    }

    return (1L shl width) - 1L
}
