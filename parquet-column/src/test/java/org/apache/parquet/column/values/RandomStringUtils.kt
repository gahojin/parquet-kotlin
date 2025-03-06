/*
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
package org.apache.parquet.column.values

import java.security.SecureRandom

object RandomStringUtils {
    private val alphabetics: List<Char> = ('a'..'z') + ('A'..'Z')
    private val alphanumeric: List<Char> = ('a'..'z') + ('A'..'Z') + ('0'..'9')
    private val secure = SecureRandom()

    @JvmStatic
    fun randomAlphabetic(length: Int) = buildString {
        repeat(length) {
            append(alphabetics[secure.nextInt(alphabetics.size)])
        }
    }

    @JvmStatic
    fun randomAlphanumeric(length: Int) = buildString {
        repeat(length) {
            append(alphanumeric[secure.nextInt(alphabetics.size)])
        }
    }

    @JvmStatic
    fun randomAlphabetic(minLengthInclusive: Int, maxLengthExclusive: Int): String {
        return random(minLengthInclusive, maxLengthExclusive) {
            randomAlphabetic(it)
        }
    }

    @JvmStatic
    fun randomAlphanumeric(minLengthInclusive: Int, maxLengthExclusive: Int): String {
        return random(minLengthInclusive, maxLengthExclusive) {
            randomAlphanumeric(it)
        }
    }

    private inline fun random(minLengthInclusive: Int, maxLengthExclusive: Int, block: (Int) -> String): String {
        val length = if (maxLengthExclusive > minLengthInclusive) {
            minLengthInclusive + secure.nextInt(maxLengthExclusive - minLengthInclusive)
        } else {
            minLengthInclusive
        }
        return block(length)
    }
}
