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
package org.apache.parquet.hadoop.metadata

import java.io.Serializable

// No need to copy the array here as the only published ColumnPath instances are created by the toCanonical
class ColumnPath private constructor(
    private val p: Array<out String>,
) : Iterable<String>, Serializable {
    override fun equals(other: Any?): Boolean {
        if (other is ColumnPath) {
            return p.contentEquals(other.p)
        }
        return false
    }

    override fun hashCode(): Int {
        return p.contentHashCode()
    }

    fun toDotString(): String {
        return p.joinToString(".")
    }

    override fun toString(): String {
        return p.contentToString()
    }

    override fun iterator(): Iterator<String> {
        return p.iterator()
    }

    fun toArray(): Array<out String> {
        return p.clone()
    }

    fun toList(): List<String> {
        return p.toList()
    }

    companion object {
        private val paths = object : Canonicalizer<ColumnPath>() {
            override fun toCanonical(value: ColumnPath): ColumnPath {
                val path = value.p.map { it.intern() }.toTypedArray()
                return ColumnPath(path)
            }
        }

        @JvmStatic
        fun fromDotString(path: String): ColumnPath {
            requireNotNull(path) { "path cannot be null" }
            return get(*path.split("\\.".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
        }

        @JvmStatic
        fun get(vararg path: String): ColumnPath {
            return paths.canonicalize(ColumnPath(path))
        }
    }
}
