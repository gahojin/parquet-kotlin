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

import java.util.concurrent.ConcurrentHashMap

/**
 * returns canonical representation of objects (similar to String.intern()) to save memory
 * if a.equals(b) then canonicalize(a) == canonicalize(b)
 * this class is thread safe
 *
 * @param <T> the type of values canonicalized by subclasses
 */
open class Canonicalizer<T> {
    private val canonicals = ConcurrentHashMap<T, T>()

    /**
     * @param value the value to canonicalize
     * @return the corresponding canonical value
     */
    fun canonicalize(value: T): T {
        return canonicals.getOrPut(value) {
            toCanonical(value)
        }
    }

    /**
     * @param value the value to canonicalize if needed
     * @return the canonicalized value
     */
    protected open fun toCanonical(value: T): T = value
}
