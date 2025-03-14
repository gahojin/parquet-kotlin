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
package org.apache.parquet.filter2.predicate

/**
 * Converts a `Class<primitive>` to it's corresponding `Class<Boxed>`, eg
 * `Class<int>` to `Class<Integer>`
 */
object PrimitiveToBoxedClass {
    private val primitiveToBoxed = mapOf(
        Boolean::class.javaPrimitiveType to Boolean::class.javaObjectType,
        Byte::class.javaPrimitiveType to Byte::class.javaObjectType,
        Short::class.javaPrimitiveType to Short::class.javaObjectType,
        Char::class.javaPrimitiveType to Char::class.javaObjectType,
        Int::class.javaPrimitiveType to Int::class.javaObjectType,
        Long::class.javaPrimitiveType to Long::class.javaObjectType,
        Float::class.javaPrimitiveType to Float::class.javaObjectType,
        Double::class.javaPrimitiveType to Double::class.javaObjectType,
    )

    @JvmStatic
    fun get(c: Class<*>): Class<*>? {
        require(c.isPrimitive) { "Class $c is not primitive!" }
        return primitiveToBoxed[c]
    }
}
