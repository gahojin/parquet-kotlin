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
package org.apache.parquet.util

import org.apache.parquet.ParquetRuntimeException

/**
 * Utility class to handle [AutoCloseable] objects.
 */
object AutoCloseables {
    /**
     * Invokes the [AutoCloseable.close] method of each specified objects in a way that guarantees that all the
     * methods will be invoked even if an exception is occurred before. It also gracefully handles `null`
     * [AutoCloseable] instances by skipping them.
     *
     * @param autoCloseables the objects to be closed
     * @throws Exception the compound exception built from the exceptions thrown by the close methods
     */
    @JvmStatic
    @Throws(Throwable::class)
    fun close(autoCloseables: Iterable<AutoCloseable?>) {
        var root: Throwable? = null
        for (autoCloseable in autoCloseables) {
            try {
                autoCloseable?.close()
            } catch (e: Throwable) {
                if (root == null) {
                    root = e
                } else {
                    root.addSuppressed(e)
                }
            }
        }
        if (root != null) {
            throw root
        }
    }

    /**
     * Works similarly to [.close] but it wraps the thrown exception (if any) into a
     * [ParquetCloseResourceException].
     */
    @JvmStatic
    @Throws(ParquetCloseResourceException::class)
    fun uncheckedClose(autoCloseables: Iterable<AutoCloseable?>) {
        try {
            close(autoCloseables)
        } catch (e: Throwable) {
            throw ParquetCloseResourceException(e)
        }
    }

    /**
     * Works similarly to [.close] but it wraps the thrown exception (if any) into a
     * [ParquetCloseResourceException].
     */
    @JvmStatic
    fun uncheckedClose(vararg autoCloseables: AutoCloseable?) {
        uncheckedClose(autoCloseables.toList())
    }

    class ParquetCloseResourceException internal constructor(e: Throwable) :
        ParquetRuntimeException("Unable to close resource", e)
}
