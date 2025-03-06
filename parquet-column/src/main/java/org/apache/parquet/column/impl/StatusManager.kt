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
package org.apache.parquet.column.impl

/**
 * Interface to manage the current error status. It is used to share the status of all the different (column, page,
 * etc.) writer/reader instances.
 */
internal interface StatusManager {
    /**
     * Returns whether the current process is aborted.
     *
     * @return `true` if the current process is aborted, `false` otherwise
     */
    val isAborted: Boolean

    /**
     * To be invoked if the current process is to be aborted. For example in case of an exception is occurred during
     * writing a page.
     */
    fun abort()

    companion object {
        /**
         * Creates an instance of the default [StatusManager] implementation.
         *
         * @return the newly created [StatusManager] instance
         */
        @JvmStatic
        fun create(): StatusManager {
            return object : StatusManager {
                override var isAborted: Boolean = false
                    private set

                override fun abort() {
                    isAborted = true
                }
            }
        }
    }
}
