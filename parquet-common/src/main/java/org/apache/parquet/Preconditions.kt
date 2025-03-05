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
package org.apache.parquet

/**
 * Utility for parameter validation
 */
object Preconditions {
    /**
     * Precondition-style validation that throws [IllegalArgumentException].
     *
     * @param isValid `true` if valid, `false` if an exception should be
     * thrown
     * @param message A String message for the exception.
     * @throws IllegalArgumentException if `isValid` is false
     */
    @JvmStatic
    @Throws(IllegalArgumentException::class)
    fun checkArgument(isValid: Boolean, message: String) {
        require(isValid) { message }
    }

    /**
     * Precondition-style validation that throws [IllegalArgumentException].
     *
     * @param isValid `true` if valid, `false` if an exception should be
     * thrown
     * @param message A String message for the exception.
     * @param args    Objects used to fill in `%s` placeholders in the message
     * @throws IllegalArgumentException if `isValid` is false
     */
    @JvmStatic
    @Throws(IllegalArgumentException::class)
    fun checkArgument(isValid: Boolean, message: String, vararg args: Any?) {
        require(isValid) { message.format(*strings(args)) }
    }

    /**
     * Precondition-style validation that throws [IllegalStateException].
     *
     * @param isValid `true` if valid, `false` if an exception should be
     * thrown
     * @param message A String message for the exception.
     * @throws IllegalStateException if `isValid` is false
     */
    @JvmStatic
    @Throws(IllegalStateException::class)
    fun checkState(isValid: Boolean, message: String) {
        check(isValid) { message }
    }

    /**
     * Precondition-style validation that throws [IllegalStateException].
     *
     * @param isValid `true` if valid, `false` if an exception should be
     * thrown
     * @param message A String message for the exception.
     * @param args    Objects used to fill in `%s` placeholders in the message
     * @throws IllegalStateException if `isValid` is false
     */
    @JvmStatic
    @Throws(IllegalStateException::class)
    fun checkState(isValid: Boolean, message: String, vararg args: Any?) {
        check(isValid) { message.format(*strings(args)) }
    }

    private fun strings(objects: Array<out Any?>): Array<String?> {
        val strings = arrayOfNulls<String>(objects.size)
        var i = 0
        while (i < objects.size) {
            strings[i] = objects[i].toString()
            i += 1
        }
        return strings
    }
}
