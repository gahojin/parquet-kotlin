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
package org.apache.parquet.conf

/**
 * Configuration interface with the methods necessary to configure Parquet applications.
 */
interface ParquetConfiguration : Iterable<@JvmSuppressWildcards Map.Entry<String, String>> {
    /**
     * Sets the value of the name property.
     *
     * @param name  the property to set
     * @param value the value to set the property to
     */
    fun set(name: String, value: String)

    /**
     * Sets the value of the name property to a long.
     *
     * @param name  the property to set
     * @param value the value to set the property to
     */
    fun setLong(name: String, value: Long)

    /**
     * Sets the value of the name property to an integer.
     *
     * @param name  the property to set
     * @param value the value to set the property to
     */
    fun setInt(name: String, value: Int)

    /**
     * Sets the value of the name property to a boolean.
     *
     * @param name  the property to set
     * @param value the value to set the property to
     */
    fun setBoolean(name: String, value: Boolean)

    /**
     * Sets the value of the name property to an array of comma delimited values.
     *
     * @param name  the property to set
     * @param value the values to set the property to
     */
    fun setStrings(name: String, vararg value: String)

    /**
     * Sets the value of the name property to a class.
     *
     * @param name  the property to set
     * @param value the value to set the property to
     * @param xface the interface implemented by the value
     */
    fun setClass(name: String, value: Class<*>, xface: Class<*>)

    /**
     * Gets the value of the name property. Returns null if no such value exists.
     *
     * @param name the property to retrieve the value of
     * @return the value of the property, or null if it does not exist
     */
    fun get(name: String): String?

    /**
     * Gets the value of the name property. Returns the default value if no such value exists.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the value of the property, or the default value if it does not exist
     */
    fun get(name: String, defaultValue: String): String

    /**
     * Gets the value of the name property as a long. Returns the default value if no such value exists.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the value of the property as a long, or the default value if it does not exist
     */
    fun getLong(name: String, defaultValue: Long): Long

    /**
     * Gets the value of the name property as an integer. Returns the default value if no such value exists.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the value of the property as an integer, or the default value if it does not exist
     */
    fun getInt(name: String, defaultValue: Int): Int

    /**
     * Gets the value of the name property as a boolean. Returns the default value if no such value exists.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the value of the property as a boolean, or the default value if it does not exist
     */
    fun getBoolean(name: String, defaultValue: Boolean): Boolean

    /**
     * Gets the trimmed value of the name property. Returns null if no such value exists.
     *
     * @param name the property to retrieve the value of
     * @return the trimmed value of the property, or null if it does not exist
     */
    fun getTrimmed(name: String): String?

    /**
     * Gets the trimmed value of the name property as a boolean.
     * Returns the default value if no such value exists.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the trimmed value of the property, or the default value if it does not exist
     */
    fun getTrimmed(name: String, defaultValue: String): String

    /**
     * Gets the value of the name property as an array of [String]s.
     * Returns the default value if no such value exists.
     * Interprets the stored value as a comma delimited array.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the value of the property as an array, or the default value if it does not exist
     */
    fun getStrings(name: String, defaultValue: Array<String>): Array<String>

    /**
     * Gets the value of the name property as a class. Returns the default value if no such value exists.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the value of the property as a class, or the default value if it does not exist
     */
    fun getClass(name: String, defaultValue: Class<*>): Class<*>

    /**
     * Gets the value of the name property as a class implementing the xface interface.
     * Returns the default value if no such value exists.
     *
     * @param name         the property to retrieve the value of
     * @param defaultValue the default return if no value is set for the property
     * @return the value of the property as a class, or the default value if it does not exist
     */
    fun <U> getClass(name: String, defaultValue: Class<out U>, xface: Class<U>): Class<out U>

    /**
     * Load a class by name.
     *
     * @param name the name of the [Class] to load
     * @return the loaded class
     * @throws ClassNotFoundException when the specified class cannot be found
     */
    @Throws(ClassNotFoundException::class)
    fun getClassByName(name: String): Class<*>
}
