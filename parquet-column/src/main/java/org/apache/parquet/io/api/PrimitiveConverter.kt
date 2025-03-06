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
package org.apache.parquet.io.api

import org.apache.parquet.column.Dictionary

/**
 * converter for leaves of the schema
 */
abstract class PrimitiveConverter : Converter() {
    override val isPrimitive: Boolean = true

    override fun asPrimitiveConverter() = this

    /**
     * if it returns true we will attempt to use dictionary based conversion instead
     *
     * @return if dictionary is supported
     */
    open fun hasDictionarySupport(): Boolean {
        return false
    }

    /**
     * Set the dictionary to use if the data was encoded using dictionary encoding
     * and the converter hasDictionarySupport().
     *
     * @param dictionary the dictionary to use for conversion
     */
    open fun setDictionary(dictionary: Dictionary) {
        throw UnsupportedOperationException(javaClass.getName())
    }

    /* runtime calls  */
    /**
     * add a value based on the dictionary set with setDictionary()
     * Will be used if the Converter has dictionary support and the data was encoded using a dictionary
     *
     * @param dictionaryId the id in the dictionary of the value to add
     */
    open fun addValueFromDictionary(dictionaryId: Int) {
        throw UnsupportedOperationException(javaClass.getName())
    }

    /**
     * @param value value to set
     */
    open fun addBinary(value: Binary) {
        throw UnsupportedOperationException(javaClass.getName())
    }

    /**
     * @param value value to set
     */
    open fun addBoolean(value: Boolean) {
        throw UnsupportedOperationException(javaClass.getName())
    }

    /**
     * @param value value to set
     */
    open fun addDouble(value: Double) {
        throw UnsupportedOperationException(javaClass.getName())
    }

    /**
     * @param value value to set
     */
    open fun addFloat(value: Float) {
        throw UnsupportedOperationException(javaClass.getName())
    }

    /**
     * @param value value to set
     */
    open fun addInt(value: Int) {
        throw UnsupportedOperationException(javaClass.getName())
    }

    /**
     * @param value value to set
     */
    open fun addLong(value: Long) {
        throw UnsupportedOperationException(javaClass.getName())
    }
}
