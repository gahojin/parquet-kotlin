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
package org.apache.parquet.column

import org.apache.parquet.io.api.Binary

/**
 * Reader for (repetition level, definition level, values) triplets.
 * At any given point in time, a ColumnReader points to a single (r, d, v) triplet.
 * In order to move to the next triplet, call [.consume].
 *
 * Depending on the type and the encoding of the column only a subset of the get* methods are implemented.
 * Dictionary specific methods enable the upper layers to read the dictionary IDs without decoding the data.
 * In particular the Converter will decode the strings in the dictionary only once and iterate on the
 * dictionary IDs instead of the values.
 *
 * Each iteration looks at the current definition level and value as well as the next repetition level:
 *
 *  *  The current definition level defines if the value is null.
 *  *  If the value is defined we can read it with the correct get*() method.
 *  *  Looking ahead to the next repetition determines what is the next column to read for in the FSA.
 */
interface ColumnReader {
    @get:Deprecated(
        """will be removed in 2.0.0; Total values might not be able to be counted before reading the values (e.g.
    in case of column index based filtering)"""
    )
    val totalValueCount: Long

    /**
     * the repetition level for the current value.
     *
     * must return 0 when isFullyConsumed() == true
     */
    val currentRepetitionLevel: Int

    /** the definition level for the current value */
    val currentDefinitionLevel: Int

    /**
     * available when the underlying encoding is dictionary based
     *
     * @return the dictionary id for the current value
     */
    val currentValueDictionaryID: Int

    /**
     * @return the current value
     */
    val integer: Int

    /**
     * @return the current value
     */
    val boolean: Boolean

    /**
     * @return the current value
     */
    val long: Long

    /**
     * @return the current value
     */
    val binary: Binary

    /**
     * @return the current value
     */
    val float: Float

    /**
     * @return the current value
     */
    val double: Double

    /**
     * @return Descriptor of the column.
     */
    val descriptor: ColumnDescriptor

    /**
     * Consume the current triplet, moving to the next value.
     */
    fun consume()

    /**
     * writes the current value to the converter
     */
    fun writeCurrentValueToConverter()

    /**
     * Skip the current value
     */
    fun skip()
}
