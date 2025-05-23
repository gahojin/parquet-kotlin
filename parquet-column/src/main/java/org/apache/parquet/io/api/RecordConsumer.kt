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

/**
 * Abstraction for writing records
 * It decouples the striping algorithm from the actual record model
 * example:
 * ```
 * startMessage()
 *  startField("A", 0)
 *   addValue(1)
 *   addValue(2)
 *  endField("A", 0)
 *  startField("B", 1)
 *   startGroup()
 *    startField("C", 0)
 *     addValue(3)
 *    endField("C", 0)
 *   endGroup()
 *  endField("B", 1)
 * endMessage()
 * ```
 *
 * would produce the following message:
 * ```
 * {
 *   A: [1, 2]
 *   B: {
 *     C: 3
 *   }
 * }
 * ```
 */
abstract class RecordConsumer {
    /**
     * start a new record
     */
    abstract fun startMessage()

    /**
     * end of a record
     */
    abstract fun endMessage()

    /**
     * start of a field in a group or message
     * if the field is repeated the field is started only once and all values added in between start and end
     *
     * @param field name of the field
     * @param index of the field in the group or message
     */
    abstract fun startField(field: String, index: Int)

    /**
     * end of a field in a group or message
     *
     * @param field name of the field
     * @param index of the field in the group or message
     */
    abstract fun endField(field: String, index: Int)

    /**
     * start of a group in a field
     */
    abstract fun startGroup()

    /**
     * end of a group in a field
     */
    abstract fun endGroup()

    /**
     * add an int value in the current field
     *
     * @param value an int value
     */
    abstract fun addInteger(value: Int)

    /**
     * add a long value in the current field
     *
     * @param value a long value
     */
    abstract fun addLong(value: Long)

    /**
     * add a boolean value in the current field
     *
     * @param value a boolean value
     */
    abstract fun addBoolean(value: Boolean)

    /**
     * add a binary value in the current field
     *
     * @param value a binary value
     */
    abstract fun addBinary(value: Binary)

    /**
     * add a float value in the current field
     *
     * @param value a float value
     */
    abstract fun addFloat(value: Float)

    /**
     * add a double value in the current field
     *
     * @param value a double value
     */
    abstract fun addDouble(value: Double)

    /**
     * NoOps by default
     * Subclass class can implement its own flushing logic
     */
    // TODO: make this abstract in 2.0
    open fun flush() = Unit
}
