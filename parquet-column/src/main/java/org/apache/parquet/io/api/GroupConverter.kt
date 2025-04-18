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
 * converter for group nodes
 */
abstract class GroupConverter : Converter() {
    override val isPrimitive: Boolean = false

    override fun asGroupConverter(): GroupConverter = this

    /**
     * called at initialization based on schema
     * must consistently return the same object
     *
     * @param fieldIndex index of the field in this group
     * @return the corresponding converter
     */
    abstract fun getConverter(fieldIndex: Int): Converter

    /* runtime calls  **/
    /**
     * called at the beginning of the group managed by this converter
     */
    abstract fun start()

    /**
     * call at the end of the group
     */
    abstract fun end()
}
