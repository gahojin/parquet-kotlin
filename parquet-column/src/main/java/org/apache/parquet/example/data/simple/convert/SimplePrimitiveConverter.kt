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
package org.apache.parquet.example.data.simple.convert

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

internal class SimplePrimitiveConverter(
    private val parent: SimpleGroupConverter,
    private val index: Int,
) : PrimitiveConverter() {
    override fun addBinary(value: Binary) {
        parent.currentRecord.add(index, value)
    }

    override fun addBoolean(value: Boolean) {
        parent.currentRecord.add(index, value)
    }

    override fun addDouble(value: Double) {
        parent.currentRecord.add(index, value)
    }

    override fun addFloat(value: Float) {
        parent.currentRecord.add(index, value)
    }

    override fun addInt(value: Int) {
        parent.currentRecord.add(index, value)
    }

    override fun addLong(value: Long) {
        parent.currentRecord.add(index, value)
    }
}
