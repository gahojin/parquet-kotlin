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
package org.apache.parquet.filter2.recordlevel

import org.apache.parquet.column.Dictionary
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

/**
 * see [FilteringRecordMaterializer]
 *
 * This pass-through proxy for a delegate [PrimitiveConverter] also
 * updates the [ValueInspector]s of a [IncrementallyUpdatedFilterPredicate]
 */
class FilteringPrimitiveConverter(
    private val delegate: PrimitiveConverter,
    private val valueInspectors: Array<ValueInspector>,
) : PrimitiveConverter() {

    // TODO: this works, but
    // TODO: essentially turns off the benefits of dictionary support
    // TODO: even if the underlying delegate supports it.
    // TODO: we should support it here. (https://issues.apache.org/jira/browse/PARQUET-36)
    override fun hasDictionarySupport(): Boolean = false

    override fun setDictionary(dictionary: Dictionary) {
        throw UnsupportedOperationException("FilteringPrimitiveConverter doesn't have dictionary support")
    }

    override fun addValueFromDictionary(dictionaryId: Int) {
        throw UnsupportedOperationException("FilteringPrimitiveConverter doesn't have dictionary support")
    }

    override fun addBinary(value: Binary) {
        for (valueInspector in valueInspectors) {
            valueInspector.update(value)
        }
        delegate.addBinary(value)
    }

    override fun addBoolean(value: Boolean) {
        for (valueInspector in valueInspectors) {
            valueInspector.update(value)
        }
        delegate.addBoolean(value)
    }

    override fun addDouble(value: Double) {
        for (valueInspector in valueInspectors) {
            valueInspector.update(value)
        }
        delegate.addDouble(value)
    }

    override fun addFloat(value: Float) {
        for (valueInspector in valueInspectors) {
            valueInspector.update(value)
        }
        delegate.addFloat(value)
    }

    override fun addInt(value: Int) {
        for (valueInspector in valueInspectors) {
            valueInspector.update(value)
        }
        delegate.addInt(value)
    }

    override fun addLong(value: Long) {
        for (valueInspector in valueInspectors) {
            valueInspector.update(value)
        }
        delegate.addLong(value)
    }
}
