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
package org.apache.parquet.variant

/**
 * Builder for creating Variant object, used by VariantBuilder.
 */
open class VariantObjectBuilder internal constructor(
    metadata: Metadata,
) : VariantBuilder(metadata) {
    /** The FieldEntry list for the fields of this object.  */
    private val fields = arrayListOf<FieldEntry>()

    /** The number of values appended to this object.  */
    var numValues: Long = 0
        protected set

    /**
     * Appends an object key to this object. This method must be called before appending any value.
     * @param key the key to append
     */
    fun appendKey(key: String) {
        check(fields.size <= numValues) { "Cannot call appendKey() before appending a value for the previous key." }
        updateLastValueSize()
        fields.add(FieldEntry(key, addDictionaryKey(key), writePos))
    }

    /**
     * Revert the last call to appendKey. May only be done if the corresponding value was not yet
     * added. Used when reading data from Parquet, where a field may be non-null, but turn out to be
     * missing (i.e. has null value and typed_value fields).
     */
    fun dropLastKey() {
        check(fields.size.toLong() == numValues + 1) { "Can only drop the last added key with no corresponding value." }
        fields.removeAt(fields.size - 1)
    }

    /**
     * Returns the list of FieldEntry in this object. The state of the object is validated, so this
     * object is guaranteed to have the same number of keys and values.
     * @return the list of fields in this object
     */
    fun validateAndGetFields(): ArrayList<FieldEntry> {
        check(fields.size.toLong() == numValues) {
            "Number of object keys (%d) do not match the number of values (%d).".format(fields.size, numValues)
        }
        checkMultipleNested("Cannot call endObject() while a nested object/array is still open.")
        updateLastValueSize()
        return fields
    }

    override fun onAppend() {
        checkAppendWhileNested()
        check(numValues == (fields.size - 1).toLong()) { "Cannot append an object value before calling appendKey()" }
        numValues++
    }

    override fun onStartNested() {
        checkMultipleNested("Cannot call startObject()/startArray() without calling endObject()/endArray() first.")
        numValues++
    }

    private fun updateLastValueSize() {
        if (!fields.isEmpty()) {
            val lastField = fields[fields.size - 1]
            lastField.updateValueSize(writePos() - lastField.offset)
        }
    }
}
