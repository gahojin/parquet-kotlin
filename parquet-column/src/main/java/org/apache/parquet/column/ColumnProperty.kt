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

import org.apache.parquet.hadoop.metadata.ColumnPath
import org.apache.parquet.hadoop.metadata.ColumnPath.Companion.fromDotString
import org.apache.parquet.hadoop.metadata.ColumnPath.Companion.get

/**
 * Represents a Parquet property that may have different values for the different columns.
 */
internal abstract class ColumnProperty<T> {
    private open class DefaultColumnProperty<T>(
        override val defaultValue: T,
    ) : ColumnProperty<T>() {
        override fun getValue(columnPath: ColumnPath): T {
            return defaultValue
        }

        override fun toString(): String {
            return defaultValue.toString()
        }
    }

    private class MultipleColumnProperty<T>(
        defaultValue: T,
        var values: MutableMap<ColumnPath, T>,
    ) : DefaultColumnProperty<T>(defaultValue) {

        override fun getValue(columnPath: ColumnPath): T {
            return values.getOrDefault(columnPath, defaultValue)
        }

        override fun toString(): String {
            return "$defaultValue $values"
        }
    }

    internal class Builder<T> internal constructor(
        private var defaultValue: T,
    ) {
        internal val values: MutableMap<ColumnPath, T> = hashMapOf()

        fun withDefaultValue(defaultValue: T) = apply {
            this.defaultValue = defaultValue
        }

        fun withValue(columnPath: ColumnPath, value: T) = apply {
            values.put(columnPath, value)
        }

        fun withValue(columnPath: String, value: T) = withValue(fromDotString(columnPath), value)

        fun withValue(
            columnDescriptor: ColumnDescriptor,
            value: T,
        ) = withValue(get(*columnDescriptor.path), value)

        fun build(): ColumnProperty<T> {
            return if (values.isEmpty()) {
                DefaultColumnProperty<T>(defaultValue)
            } else {
                MultipleColumnProperty<T>(defaultValue, values)
            }
        }
    }

    abstract val defaultValue: T

    abstract fun getValue(columnPath: ColumnPath): T

    fun getValue(columnPath: String) = getValue(fromDotString(columnPath))

    fun getValue(columnDescriptor: ColumnDescriptor) = getValue(get(*columnDescriptor.path))

    companion object {
        fun <T> builder(defaultValue: T): Builder<T> {
            return Builder<T>(defaultValue)
        }

        fun <T> builder(defaultValue: () -> T): Builder<T> {
            return Builder<T>(defaultValue())
        }

        fun <T> builder(toCopy: ColumnProperty<T>): Builder<T> {
            return builder<T>(toCopy.defaultValue).also {
                if (toCopy is MultipleColumnProperty<T>) {
                    it.values.putAll(toCopy.values)
                }
            }
        }
    }
}
