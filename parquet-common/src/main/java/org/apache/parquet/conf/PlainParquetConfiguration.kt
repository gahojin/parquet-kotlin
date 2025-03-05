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
 * Configuration for Parquet without Hadoop dependency.
 */
class PlainParquetConfiguration(
    private val map: MutableMap<String, String> = hashMapOf(),
) : ParquetConfiguration {
    override fun set(name: String, value: String) {
        map[name] = value
    }

    override fun setLong(name: String, value: Long) {
        set(name, value.toString())
    }

    override fun setInt(name: String, value: Int) {
        set(name, value.toString())
    }

    override fun setBoolean(name: String, value: Boolean) {
        set(name, value.toString())
    }

    override fun setStrings(name: String, vararg value: String) {
        if (value.isNotEmpty()) {
            val sb = StringBuilder(value[0])
            for (i in 1..<value.size) {
                sb.append(',')
                sb.append(value[i])
            }
            set(name, sb.toString())
        } else {
            set(name, "")
        }
    }

    override fun setClass(name: String, value: Class<*>, xface: Class<*>) {
        if (xface.isAssignableFrom(value)) {
            set(name, value.name)
        } else {
            throw RuntimeException(
                xface.canonicalName + " is not assignable from " + value.canonicalName
            )
        }
    }

    override fun get(name: String): String? {
        return map[name]
    }

    override fun get(name: String, defaultValue: String): String {
        val value = get(name)
        return value ?: defaultValue
    }

    override fun getLong(name: String, defaultValue: Long): Long {
        val value = get(name)
        return value?.toLong() ?: defaultValue
    }

    override fun getInt(name: String, defaultValue: Int): Int {
        val value = get(name)
        return value?.toInt() ?: defaultValue
    }

    override fun getBoolean(name: String, defaultValue: Boolean): Boolean {
        val value = get(name)
        return value?.toBoolean() ?: defaultValue
    }

    override fun getTrimmed(name: String): String? {
        val value = get(name)
        return value?.trim { it <= ' ' }
    }

    override fun getTrimmed(name: String, defaultValue: String): String {
        val value = get(name)
        return value?.trim { it <= ' ' } ?: defaultValue
    }

    override fun getStrings(name: String, defaultValue: Array<String>): Array<String> {
        val value = get(name)
        return value?.split(",".toRegex())?.dropLastWhile { it.isEmpty() }?.toTypedArray() ?: defaultValue
    }

    override fun getClass(name: String, defaultValue: Class<*>): Class<*> {
        val value = get(name)
        return if (value != null) {
            try {
                Class.forName(value)
            } catch (e: ClassNotFoundException) {
                throw RuntimeException(e)
            }
        } else {
            defaultValue
        }
    }

    override fun <U> getClass(name: String, defaultValue: Class<out U>, xface: Class<U>): Class<out U> {
        val value = getClass(name, defaultValue)
        if (value.isAssignableFrom(xface)) {
            return value as Class<out U>
        }
        return defaultValue
    }

    @Throws(ClassNotFoundException::class)
    override fun getClassByName(name: String): Class<*> {
        return Class.forName(name)
    }

    override fun iterator(): Iterator<Map.Entry<String, String>> {
        return map.entries.iterator()
    }
}
