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
 * a dictionary to decode dictionary based encodings
 */
abstract class Dictionary(val encoding: Encoding) {
    abstract val maxId: Int

    open fun decodeToBinary(id: Int): Binary {
        throw UnsupportedOperationException(javaClass.getName())
    }

    open fun decodeToInt(id: Int): Int {
        throw UnsupportedOperationException(javaClass.getName())
    }

    open fun decodeToLong(id: Int): Long {
        throw UnsupportedOperationException(javaClass.getName())
    }

    open fun decodeToFloat(id: Int): Float {
        throw UnsupportedOperationException(javaClass.getName())
    }

    open fun decodeToDouble(id: Int): Double {
        throw UnsupportedOperationException(javaClass.getName())
    }

    open fun decodeToBoolean(id: Int): Boolean {
        throw UnsupportedOperationException(javaClass.getName())
    }
}
