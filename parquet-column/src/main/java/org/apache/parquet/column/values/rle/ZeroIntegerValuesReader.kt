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
package org.apache.parquet.column.values.rle

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.column.values.ValuesReader
import java.io.IOException

/**
 * ColumnReader which does not read any actual data, but rather simply produces
 * an endless stream of constant values.
 * Mainly used to read definition levels when the only possible value is 0
 */
class ZeroIntegerValuesReader : ValuesReader() {
    override fun readInteger(): Int {
        return 0
    }

    @Throws(IOException::class)
    override fun initFromPage(valueCount: Int, stream: ByteBufferInputStream) {
        updateNextOffset(0)
    }

    override fun skip() = Unit

    override fun skip(n: Int) = Unit
}
