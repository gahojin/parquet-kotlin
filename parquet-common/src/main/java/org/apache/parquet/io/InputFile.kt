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
package org.apache.parquet.io

import java.io.IOException

/**
 * `InputFile` is an interface with the methods needed by Parquet to read
 * data files using [SeekableInputStream] instances.
 */
interface InputFile {
    /** the total length of the file, in bytes. */
    @get:Throws(IOException::class)
    val length: Long

    /**
     * Open a new [SeekableInputStream] for the underlying data file.
     *
     * @return a new [SeekableInputStream] to read the file
     * @throws IOException if the stream cannot be opened
     */
    @Throws(IOException::class)
    fun newStream(): SeekableInputStream
}
