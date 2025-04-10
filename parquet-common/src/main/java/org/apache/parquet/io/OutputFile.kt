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
 * `OutputFile` is an interface with the methods needed by Parquet to write
 * data files using [PositionOutputStream] instances.
 */
interface OutputFile {
    /**  the path of the file, as a [String]. */
    val path: String?
        get() = null

    /**
     * Opens a new [PositionOutputStream] for the data file to create.
     *
     * @return a new [PositionOutputStream] to write the file
     * @throws IOException if the stream cannot be opened
     */
    @Throws(IOException::class)
    fun create(blockSizeHint: Long): PositionOutputStream

    /**
     * Opens a new [PositionOutputStream] for the data file to create or overwrite.
     *
     * @return a new [PositionOutputStream] to write the file
     * @throws IOException if the stream cannot be opened
     */
    @Throws(IOException::class)
    fun createOrOverwrite(blockSizeHint: Long): PositionOutputStream

    /**
     * @return a flag indicating if block size is supported.
     */
    fun supportsBlockSize(): Boolean

    /**
     * @return the default block size.
     */
    fun defaultBlockSize(): Long
}
