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

import java.io.BufferedOutputStream
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/**
 * `LocalOutputFile` is an implementation needed by Parquet to write
 * to local data files using [PositionOutputStream] instances.
 */
class LocalOutputFile(
    path: Path,
) : OutputFile {
    private val _path: Path = path

    private inner class LocalPositionOutputStream(
        buffer: Int,
        vararg openOption: StandardOpenOption,
    ) : PositionOutputStream() {
        private val stream = BufferedOutputStream(Files.newOutputStream(_path, *openOption), buffer)
        override var pos: Long = 0L
            private set

        @Throws(IOException::class)
        override fun write(data: Int) {
            pos++
            stream.write(data)
        }

        @Throws(IOException::class)
        override fun write(data: ByteArray) {
            pos += data.size.toLong()
            stream.write(data)
        }

        @Throws(IOException::class)
        override fun write(data: ByteArray, off: Int, len: Int) {
            pos += len.toLong()
            stream.write(data, off, len)
        }

        @Throws(IOException::class)
        override fun flush() {
            stream.flush()
        }

        @Throws(IOException::class)
        override fun close() {
            stream.use { it.flush() }
        }
    }

    @Throws(IOException::class)
    override fun create(blockSize: Long): PositionOutputStream {
        return LocalPositionOutputStream(
            buffer = BUFFER_SIZE_DEFAULT,
            StandardOpenOption.CREATE_NEW,
        )
    }

    @Throws(IOException::class)
    override fun createOrOverwrite(blockSize: Long): PositionOutputStream {
        return LocalPositionOutputStream(
            buffer = BUFFER_SIZE_DEFAULT,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
        )
    }

    override fun supportsBlockSize(): Boolean = false

    override fun defaultBlockSize(): Long = -1L

    override val path: String? = _path.toString()

    companion object {
        private const val BUFFER_SIZE_DEFAULT = 4096
    }
}
