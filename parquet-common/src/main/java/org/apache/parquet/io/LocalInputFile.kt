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
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Path

/**
 * `LocalInputFile` is an implementation needed by Parquet to read
 * from local data files using [SeekableInputStream] instances.
 */
class LocalInputFile(
    private val path: Path,
) : InputFile {
    @get:Throws(IOException::class)
    override var length: Long = -1L
        get() {
            if (field == -1L) {
                field = path.toFile().length()
            }
            return field
        }
        private set

    @Throws(IOException::class)
    override fun newStream(): SeekableInputStream {
        return object : SeekableInputStream() {
            private val randomAccessFile = RandomAccessFile(path.toFile(), "r")

            @Throws(IOException::class)
            override fun read(): Int {
                return randomAccessFile.read()
            }

            @get:Throws(IOException::class)
            override val pos: Long
                get() = randomAccessFile.filePointer

            @Throws(IOException::class)
            override fun seek(newPos: Long) {
                randomAccessFile.seek(newPos)
            }

            @Throws(IOException::class)
            override fun readFully(bytes: ByteArray) {
                randomAccessFile.readFully(bytes)
            }

            @Throws(IOException::class)
            override fun readFully(bytes: ByteArray, start: Int, len: Int) {
                randomAccessFile.readFully(bytes, start, len)
            }

            @Throws(IOException::class)
            override fun read(buf: ByteBuffer): Int {
                val buffer = ByteArray(buf.remaining())
                val code = read(buffer)
                buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining())
                return code
            }

            @Throws(IOException::class)
            override fun readFully(buf: ByteBuffer) {
                val buffer = ByteArray(buf.remaining())
                readFully(buffer)
                buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining())
            }

            @Throws(IOException::class)
            override fun close() {
                randomAccessFile.close()
            }
        }
    }
}
