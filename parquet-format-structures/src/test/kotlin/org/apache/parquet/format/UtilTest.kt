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
package org.apache.parquet.format

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.apache.parquet.format.Util.readFileMetaData
import org.apache.parquet.format.Util.readPageHeader
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream

class UtilTest : StringSpec({
    "readFileMetaData" {
        val md = FileMetaData(
            version = 1,
            schema = listOf(SchemaElement(name = "foo")),
            numRows = 10,
            rowGroups = listOf(
                RowGroup(
                    columns = listOf(ColumnChunk(fileOffset = 1), ColumnChunk(fileOffset = 1)),
                    totalByteSize = 10,
                    numRows = 5,
                ),
                RowGroup(
                    columns = listOf(ColumnChunk(fileOffset = 2), ColumnChunk(fileOffset = 3)),
                    totalByteSize = 11,
                    numRows = 5,
                ),
            )
        )
        val buffer = ByteArrayOutputStream()
        md.write(buffer)
        val md2 = readFileMetaData(buffer.inputStream())

        md2 shouldBe md
    }

    "readFileMetaData (skipRowGroup=true)" {
        val md = FileMetaData(
            version = 1,
            schema = listOf(SchemaElement(name = "foo")),
            numRows = 10,
            rowGroups = listOf(
                RowGroup(
                    columns = listOf(ColumnChunk(fileOffset = 1), ColumnChunk(fileOffset = 1)),
                    totalByteSize = 10,
                    numRows = 5,
                ),
                RowGroup(
                    columns = listOf(ColumnChunk(fileOffset = 2), ColumnChunk(fileOffset = 3)),
                    totalByteSize = 11,
                    numRows = 5,
                ),
            )
        )
        val buffer = ByteArrayOutputStream()
        md.write(buffer)
        val md2 = readFileMetaData(buffer.inputStream(), skipRowGroups = true)

        md2 shouldBe md.copy(rowGroups = emptyList())
    }

    "invalidPageHeader" {
        val ph = PageHeader(
            type = PageType.DATA_PAGE,
            uncompressedPageSize = 100,
            compressedPageSize = -50,
        )
        val buffer = ByteArrayOutputStream()
        ph.write(buffer)

        shouldThrow<InvalidParquetMetadataException> {
            readPageHeader(buffer.inputStream())
        }.message.shouldContain("Compressed page size")
    }
})

private fun ByteArrayOutputStream.inputStream(): InputStream {
    return ByteArrayInputStream(toByteArray())
}
