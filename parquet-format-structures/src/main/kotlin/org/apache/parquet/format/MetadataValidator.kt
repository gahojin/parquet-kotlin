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

/**
 * Utility class to validate different types of Parquet metadata (e.g. footer, page headers etc.).
 */
object MetadataValidator {
    @JvmStatic
    fun validate(pageHeader: PageHeader): PageHeader {
        val compressedPageSize = pageHeader.compressedPageSize
        validateValue(compressedPageSize >= 0) {
            "Compressed page size must not be negative but was: $compressedPageSize"
        }
        return pageHeader
    }

    @JvmStatic
    fun validateValue(valid: Boolean, block: () -> String) {
        if (!valid) {
            throw InvalidParquetMetadataException(block())
        }
    }
}

fun PageHeader.validate(): PageHeader = MetadataValidator.validate(this)
