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

import org.apache.parquet.io.api.RecordMaterializer

/**
 * used to read empty schema
 *
 * @param <T> the type of the materialized record
 */
internal class EmptyRecordReader<T>(
    private val recordMaterializer: RecordMaterializer<T>,
) : RecordReader<T>() {
    // TODO: validator(wrap(recordMaterializer), validating, root.getType());
    private val recordConsumer = recordMaterializer.rootConverter

    override fun read(): T? {
        recordConsumer.start()
        recordConsumer.end()
        return recordMaterializer.currentRecord
    }
}
