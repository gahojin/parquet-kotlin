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
package org.apache.parquet.io.api

import org.apache.parquet.io.ParquetDecodingException

/**
 * Top-level class which should be implemented in order to materialize objects from
 * a stream of Parquet data.
 *
 * Each record will be wrapped by [GroupConverter.start] and [GroupConverter.end],
 * between which the appropriate fields will be materialized.
 *
 * @param <T> the materialized object class
 */
abstract class RecordMaterializer<T> {
    /**
     * @return the result of the conversion
     * @throws RecordMaterializationException to signal that a record cannot be materialized, but can be skipped
     */
    abstract val currentRecord: T?

    /**
     * @return the root converter for this tree
     */
    abstract val rootConverter: GroupConverter?

    /**
     * Called if [.getCurrentRecord] isn't going to be called.
     */
    open fun skipCurrentRecord() {}

    /**
     * This exception signals that the current record is cannot be converted from parquet columns to a materialized
     * record, but can be skipped if requested. This exception should be used to signal errors like a union with no
     * set values, or an error in converting parquet primitive values to a materialized record. It should not
     * be used to signal unrecoverable errors, like a data column being corrupt or unreadable.
     */
    class RecordMaterializationException : ParquetDecodingException {
        constructor() : super()

        constructor(message: String?, cause: Throwable?) : super(message, cause)

        constructor(message: String?) : super(message)

        constructor(cause: Throwable?) : super(cause)
    }
}
