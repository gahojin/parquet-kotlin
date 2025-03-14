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

import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.filter.UnboundRecordFilter
import org.apache.parquet.io.api.RecordMaterializer

/**
 * Extends the
 *
 * @param root          the root of the schema
 * @param validating
 * @param columnStore
 * @param unboundFilter Filter records, pass in NULL_FILTER to leave unfiltered.
 */
internal class FilteredRecordReader<T>(
    root: MessageColumnIO,
    recordMaterializer: RecordMaterializer<T>,
    validating: Boolean,
    columnStore: ColumnReadStoreImpl,
    unboundFilter: UnboundRecordFilter?,
    private val recordCount: Long,
) : RecordReaderImplementation<T>(root, recordMaterializer, validating, columnStore) {
    private val recordFilter = unboundFilter?.bind(columnReaders)
    private var recordsRead: Long = 0

    /**
     * Override read() method to provide skip.
     */
    override fun read(): T? {
        skipToMatch()
        if (recordsRead == recordCount) {
            return null
        }
        ++recordsRead
        return super.read()
    }

    // FilteredRecordReader skips forwards itself, it never asks the layer above to do the skipping for it.
    // This is different from how filtering is handled in the filter2 API
    override fun shouldSkipCurrentRecord(): Boolean {
        return false
    }

    /**
     * Skips forwards until the filter finds the first match. Returns false
     * if none found.
     */
    private fun skipToMatch() {
        while (recordsRead < recordCount && !recordFilter!!.isMatch) {
            var currentState = getState(0)
            do {
                val columnReader = currentState.column

                // currentLevel = depth + 1 at this point
                // set the current value
                if (columnReader.currentDefinitionLevel >= currentState.maxDefinitionLevel) {
                    columnReader.skip()
                }
                columnReader.consume()

                // Based on repetition level work out next state to go to
                val nextR = if (currentState.maxRepetitionLevel == 0) 0 else columnReader.currentRepetitionLevel
                currentState = currentState.getNextState(nextR) ?: break
            } while (true)

            ++recordsRead
        }
    }
}
