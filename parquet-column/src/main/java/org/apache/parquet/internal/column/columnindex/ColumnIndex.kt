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
package org.apache.parquet.internal.column.columnindex

import org.apache.parquet.filter2.predicate.FilterPredicate
import java.nio.ByteBuffer
import java.util.PrimitiveIterator

/**
 * Column index containing min/max and null count values for the pages in a column chunk. It also implements methods of
 * [Visitor] to return the indexes of the matching pages. They are used by [ColumnIndexFilter].
 *
 * @see org.apache.parquet.format.ColumnIndex
 */
interface ColumnIndex : FilterPredicate.Visitor<PrimitiveIterator.OfInt> {
    /**
     * @return the boundary order of the min/max values; used for converting to the related thrift object
     */
    val boundaryOrder: BoundaryOrder

    /**
     * @return the unmodifiable list of null counts; used for converting to the related thrift object
     */
    val nullCounts: List<Long>?

    /**
     * @return the unmodifiable list of null pages; used for converting to the related thrift object
     */
    val nullPages: List<Boolean>

    /**
     * @return the list of the min values as [ByteBuffer]s; used for converting to the related thrift object
     */
    val minValues: List<ByteBuffer>

    /**
     * @return the list of the max values as [ByteBuffer]s; used for converting to the related thrift object
     */
    val maxValues: List<ByteBuffer>

    /**
     * @return the unmodifiable list of the repetition level histograms for each page concatenated together; used for
     * converting to the related thrift object
     */
    val repetitionLevelHistogram: List<Long>
        get() = throw UnsupportedOperationException("Repetition level histogram is not implemented")

    /**
     * @return the unmodifiable list of the definition level histograms for each page concatenated together; used for
     * converting to the related thrift object
     */
    val definitionLevelHistogram: List<Long>
        get() = throw UnsupportedOperationException("Definition level histogram is not implemented")
}
