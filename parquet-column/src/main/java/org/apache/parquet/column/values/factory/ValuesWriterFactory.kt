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
package org.apache.parquet.column.values.factory

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.ParquetProperties
import org.apache.parquet.column.values.ValuesWriter

/**
 * Can be overridden to allow users to manually test different strategies to create ValuesWriters.
 * To do this, the ValuesWriterFactory to be used must be passed to the [ParquetProperties.Builder].
 *
 * Lifecycle of ValuesWriterFactories is:
 *
 *  *  Initialized while creating a [ParquetProperties] using the Builder
 *  *  If the factory must read Hadoop config, it needs to implement the Configurable interface.
 * In addition to that, ParquetOutputFormat needs to be updated to pass in the Hadoop config via the setConf()
 * method on the Configurable interface.
 *  *  newValuesWriter is called once per column for every block of data.
 */
interface ValuesWriterFactory {
    /**
     * Used to initialize the factory. This method is called before newValuesWriter()
     *
     * @param parquetProperties a write configuration
     */
    fun initialize(parquetProperties: ParquetProperties)

    /**
     * Creates a ValuesWriter to write values for the given column.
     *
     * @param descriptor a column descriptor
     * @return a new values writer for values in the descriptor's column
     */
    fun newValuesWriter(descriptor: ColumnDescriptor): ValuesWriter
}
