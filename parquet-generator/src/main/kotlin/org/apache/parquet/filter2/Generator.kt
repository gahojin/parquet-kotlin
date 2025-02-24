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
package org.apache.parquet.filter2

import org.apache.parquet.encoding.bitpacking.ByteBasedBitPackingGenerator
import org.apache.parquet.encoding.bitpacking.IntBasedBitPackingGenerator

/**
 * main class for code generation hook in build for filter2 generation
 */
object Generator {
    @JvmStatic
    fun main(args: Array<String>) {
        IncrementallyUpdatedFilterPredicateGenerator.main(args)
    }
}
