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
package org.apache.parquet.column.values.bitpacking

/**
 * Packs and unpacks into ints
 * <p>
 * packing unpacking treats:
 * - 32 values at a time
 * - bitWidth ints at a time.
 */
abstract class IntPacker(val bitWidth: Int) {
    /**
     * pack 32 values from input at inputPos into bitWidth ints in output at outputPos.
     * nextPosition: inputPos += 32; outputPos += getBitWidth()
     *
     * @param input  the input values
     * @param inputPos  where to read from in input
     * @param output the output ints
     * @param outputPos where to write to in output
     */
    abstract fun pack32Values(input: IntArray, inputPos: Int, output: IntArray, outputPos: Int)

    /**
     * unpack bitWidth ints from input at inputPos into 32 values in output at outputPos.
     * nextPosition: inputPos += getBitWidth(); outputPos += 32
     *
     * @param input  the input int
     * @param inputPos  where to read from in input
     * @param output the output values
     * @param outputPos where to write to in output
     */
    abstract fun unpack32Values(input: IntArray, inputPos: Int, output: IntArray, outputPos: Int)
}
