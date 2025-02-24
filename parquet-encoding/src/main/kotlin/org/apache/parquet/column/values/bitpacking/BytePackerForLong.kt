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

import java.nio.ByteBuffer

/**
 * Packs and unpacks INT64 into bytes
 * <p>
 * packing unpacking treats: - n values at a time (with n % 8 == 0) - bitWidth * (n/8) bytes at a
 * time.
 */
abstract class BytePackerForLong(val bitWidth: Int) {
    /**
     * pack 8 values from input at inputPos into bitWidth bytes in output at outputPos. nextPosition: inputPos
     * += 8; outputPos += getBitWidth()
     *
     * @param input  the input values
     * @param inputPos  where to read from in input
     * @param output the output bytes
     * @param outputPos where to write to in output
     */
    abstract fun pack8Values(input: LongArray, inputPos: Int, output: ByteArray, outputPos: Int)

    /**
     * pack 32 values from input at inputPos into bitWidth * 4 bytes in output at outputPos. nextPosition:
     * inputPos += 32; outputPos += getBitWidth() * 4
     *
     * @param input  the input values
     * @param inputPos  where to read from in input
     * @param output the output bytes
     * @param outputPos where to write to in output
     */
    abstract fun pack32Values(input: LongArray, inputPos: Int, output: ByteArray, outputPos: Int)

    /**
     * unpack bitWidth bytes from input at inputPos into 8 values in output at outputPos. nextPosition:
     * inputPos += getBitWidth(); outputPos += 8
     *
     * @param input  the input bytes
     * @param inputPos  where to read from in input
     * @param output the output values
     * @param outputPos where to write to in output
     */
    abstract fun unpack8Values(input: ByteArray, inputPos: Int, output: LongArray, outputPos: Int)

    /**
     * unpack bitWidth * 4 bytes from input at inputPos into 32 values in output at outputPos. nextPosition:
     * inputPos += getBitWidth() * 4; outputPos += 32
     *
     * @param input  the input bytes
     * @param inputPos  where to read from in input
     * @param output the output values
     * @param outputPos where to write to in output
     */
    abstract fun unpack32Values(input: ByteArray, inputPos: Int, output: LongArray, outputPos: Int)

    /**
     * unpack bitWidth bytes from input at inputPos into 8 values in output at outputPos. nextPosition:
     * inputPos += getBitWidth(); outputPos += 8
     *
     * @param input  the input bytes
     * @param inputPos  where to read from in input
     * @param output the output values
     * @param outputPos where to write to in output
     */
    abstract fun unpack8Values(input: ByteBuffer, inputPos: Int, output: LongArray, outputPos: Int)

    /**
     * unpack bitWidth * 4 bytes from input at inputPos into 32 values in output at outputPos. nextPosition:
     * inputPos += getBitWidth() * 4; outputPos += 32
     *
     * @param input  the input bytes
     * @param inputPos  where to read from in input
     * @param output the output values
     * @param outputPos where to write to in output
     */
    abstract fun unpack32Values(input: ByteBuffer, inputPos: Int, output: LongArray, outputPos: Int)
}
