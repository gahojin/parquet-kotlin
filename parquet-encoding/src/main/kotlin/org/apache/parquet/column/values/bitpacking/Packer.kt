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
 * Factory for packing implementations.
 */
enum class Packer {
    /**
     * packers who fill the Least Significant Bit First
     * int and byte packer have the same result on Big Endian architectures
     */
    BIG_ENDIAN {
        override fun newIntPacker(width: Int): IntPacker {
            return beIntPackerFactory.newIntPacker(width)
        }

        override fun newBytePacker(width: Int): BytePacker {
            return beBytePackerFactory.newBytePacker(width)
        }

        override fun newBytePackerForLong(width: Int): BytePackerForLong {
            return beBytePackerForLongFactory.newBytePackerForLong(width)
        }
    },

    /**
     * packers who fill the Most Significant Bit first
     * int and byte packer have the same result on Little Endian architectures
     */
    LITTLE_ENDIAN {
        override fun newIntPacker(width: Int): IntPacker {
            return leIntPackerFactory.newIntPacker(width)
        }

        override fun newBytePacker(width: Int): BytePacker {
            return leBytePackerFactory.newBytePacker(width)
        }

        override fun newBytePackerForLong(width: Int): BytePackerForLong {
            return leBytePackerForLongFactory.newBytePackerForLong(width)
        }
    };

    /**
     * @param width the width in bits of the packed values
     * @return an int based packer
     */
    abstract fun newIntPacker(width: Int): IntPacker

    /**
     * @param width the width in bits of the packed values
     * @return a byte based packer
     */
    abstract fun newBytePacker(width: Int): BytePacker

    /**
     * @param width the width in bits of the packed values
     * @return a byte based packer for INT64
     */
    abstract fun newBytePackerForLong(width: Int): BytePackerForLong

    companion object {
        internal val beIntPackerFactory: IntPackerFactory = LemireBitPackingBE.factory
        internal val leIntPackerFactory: IntPackerFactory = LemireBitPackingLE.factory
        internal val beBytePackerFactory: BytePackerFactory = ByteBitPackingBE.factory
        internal val leBytePackerFactory: BytePackerFactory = ByteBitPackingLE.factory
        internal val beBytePackerForLongFactory: BytePackerForLongFactory = ByteBitPackingForLongBE.factory
        internal val leBytePackerForLongFactory: BytePackerForLongFactory = ByteBitPackingForLongLE.factory
    }
}
