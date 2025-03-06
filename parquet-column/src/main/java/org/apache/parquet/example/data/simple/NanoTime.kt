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
package org.apache.parquet.example.data.simple

import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import java.nio.ByteBuffer
import java.nio.ByteOrder

class NanoTime(val julianDay: Int, val timeOfDayNanos: Long) : Primitive() {
    fun toBinary(): Binary {
        val buf = ByteBuffer.allocate(12)
        buf.order(ByteOrder.LITTLE_ENDIAN)
        buf.putLong(timeOfDayNanos)
        buf.putInt(julianDay)
        buf.flip()
        return Binary.fromConstantByteBuffer(buf)
    }

    fun toInt96(): Int96Value {
        return Int96Value(toBinary())
    }

    override fun writeValue(recordConsumer: RecordConsumer) {
        recordConsumer.addBinary(toBinary())
    }

    override fun toString(): String {
        return "NanoTime{julianDay=$julianDay, timeOfDayNanos=$timeOfDayNanos}"
    }

    companion object {
        @JvmStatic
        fun fromBinary(bytes: Binary): NanoTime {
            require(bytes.length() == 12) { "Must be 12 bytes" }
            val buf = bytes.toByteBuffer()
            buf.order(ByteOrder.LITTLE_ENDIAN)
            val timeOfDayNanos = buf.getLong()
            val julianDay = buf.getInt()
            return NanoTime(julianDay, timeOfDayNanos)
        }

        @JvmStatic
        fun fromInt96(int96: Int96Value): NanoTime {
            val buf = int96.int96.toByteBuffer()
            return NanoTime(buf.getInt(), buf.getLong())
        }
    }
}
