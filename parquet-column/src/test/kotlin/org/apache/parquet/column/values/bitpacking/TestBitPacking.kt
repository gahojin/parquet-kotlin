package org.apache.parquet.column.values.bitpacking

object TestBitPacking {
    @JvmStatic
    fun toString(value: ByteArray): String {
        return value.joinToString(" ") {
            Integer.toBinaryString(it.toInt() and 0xff).padStart(8, '0')
        }
    }

    @JvmStatic
    fun toString(value: IntArray): String {
        return value.joinToString(" ")
    }
}
