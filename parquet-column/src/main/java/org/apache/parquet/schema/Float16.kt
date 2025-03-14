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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.schema

import org.apache.parquet.io.api.Binary
import java.lang.Float
import kotlin.Boolean
import kotlin.Int
import kotlin.Short
import kotlin.String
import kotlin.toString

/**
 * The class is a utility class to manipulate half-precision 16-bit
 * [IEEE 754](https://en.wikipedia.org/wiki/Half-precision_floating-point_format)
 * floating point data types (also called fp16 or binary16). A half-precision float can be
 * created from or converted to single-precision floats, and is stored in a short data type.
 * The IEEE 754 standard specifies an float16 as having the following format:
 *
 *  * Sign bit: 1 bit
 *  * Exponent width: 5 bits
 *  * Significand: 10 bits
 *
 * The format is laid out as follows:
 * ```
 * 1   11111   1111111111
 * ^   --^--   -----^----
 * sign  |          |_______ significand
 * |
 * -- exponent
 * ```
 *
 * Half-precision floating points can be useful to save memory and/or
 * bandwidth at the expense of range and precision when compared to single-precision
 * floating points (float32).
 *
 * Ref: https://android.googlesource.com/platform/libcore/+/master/luni/src/main/java/libcore/util/FP16.java
 */
object Float16 {
    // Positive infinity of type half-precision float.
    private const val POSITIVE_INFINITY = 0x7c00.toShort()

    // A Not-a-Number representation of a half-precision float.
    private const val NaN = 0x7e00.toShort()

    // The bitmask to and a number with to obtain the sign bit.
    private const val SIGN_MASK = 0x8000

    // The offset to shift by to obtain the exponent bits.
    private const val EXPONENT_SHIFT = 10

    // The bitmask to and a number shifted by EXPONENT_SHIFT right, to obtain exponent bits.
    private const val SHIFTED_EXPONENT_MASK = 0x1f

    // The bitmask to and a number with to obtain significand bits.
    private const val SIGNIFICAND_MASK = 0x3ff

    // The offset of the exponent from the actual value.
    private const val EXPONENT_BIAS = 15

    // The offset to shift by to obtain the sign bit.
    private const val SIGN_SHIFT = 15

    // The bitmask to AND with to obtain exponent and significand bits.
    private const val EXPONENT_SIGNIFICAND_MASK = 0x7fff

    private const val FP32_SIGN_SHIFT = 31
    private const val FP32_EXPONENT_SHIFT = 23
    private const val FP32_SHIFTED_EXPONENT_MASK = 0xff
    private const val FP32_SIGNIFICAND_MASK = 0x7fffff
    private const val FP32_EXPONENT_BIAS = 127
    private const val FP32_QNAN_MASK = 0x400000
    private const val FP32_DENORMAL_MAGIC = 126 shl 23
    private val FP32_DENORMAL_FLOAT = Float.intBitsToFloat(FP32_DENORMAL_MAGIC)

    /**
     * Returns true if the specified half-precision float value represents
     * a Not-a-Number, false otherwise.
     *
     * @param h A half-precision float value
     * @return True if the value is a NaN, false otherwise
     */
    @JvmStatic
    fun isNaN(h: Short): Boolean {
        return (h.toInt() and EXPONENT_SIGNIFICAND_MASK) > POSITIVE_INFINITY
    }

    /**
     * Compares the two specified half-precision float values. The following
     * conditions apply during the comparison:
     *
     *  * NaN is considered by this method to be equal to itself and greater
     * than all other half-precision float values (including `#POSITIVE_INFINITY`)
     *  * POSITIVE_ZERO is considered by this method to be greater than NEGATIVE_ZERO.
     *
     * @param x The first half-precision float value to compare.
     * @param y The second half-precision float value to compare
     *
     * @return  The value `0` if `x` is numerically equal to `y`, a
     * value less than `0` if `x` is numerically less than `y`,
     * and a value greater than `0` if `x` is numerically greater
     * than `y`
     */
    @JvmStatic
    fun compare(x: Short, y: Short): Int {
        val xIsNaN = isNaN(x)
        val yIsNaN = isNaN(y)

        if (!xIsNaN && !yIsNaN) {
            val first = (if ((x.toInt() and SIGN_MASK) != 0) 0x8000 - (x.toInt() and 0xffff) else x.toInt() and 0xffff)
            val second = (if ((y.toInt() and SIGN_MASK) != 0) 0x8000 - (y.toInt() and 0xffff) else y.toInt() and 0xffff)
            // Returns true if the first half-precision float value is less
            // (smaller toward negative infinity) than the second half-precision float value.
            if (first < second) {
                return -1
            }

            // Returns true if the first half-precision float value is greater
            // (larger toward positive infinity) than the second half-precision float value.
            if (first > second) {
                return 1
            }
        }

        // Collapse NaNs, akin to halfToIntBits(), but we want to keep
        // (signed) short value types to preserve the ordering of -0.0
        // and +0.0
        val xBits = if (xIsNaN) NaN else x
        val yBits = if (yIsNaN) NaN else y
        return (if (xBits == yBits) 0 else (if (xBits < yBits) -1 else 1))
    }

    /**
     * Converts the specified half-precision float value in Binary little endian into a
     * single-precision float value. The following special cases are handled:
     * If the input is NaN, the returned value is Float NaN.
     * If the input is POSITIVE_INFINITY or NEGATIVE_INFINITY, the returned value is respectively
     * Float POSITIVE_INFINITY or Float NEGATIVE_INFINITY.
     * If the input is 0 (positive or negative), the returned value is +/-0.0f.
     * Otherwise, the returned value is a normalized single-precision float value.
     *
     * @param b The half-precision float value in Binary little endian to convert to single-precision
     * @return A normalized single-precision float value
     */
    @JvmStatic
    fun toFloat(b: Binary): kotlin.Float {
        val h = b.get2BytesLittleEndian()
        val bits = h.toInt() and 0xffff
        val s = bits and SIGN_MASK
        val e = (bits ushr EXPONENT_SHIFT) and SHIFTED_EXPONENT_MASK
        val m = (bits) and SIGNIFICAND_MASK
        var outE = 0
        var outM = 0
        if (e == 0) { // Denormal or 0
            if (m != 0) {
                // Convert denorm fp16 into normalized fp32
                var o = Float.intBitsToFloat(FP32_DENORMAL_MAGIC + m)
                o -= FP32_DENORMAL_FLOAT
                return if (s == 0) o else -o
            }
        } else {
            outM = m shl 13
            if (e == 0x1f) { // Infinite or NaN
                outE = 0xff
                if (outM != 0) { // SNaNs are quieted
                    outM = outM or FP32_QNAN_MASK
                }
            } else {
                outE = e - EXPONENT_BIAS + FP32_EXPONENT_BIAS
            }
        }
        val out = (s shl 16) or (outE shl FP32_EXPONENT_SHIFT) or outM
        return Float.intBitsToFloat(out)
    }

    /**
     * Converts the specified single-precision float value into a
     * half-precision float value. The following special cases are handled:
     *
     * If the input is NaN, the returned value is NaN.
     * If the input is Float POSITIVE_INFINITY or Float NEGATIVE_INFINITY,
     * the returned value is respectively POSITIVE_INFINITY or NEGATIVE_INFINITY.
     * If the input is 0 (positive or negative), the returned value is
     * POSITIVE_ZERO or NEGATIVE_ZERO.
     * If the input is a less than MIN_VALUE, the returned value
     * is flushed to POSITIVE_ZERO or NEGATIVE_ZERO.
     * If the input is a less than MIN_NORMAL, the returned value
     * is a denorm half-precision float.
     * Otherwise, the returned value is rounded to the nearest
     * representable half-precision float value.
     *
     * @param f The single-precision float value to convert to half-precision
     * @return A half-precision float value
     */
    @JvmStatic
    fun toFloat16(f: kotlin.Float): Short {
        val bits = Float.floatToRawIntBits(f)
        val s = (bits ushr FP32_SIGN_SHIFT)
        var e = (bits ushr FP32_EXPONENT_SHIFT) and FP32_SHIFTED_EXPONENT_MASK
        var m = (bits) and FP32_SIGNIFICAND_MASK
        var outE = 0
        var outM = 0
        if (e == 0xff) { // Infinite or NaN
            outE = 0x1f
            outM = if (m != 0) 0x200 else 0
        } else {
            e = e - FP32_EXPONENT_BIAS + EXPONENT_BIAS
            if (e >= 0x1f) { // Overflow
                outE = 0x1f
            } else if (e <= 0) { // Underflow
                if (e < -10) {
                    // The absolute fp32 value is less than MIN_VALUE, flush to +/-0
                } else {
                    // The fp32 value is a normalized float less than MIN_NORMAL,
                    // we convert to a denorm fp16
                    m = m or 0x800000
                    val shift = 14 - e
                    outM = m shr shift
                    val lowm = m and ((1 shl shift) - 1)
                    val hway = 1 shl (shift - 1)
                    // if above halfway or exactly halfway and outM is odd
                    if (lowm + (outM and 1) > hway) {
                        // Round to nearest even
                        // Can overflow into exponent bit, which surprisingly is OK.
                        // This increment relies on the +outM in the return statement below
                        outM++
                    }
                }
            } else {
                outE = e
                outM = m shr 13
                // if above halfway or exactly halfway and outM is odd
                if ((m and 0x1fff) + (outM and 0x1) > 0x1000) {
                    // Round to nearest even
                    // Can overflow into exponent bit, which surprisingly is OK.
                    // This increment relies on the +outM in the return statement below
                    outM++
                }
            }
        }
        // The outM is added here as the +1 increments for outM above can
        // cause an overflow in the exponent bit which is OK.
        return ((s shl SIGN_SHIFT) or (outE shl EXPONENT_SHIFT) + outM).toShort()
    }

    /**
     * Returns a string representation of the specified half-precision
     * float value. Calling this method is equivalent to calling
     * `Float.toString(toFloat(h))`. See [Float.toString]
     * for more information on the format of the string representation.
     *
     * @param h A half-precision float value in binary little-endian format
     * @return A string representation of the specified value
     */
    @JvmStatic
    fun toFloatString(h: Binary): String {
        return toFloat(h).toString()
    }
}
