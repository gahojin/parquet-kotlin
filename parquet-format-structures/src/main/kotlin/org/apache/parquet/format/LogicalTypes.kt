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
package org.apache.parquet.format

/**
 * Convenience instances of logical type classes.
 */
object LogicalTypes {
    object TImeUnits {
        @JvmField val MILLIS = TimeUnit.MILLIS(MilliSeconds())
        @JvmField val MICROS = TimeUnit.MICROS(MicroSeconds())
    }

    @JvmField val UTF8 = LogicalType.STRING(StringType())
    @JvmField val MAP = LogicalType.MAP(MapType())
    @JvmField val LIST = LogicalType.LIST(ListType())
    @JvmField val ENUM = LogicalType.ENUM(EnumType())
    @JvmField val DATE = LogicalType.DATE(DateType())
    @JvmField val TIME_MILLIS = LogicalType.TIME(TimeType(true, TImeUnits.MILLIS))
    @JvmField val TIME_MICROS = LogicalType.TIME(TimeType(true, TImeUnits.MICROS))
    @JvmField val TIMESTAMP_MILLIS = LogicalType.TIMESTAMP(TimestampType(true, TImeUnits.MILLIS))
    @JvmField val TIMESTAMP_MICROS = LogicalType.TIMESTAMP(TimestampType(true, TImeUnits.MICROS))
    @JvmField val INT_8 = LogicalType.INTEGER(IntType(8.toByte(), true))
    @JvmField val INT_16 = LogicalType.INTEGER(IntType(16.toByte(), true))
    @JvmField val INT_32 = LogicalType.INTEGER(IntType(32.toByte(), true))
    @JvmField val INT_64 = LogicalType.INTEGER(IntType(64.toByte(), true))
    @JvmField val UINT_8 = LogicalType.INTEGER(IntType(8.toByte(), false))
    @JvmField val UINT_16 = LogicalType.INTEGER(IntType(16.toByte(), false))
    @JvmField val UINT_32 = LogicalType.INTEGER(IntType(32.toByte(), false))
    @JvmField val UINT_64 = LogicalType.INTEGER(IntType(64.toByte(), false))
    @JvmField val UNKNOWN = LogicalType.UNKNOWN(NullType())
    @JvmField val JSON = LogicalType.JSON(JsonType())
    @JvmField val BSON = LogicalType.BSON(BsonType())
    @JvmField val FLOAT16 = LogicalType.FLOAT16(Float16Type())
    @JvmField val UUID = LogicalType.UUID(UUIDType())


    @Suppress("FunctionName")
    @JvmStatic fun DECIMAL(scale: Int, precision: Int): LogicalType {
        return LogicalType.DECIMAL(DecimalType(scale, precision))
    }

    @Suppress("FunctionName")
    @JvmStatic fun VARIANT(specificationVersion: Byte): LogicalType {
        return LogicalType.VARIANT(VariantType(specificationVersion = specificationVersion))
    }
}
