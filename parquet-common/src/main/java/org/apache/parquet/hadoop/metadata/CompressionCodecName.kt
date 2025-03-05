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
package org.apache.parquet.hadoop.metadata

import org.apache.parquet.format.CompressionCodec
import org.apache.parquet.hadoop.codec.CompressionCodecNotSupportedException

enum class CompressionCodecName(
    val hadoopCompressionCodecClassName: String?,
    val parquetCompressionCodec: CompressionCodec,
    @JvmField val extension: String,
) {
    UNCOMPRESSED(null, CompressionCodec.UNCOMPRESSED, ""),
    SNAPPY("org.apache.parquet.hadoop.codec.SnappyCodec", CompressionCodec.SNAPPY, ".snappy"),
    GZIP("org.apache.hadoop.io.compress.GzipCodec", CompressionCodec.GZIP, ".gz"),
    LZO("com.hadoop.compression.lzo.LzoCodec", CompressionCodec.LZO, ".lzo"),
    BROTLI("org.apache.hadoop.io.compress.BrotliCodec", CompressionCodec.BROTLI, ".br"),
    LZ4("org.apache.hadoop.io.compress.Lz4Codec", CompressionCodec.LZ4, ".lz4hadoop"),
    ZSTD("org.apache.parquet.hadoop.codec.ZstandardCodec", CompressionCodec.ZSTD, ".zstd"),
    LZ4_RAW("org.apache.parquet.hadoop.codec.Lz4RawCodec", CompressionCodec.LZ4_RAW, ".lz4raw");

    val hadoopCompressionCodecClass: Class<*>?
        get() = hadoopCompressionCodecClassName?.let {
            try {
                Class.forName(it)
            } catch (_: ClassNotFoundException) {
                null
            }
        }

    companion object {
        @JvmStatic
        fun fromConf(name: String?): CompressionCodecName {
            return name?.let { valueOf(it.uppercase()) } ?: UNCOMPRESSED
        }

        @JvmStatic
        fun fromCompressionCodec(clazz: Class<*>?): CompressionCodecName {
            return clazz?.name?.let { name ->
                entries.firstOrNull { it.hadoopCompressionCodecClassName == name }
                    ?: throw CompressionCodecNotSupportedException(clazz)
            } ?: UNCOMPRESSED
        }

        @JvmStatic
        fun fromParquet(codec: CompressionCodec): CompressionCodecName {
            return checkNotNull(entries.firstOrNull {
                it.parquetCompressionCodec == codec
            }) {
                "Unknown compression codec $codec"
            }
        }
    }
}
