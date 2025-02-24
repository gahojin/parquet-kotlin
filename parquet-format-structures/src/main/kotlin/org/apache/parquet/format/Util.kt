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

import jp.co.gahojin.thrifty.Struct
import jp.co.gahojin.thrifty.TType
import jp.co.gahojin.thrifty.compactProtocol
import jp.co.gahojin.thrifty.kotlin.Adapter
import jp.co.gahojin.thrifty.protocol.Protocol
import jp.co.gahojin.thrifty.transport
import okio.Buffer
import okio.ByteString
import okio.IOException
import okio.buffer
import okio.sink
import okio.source
import java.io.InputStream
import java.io.OutputStream

/**
 * Utility to read/write metadata
 * We use the TCompactProtocol to serialize metadata
 */
object Util {
    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun writeColumnIndex(
        columnIndex: ColumnIndex,
        to: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        write(columnIndex, to, encryptor, AAD)
    }

    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun writeOffsetIndex(
        offsetIndex: OffsetIndex,
        to: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        write(offsetIndex, to, encryptor, AAD)
    }

    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun writeBloomFilterHeader(
        header: BloomFilterHeader,
        to: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        write(header, to, encryptor, AAD)
    }

    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun writePageHeader(
        header: PageHeader,
        to: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        write(header, to, encryptor, AAD)
    }

    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun writeFileMetaData(
        fileMetaData: FileMetaData,
        to: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        write(fileMetaData, to, encryptor, AAD)
    }

    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun writeColumnMetaData(
        columnMetaData: ColumnMetaData,
        to: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        write(columnMetaData, to, encryptor, AAD)
    }

    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun writeFileCryptoMetaData(
        cryptoMetaData: FileCryptoMetaData,
        to: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        write(cryptoMetaData, to, encryptor, AAD)
    }

    @Throws(IOException::class)
    @JvmStatic
    @JvmOverloads
    fun <T : Struct> write(
        base: T,
        output: OutputStream,
        encryptor: BlockCipher.Encryptor? = null,
        AAD: ByteArray? = null,
    ) {
        encryptor?.also {
            val buffer = Buffer()
            val transport = buffer.transport()
            base.write(transport.compactProtocol())
            transport.flush()

            output.write(it.encrypt(buffer.readByteArray(), checkNotNull(AAD)))
        } ?: run {
            val buffer = output.sink().buffer()
            val transport = buffer.transport()
            base.write(transport.compactProtocol())
            transport.flush()
        }
    }

    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readColumnIndex(
        from: InputStream,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): ColumnIndex {
        return read(from, ColumnIndex.ADAPTER, descriptor, AAD)
    }

    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readOffsetIndex(
        from: InputStream,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): OffsetIndex {
        return read(from, OffsetIndex.ADAPTER, descriptor, AAD)
    }

    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readBloomFilterHeader(
        from: InputStream,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): BloomFilterHeader {
        return read(from, BloomFilterHeader.ADAPTER, descriptor, AAD)
    }

    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readPageHeader(
        from: InputStream,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): PageHeader {
        return read(from, PageHeader.ADAPTER, descriptor, AAD).validate()
    }

    /**
     * reads the meta data from the stream
     *
     * @param from the stream to read the metadata from
     * @return the resulting metadata
     * @throws IOException if any I/O error occurs during the reading
     */
    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readFileMetaData(
        from: InputStream,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): FileMetaData {
        return read(from, FileMetaData.ADAPTER, descriptor, AAD)
    }

    /**
     * reads the meta data from the stream
     *
     * @param from the stream to read the metadata from
     * @param skipRowGroups whether row groups should be skipped
     * @return the resulting metadata
     * @throws IOException if any I/O error occurs during the reading
     */
    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readFileMetaData(
        from: InputStream,
        skipRowGroups: Boolean,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): FileMetaData {
        if (skipRowGroups) {
            return read(from, SkipFileMetaDataAdapter, descriptor, AAD)
        }
        return read(from, FileMetaData.ADAPTER, descriptor, AAD)
    }

    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readFileCryptoMetaData(
        from: InputStream,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): FileCryptoMetaData {
        return read(from, FileCryptoMetaData.ADAPTER, descriptor, AAD)
    }

    @JvmStatic
    @Throws(IOException::class)
    @JvmOverloads
    fun readColumnMetaData(
        from: InputStream,
        descriptor: BlockCipher.Decryptor? = null,
        AAD: ByteArray? = null,
    ): ColumnMetaData {
        return read(from, ColumnMetaData.ADAPTER, descriptor, AAD)
    }

    private fun <T : Struct> read(
        input: InputStream,
        adapter: Adapter<T>,
        descriptor: BlockCipher.Decryptor?,
        AAD: ByteArray?,
    ): T {
        val plainText = descriptor?.decrypt(input, checkNotNull(AAD))?.inputStream()
            ?: input

        val transport = plainText.transport()
        return adapter.read(InterningProtocol(transport.compactProtocol()))
    }
}

@Suppress("NOTHING_TO_INLINE")
inline fun <T : Struct> T.write(
    output: OutputStream,
    encryptor: BlockCipher.Encryptor? = null,
    AAD: ByteArray? = null,
) = Util.write(this, output, encryptor, AAD)

@Suppress("LocalVariableName")
private object SkipFileMetaDataAdapter : Adapter<FileMetaData> {
    override fun read(protocol: Protocol): FileMetaData {
        var _local_version: Int? = null
        var _local_schema: List<SchemaElement>? = null
        var _local_num_rows: Long? = null
        var _local_key_value_metadata: List<KeyValue>? = null
        var _local_created_by: String? = null
        var _local_column_orders: List<ColumnOrder>? = null
        var _local_encryption_algorithm: EncryptionAlgorithm? = null
        var _local_footer_signing_key_metadata: ByteString? = null
        protocol.readStructBegin()
        while (true) {
            val fieldMeta = protocol.readFieldBegin()
            if (fieldMeta.typeId == TType.STOP) {
                break
            }
            when (fieldMeta.fieldId.toInt()) {
                1 -> if (fieldMeta.typeId == TType.I32) {
                    val version = protocol.readI32()
                    _local_version = version
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                2 -> if (fieldMeta.typeId == TType.LIST) {
                    val list0 = protocol.readListBegin()
                    val schema = ArrayList<SchemaElement>(list0.size)
                    for (i0 in 0..<list0.size) {
                        val item0 = SchemaElement.ADAPTER.read(protocol)
                        schema += item0
                    }
                    protocol.readListEnd()
                    _local_schema = schema
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                3 -> if (fieldMeta.typeId == TType.I64) {
                    val numRows = protocol.readI64()
                    _local_num_rows = numRows
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                4 -> if (fieldMeta.typeId == TType.LIST) {
                    val list0 = protocol.readListBegin()
                    for (i0 in 0..<list0.size) {
                        RowGroup.ADAPTER.read(protocol)
                    }
                    protocol.readListEnd()
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                5 -> if (fieldMeta.typeId == TType.LIST) {
                    val list0 = protocol.readListBegin()
                    val keyValueMetadata = ArrayList<KeyValue>(list0.size)
                    for (i0 in 0..<list0.size) {
                        val item0 = KeyValue.ADAPTER.read(protocol)
                        keyValueMetadata += item0
                    }
                    protocol.readListEnd()
                    _local_key_value_metadata = keyValueMetadata
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                6 -> if (fieldMeta.typeId == TType.STRING) {
                    val createdBy = protocol.readString()
                    _local_created_by = createdBy
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                7 -> if (fieldMeta.typeId == TType.LIST) {
                    val list0 = protocol.readListBegin()
                    val columnOrders = ArrayList<ColumnOrder>(list0.size)
                    for (i0 in 0..<list0.size) {
                        val item0 = ColumnOrder.ADAPTER.read(protocol)
                        columnOrders += item0
                    }
                    protocol.readListEnd()
                    _local_column_orders = columnOrders
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                8 -> if (fieldMeta.typeId == TType.STRUCT) {
                    val encryptionAlgorithm = EncryptionAlgorithm.ADAPTER.read(protocol)
                    _local_encryption_algorithm = encryptionAlgorithm
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                9 -> if (fieldMeta.typeId == TType.STRING) {
                    val footerSigningKeyMetadata = protocol.readBinary()
                    _local_footer_signing_key_metadata = footerSigningKeyMetadata
                } else {
                    protocol.skip(fieldMeta.typeId)
                }
                else -> protocol.skip(fieldMeta.typeId)
            }
            protocol.readFieldEnd()
        }
        protocol.readStructEnd()
        return FileMetaData(
            version = checkNotNull(_local_version) { "Required field 'version' is missing" },
            schema = checkNotNull(_local_schema) { "Required field 'schema' is missing" },
            numRows = checkNotNull(_local_num_rows) { "Required field 'numRows' is missing" },
            rowGroups = emptyList(),
            keyValueMetadata = _local_key_value_metadata,
            createdBy = _local_created_by,
            columnOrders = _local_column_orders,
            encryptionAlgorithm = _local_encryption_algorithm,
            footerSigningKeyMetadata = _local_footer_signing_key_metadata,
        )
    }

    override fun write(protocol: Protocol, struct: FileMetaData) {
        throw UnsupportedOperationException("writing not supported")
    }
}
