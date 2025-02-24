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

import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

interface BlockCipher {
    interface Encryptor {
        /**
         * Encrypts the plaintext.
         *
         * @param plaintext - starts at offset 0 of the input, and fills up the entire byte array.
         * @param AAD       - Additional Authenticated Data for the encryption (ignored in case of CTR cipher)
         * @return lengthAndCiphertext The first 4 bytes of the returned value are the ciphertext length (little endian int).
         * The ciphertext starts at offset 4  and fills up the rest of the returned byte array.
         * The ciphertext includes the nonce and (in case of GCM cipher) the tag, as detailed in the
         * Parquet Modular Encryption specification.
         */
        fun encrypt(plaintext: ByteArray, AAD: ByteArray): ByteArray
    }

    interface Decryptor {
        /**
         * Decrypts the ciphertext.
         *
         * @param lengthAndCiphertext - The first 4 bytes of the input are the ciphertext length (little endian int).
         * The ciphertext starts at offset 4  and fills up the rest of the input byte array.
         * The ciphertext includes the nonce and (in case of GCM cipher) the tag, as detailed in the
         * Parquet Modular Encryption specification.
         * @param AAD                 - Additional Authenticated Data for the decryption (ignored in case of CTR cipher)
         * @return plaintext - starts at offset 0 of the output value, and fills up the entire byte array.
         */
        fun decrypt(lengthAndCiphertext: ByteArray, AAD: ByteArray): ByteArray

        /**
         * Convenience decryption method that reads the length and ciphertext from a ByteBuffer
         *
         * @param from ByteBuffer with length and ciphertext.
         * @param AAD  - Additional Authenticated Data for the decryption (ignored in case of CTR cipher)
         * @return plaintext - starts at offset 0 of the output, and fills up the entire byte buffer.
         */
        fun decrypt(from: ByteBuffer, AAD: ByteArray): ByteBuffer

        /**
         * Convenience decryption method that reads the length and ciphertext from the input stream.
         *
         * @param from Input stream with length and ciphertext.
         * @param AAD  - Additional Authenticated Data for the decryption (ignored in case of CTR cipher)
         * @return plaintext -  starts at offset 0 of the output, and fills up the entire byte array.
         * @throws IOException - Stream I/O problems
         */
        @Throws(IOException::class)
        fun decrypt(from: InputStream, AAD: ByteArray): ByteArray
    }
}
