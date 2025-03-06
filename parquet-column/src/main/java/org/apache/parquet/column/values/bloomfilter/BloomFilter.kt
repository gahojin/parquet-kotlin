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
package org.apache.parquet.column.values.bloomfilter

import org.apache.parquet.io.api.Binary
import java.io.IOException
import java.io.OutputStream

/**
 * A Bloom filter is a compact structure to indicate whether an item is not in a set or probably
 * in a set. The Bloom filter usually consists of a bit set that represents a elements set,
 * a hash strategy and a Bloom filter algorithm.
 */
interface BloomFilter {
    /* Bloom filter Hash strategy.
     *
     * xxHash is an extremely fast hash algorithm, running at RAM speed limits. It successfully
     * completes the SMHasher test suite which evaluates collision, dispersion and randomness qualities
     * of hash functions. It shows good performance advantage from its benchmark result.
     * (see https://github.com/Cyan4973/xxHash).
     */
    enum class HashStrategy {
        XXH64;

        override fun toString(): String {
            return "xxhash"
        }
    }

    // Bloom filter algorithm.
    enum class Algorithm {
        BLOCK;

        override fun toString(): String {
            return "block"
        }
    }

    // Bloom filter compression.
    enum class Compression {
        UNCOMPRESSED;

        override fun toString(): String {
            return "uncompressed"
        }
    }

    /**
     * Get the number of bytes for bitset in this Bloom filter.
     *
     * @return The number of bytes for bitset in this Bloom filter.
     */
    val bitsetSize: Int

    /**
     * Return the hash strategy that the bloom filter apply.
     *
     * @return hash strategy that the bloom filter apply
     */
    val hashStrategy: HashStrategy

    /**
     * Return the algorithm that the bloom filter apply.
     *
     * @return algorithm that the bloom filter apply
     */
    val algorithm: Algorithm

    /**
     * Return the compress algorithm that the bloom filter apply.
     *
     * @return compress algorithm that the bloom filter apply
     */
    val compression: Compression

    /**
     * Write the Bloom filter to an output stream. It writes the Bloom filter header including the
     * bitset's length in bytes, the hash strategy, the algorithm, and the bitset.
     *
     * @param out the output stream to write
     */
    @Throws(IOException::class)
    fun writeTo(out: OutputStream?)

    /**
     * Insert an element to the Bloom filter, the element content is represented by
     * the hash value of its plain encoding result.
     *
     * @param hash the hash result of element.
     */
    fun insertHash(hash: Long)

    /**
     * Determine whether an element is in set or not.
     *
     * @param hash the hash value of element plain encoding result.
     * @return false if element is must not in set, true if element probably in set.
     */
    fun findHash(hash: Long): Boolean

    /**
     * Compare this Bloom filter to the specified object.
     *
     * @param object
     * @return true if the given object represents a Bloom filter equivalent to this Bloom filter, false otherwise.
     */
    override fun equals(`object`: Any?): Boolean

    /**
     * Compute hash for int value by using its plain encoding result.
     *
     * @param value the value to hash
     * @return hash result
     */
    fun hash(value: Int): Long

    /**
     * Compute hash for long value by using its plain encoding result.
     *
     * @param value the value to hash
     * @return hash result
     */
    fun hash(value: Long): Long

    /**
     * Compute hash for double value by using its plain encoding result.
     *
     * @param value the value to hash
     * @return hash result
     */
    fun hash(value: Double): Long

    /**
     * Compute hash for float value by using its plain encoding result.
     *
     * @param value the value to hash
     * @return hash result
     */
    fun hash(value: Float): Long

    /**
     * Compute hash for Binary value by using its plain encoding result.
     *
     * @param value the value to hash
     * @return hash result
     */
    fun hash(value: Binary): Long

    /**
     * Compute hash for Object value by using its plain encoding result.
     *
     * @param value the value to hash
     * @return hash result
     */
    fun hash(value: Any): Long

    // The boolean type is not supported because boolean type has only two values, while Bloom filter is
    // suitable for high cardinality.
    // long hash(Boolean value);

    /**
     * Determines whether a given Bloom filter can be merged into this Bloom filter. For two Bloom
     * filters to merge, they must:
     *
     *  *  have the same bit size
     *  *  have the same algorithm
     *  *  have the same hash strategy
     *
     * @param otherBloomFilter The Bloom filter to merge this Bloom filter with.
     */
    fun canMergeFrom(otherBloomFilter: BloomFilter): Boolean {
        throw UnsupportedOperationException("Merge API is not implemented.")
    }

    /**
     * Merges this Bloom filter with another Bloom filter by performing a bitwise OR of the underlying bitsets
     *
     * @param otherBloomFilter The Bloom filter to merge this Bloom filter with.
     */
    @Throws(IOException::class)
    fun merge(otherBloomFilter: BloomFilter) {
        throw UnsupportedOperationException("Merge API is not implemented.")
    }
}
