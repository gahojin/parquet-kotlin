/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.bloomfilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import net.openhft.hashing.LongHashFunction;
import org.apache.parquet.column.values.RandomStringUtils;
import org.apache.parquet.io.api.Binary;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestBlockSplitBloomFilter {

  @Test
  public void testConstructor() {
    BloomFilter bloomFilter1 = new BlockSplitBloomFilter(0);
    assertEquals(bloomFilter1.getBitsetSize(), BlockSplitBloomFilter.LOWER_BOUND_BYTES);
    BloomFilter bloomFilter3 = new BlockSplitBloomFilter(1000);
    assertEquals(bloomFilter3.getBitsetSize(), 1024);
  }

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  /*
   * This test is used to test basic operations including inserting, finding and
   * serializing and de-serializing.
   */
  @Test
  public void testBloomFilterForString() {
    final int numValues = 1024 * 1024;
    int numBytes = BlockSplitBloomFilter.optimalNumOfBits(numValues, 0.01) / 8;
    BloomFilter bloomFilter = new BlockSplitBloomFilter(numBytes);

    Set<String> testStrings = new HashSet<>();
    for (int i = 0; i < numValues; i++) {
      String str = RandomStringUtils.randomAlphabetic(1, 64);
      bloomFilter.insertHash(bloomFilter.hash(Binary.fromString(str)));
      testStrings.add(str);
    }

    for (String testString : testStrings) {
      assertTrue(bloomFilter.findHash(bloomFilter.hash(Binary.fromString(testString))));
    }
  }

  @Test
  public void testBloomFilterForPrimitives() {
    for (int i = 0; i < 4; i++) {
      long seed = System.nanoTime();
      testBloomFilterForPrimitives(seed);
    }
  }

  private void testBloomFilterForPrimitives(long seed) {
    Random random = new Random(seed);
    final int numValues = 1024 * 1024;
    final int numBytes = BlockSplitBloomFilter.optimalNumOfBits(numValues, random.nextDouble() / 10) / 8;
    BloomFilter bloomFilter = new BlockSplitBloomFilter(numBytes);

    Set<Object> values = new HashSet<>();

    for (int i = 0; i < numValues; i++) {
      int type = random.nextInt(4);
      Object v;
      switch (type) {
        case 0: {
          v = random.nextInt();
          break;
        }
        case 1: {
          v = random.nextLong();
          break;
        }
        case 2: {
          v = random.nextFloat();
          break;
        }
        default: {
          v = random.nextDouble();
        }
      }
      values.add(v);
      bloomFilter.insertHash(bloomFilter.hash(v));
    }

    for (Object v : values) {
      assertTrue(
          String.format("the value %s should not be filtered, seed = %d", v, seed),
          bloomFilter.findHash(bloomFilter.hash(v)));
    }
  }

  @Test
  public void testBloomFilterFPPAccuracy() {
    final int totalCount = 100000;
    final double FPP = 0.01;

    BloomFilter bloomFilter =
        new BlockSplitBloomFilter(BlockSplitBloomFilter.optimalNumOfBits(totalCount, FPP) / 8);

    Set<String> distinctStrings = new HashSet<>();
    while (distinctStrings.size() < totalCount) {
      String str = RandomStringUtils.randomAlphabetic(12);
      if (distinctStrings.add(str)) {
        bloomFilter.insertHash(bloomFilter.hash(Binary.fromString(str)));
      }
    }

    distinctStrings.clear();
    // The exist counts the number of times FindHash returns true.
    int exist = 0;
    while (distinctStrings.size() < totalCount) {
      String str = RandomStringUtils.randomAlphabetic(10);
      if (distinctStrings.add(str) && bloomFilter.findHash(bloomFilter.hash(Binary.fromString(str)))) {
        exist++;
      }
    }

    // The exist should be probably less than 1000 according FPP 0.01. Add 20% here for error space.
    assertTrue(exist < totalCount * (FPP * 1.2));
  }

  @Test
  public void testEquals() {
    final String[] words = {"hello", "parquet", "bloom", "filter"};
    BloomFilter bloomFilterOne = new BlockSplitBloomFilter(1024);
    BloomFilter bloomFilterTwo = new BlockSplitBloomFilter(1024);

    for (String word : words) {
      bloomFilterOne.insertHash(bloomFilterOne.hash(Binary.fromString(word)));
      bloomFilterTwo.insertHash(bloomFilterTwo.hash(Binary.fromString(word)));
    }

    assertEquals(bloomFilterOne, bloomFilterTwo);

    BloomFilter bloomFilterThree = new BlockSplitBloomFilter(1024);
    bloomFilterThree.insertHash(bloomFilterThree.hash(Binary.fromString("parquet")));

    assertNotEquals(bloomFilterTwo, bloomFilterThree);
  }

  @Test
  public void testBloomFilterNDVs() {
    // a row group of 128M with one column of long type.
    int ndv = 128 * 1024 * 1024 / 8;
    double fpp = 0.01;

    // the optimal value formula
    double numBits = -8 * ndv / Math.log(1 - Math.pow(0.01, 1.0 / 8));
    int bytes = (int) numBits / 8;
    assertTrue(bytes < 20 * 1024 * 1024);

    // a row group of 128MB with one column of UUID type
    ndv = 128 * 1024 * 1024 / java.util.UUID.randomUUID().toString().length();
    numBits = -8 * ndv / Math.log(1 - Math.pow(fpp, 1.0 / 8));
    bytes = (int) numBits / 8;
    assertTrue(bytes < 5 * 1024 * 1024);
  }

  @Test
  public void testAdaptiveBloomFilter() {
    int maxBloomFilterSize = 1024 * 1024;
    int candidateNumber = 10;
    AdaptiveBlockSplitBloomFilter adaptiveBloomFilter =
        new AdaptiveBlockSplitBloomFilter(maxBloomFilterSize, candidateNumber, 0.01, null);

    assertEquals(candidateNumber, adaptiveBloomFilter.getCandidates().size());

    Set<String> existedValue = new HashSet<>();
    while (existedValue.size() < 10000) {
      String str = RandomStringUtils.randomAlphabetic(1, 64);
      adaptiveBloomFilter.insertHash(adaptiveBloomFilter.hash(Binary.fromString(str)));
      existedValue.add(str);
    }
    // removed some small bloom filter
    assertEquals(7, adaptiveBloomFilter.getCandidates().size());
    BlockSplitBloomFilter optimalCandidate =
        adaptiveBloomFilter.optimalCandidate().getBloomFilter();
    for (String value : existedValue) {
      assertTrue(optimalCandidate.findHash(optimalCandidate.hash(Binary.fromString(value))));
    }

    int maxCandidateNDV = adaptiveBloomFilter.getCandidates().stream()
        .max(AdaptiveBlockSplitBloomFilter.BloomFilterCandidate::compareTo)
        .get()
        .getExpectedNDV();
    while (existedValue.size() < maxCandidateNDV + 1) {
      String str = RandomStringUtils.randomAlphabetic(1, 64);
      adaptiveBloomFilter.insertHash(adaptiveBloomFilter.hash(Binary.fromString(str)));
      existedValue.add(str);
    }
    // the number of distinct value exceeds the maximum candidate's expected NDV, so only the maximum candidate is
    // kept
    assertEquals(1, adaptiveBloomFilter.getCandidates().size());
  }

  @Test
  public void testMergeBloomFilter() throws IOException {
    int numBytes = BlockSplitBloomFilter.optimalNumOfBits(1024 * 5, 0.01) / 8;
    BloomFilter otherBloomFilter = new BlockSplitBloomFilter(numBytes);
    BloomFilter mergedBloomFilter = new BlockSplitBloomFilter(numBytes);
    for (int i = 0; i < 1024; i++) {
      mergedBloomFilter.insertHash(mergedBloomFilter.hash(i));
    }
    for (int i = 1024; i < 2048; i++) {
      otherBloomFilter.insertHash(otherBloomFilter.hash(i));
      // Before merging BloomFilter, `mergedBloomFilter` doesn't have any value in `otherBloomFilter`
      assertFalse(mergedBloomFilter.findHash(mergedBloomFilter.hash(i)));
    }
    mergedBloomFilter.merge(otherBloomFilter);
    // After merging BloomFilter, `mergedBloomFilter` should have all values in `otherBloomFilter`
    for (int i = 0; i < 2048; i++) {
      assertTrue(mergedBloomFilter.findHash(mergedBloomFilter.hash(i)));
    }
    for (int i = 2048; i < 3096; i++) {
      assertFalse(otherBloomFilter.findHash(otherBloomFilter.hash(i)));
      assertFalse(mergedBloomFilter.findHash(mergedBloomFilter.hash(i)));
    }
  }

  @Test
  public void testMergeBloomFilterFailed() throws IOException {
    int numBytes = BlockSplitBloomFilter.optimalNumOfBits(1024 * 5, 0.01) / 8;
    BloomFilter mergedBloomFilter = new BlockSplitBloomFilter(numBytes);
    BloomFilter otherBloomFilter = new BlockSplitBloomFilter(numBytes * 1024);
    try {
      mergedBloomFilter.merge(otherBloomFilter);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected, BloomFilters should have the same size of bitsets
    }
  }

  /**
   * Test data is output of the following program with xxHash implementation
   * from https://github.com/Cyan4973/xxHash with commit c8c4cc0f812719ce1f5b2c291159658980e7c255
   * <p>
   * #define XXH_INLINE_ALL
   * #include "xxhash.h"
   * #include <stdlib.h>
   * #include <stdio.h>
   * int main()
   * {
   * char* src = (char*) malloc(32);
   * const int N = 32;
   * for (int i = 0; i < N; i++) {
   * src[i] = (char) i;
   * }
   * <p>
   * printf("without seed\n");
   * for (int i = 0; i <= N; i++) {
   * printf("%lldL,\n", (long long) XXH64(src, i, 0));
   * }
   * <p>
   * printf("with seed 42\n");
   * for (int i = 0; i <= N; i++) {
   * printf("%lldL,\n", (long long) XXH64(src, i, 42));
   * }
   * }
   */
  private static final long[] HASHES_OF_LOOPING_BYTES_WITH_SEED_0 = {
    -1205034819632174695L, -1642502924627794072L, 5216751715308240086L, -1889335612763511331L,
    -13835840860730338L, -2521325055659080948L, 4867868962443297827L, 1498682999415010002L,
    -8626056615231480947L, 7482827008138251355L, -617731006306969209L, 7289733825183505098L,
    4776896707697368229L, 1428059224718910376L, 6690813482653982021L, -6248474067697161171L,
    4951407828574235127L, 6198050452789369270L, 5776283192552877204L, -626480755095427154L,
    -6637184445929957204L, 8370873622748562952L, -1705978583731280501L, -7898818752540221055L,
    -2516210193198301541L, 8356900479849653862L, -4413748141896466000L, -6040072975510680789L,
    1451490609699316991L, -7948005844616396060L, 8567048088357095527L, -4375578310507393311L
  };

  private static final long[] HASHES_OF_LOOPING_BYTES_WITH_SEED_42 = {
    -7444071767201028348L, -8959994473701255385L, 7116559933691734543L, 6019482000716350659L,
    -6625277557348586272L, -5507563483608914162L, 1540412690865189709L, 4522324563441226749L,
    -7143238906056518746L, -7989831429045113014L, -7103973673268129917L, -2319060423616348937L,
    -7576144055863289344L, -8903544572546912743L, 6376815151655939880L, 5913754614426879871L,
    6466567997237536608L, -869838547529805462L, -2416009472486582019L, -3059673981515537339L,
    4211239092494362041L, 1414635639471257331L, 166863084165354636L, -3761330575439628223L,
    3524931906845391329L, 6070229753198168844L, -3740381894759773016L, -1268276809699008557L,
    1518581707938531581L, 7988048690914090770L, -4510281763783422346L, -8988936099728967847L
  };

  @Test
  public void testXxHashCorrectness() {
    byte[] data = new byte[32];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;

      ByteBuffer input = ByteBuffer.wrap(data, 0, i).order(ByteOrder.nativeOrder());
      assertEquals(
          HASHES_OF_LOOPING_BYTES_WITH_SEED_0[i],
          LongHashFunction.xx(0).hashBytes(input));

      assertEquals(
          HASHES_OF_LOOPING_BYTES_WITH_SEED_42[i],
          LongHashFunction.xx(42).hashBytes(input));
    }
  }
}
