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
package org.apache.parquet.column.values.bitpacking;

import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingReader;
import org.apache.parquet.column.values.bitpacking.BitPacking.BitPackingWriter;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;

public class TestBitPacking {
  public static String toString(int[] vals) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int i : vals) {
      if (first) {
        first = false;
      } else {
        sb.append(" ");
      }
      sb.append(i);
    }
    return sb.toString();
  }

  public static String toString(long[] vals) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (long i : vals) {
      if (first) {
        first = false;
      } else {
        sb.append(" ");
      }
      sb.append(i);
    }
    return sb.toString();
  }

  public static String toString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (byte b : bytes) {
      if (first) {
        first = false;
      } else {
        sb.append(" ");
      }
      int i = b < 0 ? 256 + b : b;
      String binaryString = Integer.toBinaryString(i);
      for (int j = binaryString.length(); j < 8; ++j) {
        sb.append("0");
      }
      sb.append(binaryString);
    }
    return sb.toString();
  }
}
