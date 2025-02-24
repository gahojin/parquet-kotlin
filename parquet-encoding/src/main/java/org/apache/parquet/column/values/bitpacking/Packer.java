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

/**
 * Factory for packing implementations
 */
public enum Packer {

  /**
   * packers who fill the Least Significant Bit First
   * int and byte packer have the same result on Big Endian architectures
   */
  BIG_ENDIAN {
    @Override
    public IntPacker newIntPacker(int width) {
      return beIntPackerFactory.newIntPacker(width);
    }

    @Override
    public BytePacker newBytePacker(int width) {
      return beBytePackerFactory.newBytePacker(width);
    }

    @Override
    public BytePacker newBytePackerVector(int width) {
      throw new RuntimeException("Not currently supported!");
    }

    @Override
    public BytePackerForLong newBytePackerForLong(int width) {
      return beBytePackerForLongFactory.newBytePackerForLong(width);
    }
  },

  /**
   * packers who fill the Most Significant Bit first
   * int and byte packer have the same result on Little Endian architectures
   */
  LITTLE_ENDIAN {
    @Override
    public IntPacker newIntPacker(int width) {
      return leIntPackerFactory.newIntPacker(width);
    }

    @Override
    public BytePacker newBytePacker(int width) {
      return leBytePackerFactory.newBytePacker(width);
    }

    @Override
    public BytePacker newBytePackerVector(int width) {
      throw new RuntimeException("No enable java vector plugin on little endian architectures");
    }

    @Override
    public BytePackerForLong newBytePackerForLong(int width) {
      return leBytePackerForLongFactory.newBytePackerForLong(width);
    }
  };

  static final IntPackerFactory beIntPackerFactory = LemireBitPackingBE.factory;
  static final IntPackerFactory leIntPackerFactory = LemireBitPackingLE.factory;
  static final BytePackerFactory beBytePackerFactory = ByteBitPackingBE.factory;
  static final BytePackerFactory leBytePackerFactory = ByteBitPackingLE.factory;
  static final BytePackerForLongFactory beBytePackerForLongFactory = ByteBitPackingForLongBE.factory;
  static final BytePackerForLongFactory leBytePackerForLongFactory = ByteBitPackingForLongLE.factory;

  /**
   * @param width the width in bits of the packed values
   * @return an int based packer
   */
  public abstract IntPacker newIntPacker(int width);

  /**
   * @param width the width in bits of the packed values
   * @return a byte based packer
   */
  public abstract BytePacker newBytePacker(int width);

  public BytePacker newBytePackerVector(int width) {
    throw new RuntimeException("newBytePackerVector must be implemented by subclasses!");
  }

  /**
   * @param width the width in bits of the packed values
   * @return a byte based packer for INT64
   */
  public abstract BytePackerForLong newBytePackerForLong(int width);
}
