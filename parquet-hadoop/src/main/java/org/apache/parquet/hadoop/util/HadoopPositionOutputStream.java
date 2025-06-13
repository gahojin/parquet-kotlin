/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.hadoop.util;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.io.PositionOutputStream;

public class HadoopPositionOutputStream extends PositionOutputStream {
  private final FSDataOutputStream wrapped;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  HadoopPositionOutputStream(FSDataOutputStream wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public long getPos() throws IOException {
    return wrapped.getPos();
  }

  @Override
  public void write(int b) throws IOException {
    wrapped.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    wrapped.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    wrapped.write(b, off, len);
  }

  public void sync() throws IOException {
    wrapped.hsync();
  }

  @Override
  public void flush() throws IOException {
    wrapped.flush();
  }

  @Override
  public void close() throws IOException {
    if (closed.getAndSet(true)) {
      return;
    }
    try (FSDataOutputStream fdos = wrapped) {
      fdos.flush();
    }
  }
}
