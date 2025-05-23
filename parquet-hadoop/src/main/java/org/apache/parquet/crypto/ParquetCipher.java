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

package org.apache.parquet.crypto;

import org.apache.parquet.format.AesGcmCtrV1;
import org.apache.parquet.format.AesGcmV1;
import org.apache.parquet.format.EncryptionAlgorithm;

public enum ParquetCipher {
  AES_GCM_V1 {
    @Override
    public EncryptionAlgorithm getEncryptionAlgorithm() {
      return new EncryptionAlgorithm.AesGcmV1(new AesGcmV1());
    }
  },
  AES_GCM_CTR_V1 {
    @Override
    public EncryptionAlgorithm getEncryptionAlgorithm() {
      return new EncryptionAlgorithm.AesGcmCtrV1(new AesGcmCtrV1());
    }
  };

  public abstract EncryptionAlgorithm getEncryptionAlgorithm();
}
