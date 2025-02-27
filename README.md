parquet-kotlin
=================

このライブラリは、[Apache Parquet](https://parquet.apache.org/) のKotlin実装です。

[Apache Parquet Java](https://github.com/apache/parquet-java/) のコードをフォークしています。

Hadoop依存をなくし、Androidで動作するようにすることを目的としています。

[![Build](https://github.com/gahojin/parquet-kotlin/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/gahojin/parquet-kotlin/actions/workflows/build.yml)
[![Maven Central](https://img.shields.io/maven-central/v/jp.co.gahojin.parquet/parquet-common.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22jp.co.gahojin.parquet%22%20AND%20a:%22parquet-common%22)
[![License](https://img.shields.io/badge/license-Apache%202-green)](LICENSE)


## 進捗

- [x] parquet-generator のKotlin移行
- [x] parquet-format-structures のKotlin移行
- [x] parquet-encoding のKotlin移行
- [ ] parquet-common のKotlin移行
- [ ] parquet-column のKotlin移行
- [ ] Hadopp依存箇所のリファクタリング
- [ ] Jackson依存箇所のリファクタリング (Kotlin Serializationへ移行を検討)
- [ ] リフレクション使用箇所のリファクタリング (Android実行時の支障となるため)
- [ ] パフォーマンス改善


## License

```
Copyright 2025 GAHOJIN, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Notice

このプロダクトには、Apache Parquet Javaのコードが含まれています。NOTICEファイルには以下の内容が含まれています。

This product includes code from Apache Parquet Java, which includes the following in
its NOTICE file:

```
Apache Parquet Java
Copyright 2014-2024 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (http://www.apache.org/).
```
