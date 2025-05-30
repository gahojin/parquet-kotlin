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
package org.apache.parquet.format.converter;

import static java.util.Collections.emptyList;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.Type.INT32;
import static org.apache.parquet.format.Util.readPageHeader;
import static org.apache.parquet.format.Util.writePageHeader;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.filterFileMetaDataByMidpoint;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.filterFileMetaDataByStart;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.getOffset;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import okio.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.FixedBinaryTestUtils;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.geospatial.GeospatialTypes;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.example.Paper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.BoundingBox;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.GeospatialStatistics;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetMetadataConverter {
  private static SecureRandom random = new SecureRandom();
  private static final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
  private static final String CHAR_UPPER = CHAR_LOWER.toUpperCase();
  private static final String NUMBER = "0123456789";
  private static final String DATA_FOR_RANDOM_STRING = CHAR_LOWER + CHAR_UPPER + NUMBER;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testPageHeader() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PageType type = PageType.DATA_PAGE;
    int compSize = 10;
    int uncSize = 20;
    PageHeader pageHeader = new PageHeader(type, uncSize, compSize);
    writePageHeader(pageHeader, out);
    PageHeader readPageHeader = readPageHeader(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(pageHeader, readPageHeader);
  }

  @Test
  public void testSchemaConverter() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(Paper.schema);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(Paper.schema, schema);
  }

  @Test
  public void testSchemaConverterDecimal() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> schemaElements = parquetMetadataConverter.toParquetSchema(Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinaryDecimal")
        .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(4)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aFixedDecimal")
        .named("Message"));
    List<SchemaElement> expected = Lists.newArrayList(
        new SchemaElement(null, null, null, "Message", 2),
        new SchemaElement(Type.BYTE_ARRAY, null, FieldRepetitionType.REQUIRED, "aBinaryDecimal", null, ConvertedType.DECIMAL, 2, 9, null, new LogicalType.DECIMAL(new DecimalType(2, 9))),
        new SchemaElement(Type.FIXED_LEN_BYTE_ARRAY, 4, FieldRepetitionType.OPTIONAL, "aFixedDecimal", null, ConvertedType.DECIMAL, 2, 9, null, new LogicalType.DECIMAL(new DecimalType(2, 9)))
    );
    Assert.assertEquals(expected, schemaElements);
  }

  @Test
  public void testParquetMetadataConverterWithDictionary() throws IOException {
    ParquetMetadata parquetMetaData = createParquetMetaData(Encoding.PLAIN_DICTIONARY, Encoding.PLAIN);
    testParquetMetadataConverterWithDictionary(parquetMetaData);
  }

  @Test
  public void testParquetMetadataConverterWithDictionaryAndWithoutEncodingStats() throws IOException {
    ParquetMetadata parquetMetaData = createParquetMetaData(Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, false);
    testParquetMetadataConverterWithDictionary(parquetMetaData);
  }

  private void testParquetMetadataConverterWithDictionary(ParquetMetadata parquetMetaData) throws IOException {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData fmd1 = converter.toParquetMetadata(1, parquetMetaData);

    // Flag should be true
    fmd1.rowGroups.forEach(rowGroup -> rowGroup.columns.forEach(column -> {
      assertNotNull(column.metaData.dictionaryPageOffset);
    }));

    ByteArrayOutputStream metaDataOutputStream = new ByteArrayOutputStream();
    Util.writeFileMetaData(fmd1, metaDataOutputStream);
    ByteArrayInputStream metaDataInputStream = new ByteArrayInputStream(metaDataOutputStream.toByteArray());
    FileMetaData fmd2 = Util.readFileMetaData(metaDataInputStream);
    ParquetMetadata parquetMetaDataConverted = converter.fromParquetMetadata(fmd2);

    long dicOffsetOriginal =
        parquetMetaData.getBlocks().get(0).getColumns().get(0).getDictionaryPageOffset();
    long dicOffsetConverted =
        parquetMetaDataConverted.getBlocks().get(0).getColumns().get(0).getDictionaryPageOffset();

    Assert.assertEquals(dicOffsetOriginal, dicOffsetConverted);
  }

  @Test
  public void testParquetMetadataConverterWithoutDictionary() throws IOException {
    ParquetMetadata parquetMetaData = createParquetMetaData(null, Encoding.PLAIN);

    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData fmd1 = converter.toParquetMetadata(1, parquetMetaData);

    // Flag should be false
    fmd1.rowGroups.forEach(rowGroup -> rowGroup.columns.forEach(column -> {
      assertNull(column.metaData.dictionaryPageOffset);
    }));

    ByteArrayOutputStream metaDataOutputStream = new ByteArrayOutputStream();
    Util.writeFileMetaData(fmd1, metaDataOutputStream);
    ByteArrayInputStream metaDataInputStream = new ByteArrayInputStream(metaDataOutputStream.toByteArray());
    FileMetaData fmd2 = Util.readFileMetaData(metaDataInputStream);
    ParquetMetadata pmd2 = converter.fromParquetMetadata(fmd2);

    long dicOffsetConverted = pmd2.getBlocks().get(0).getColumns().get(0).getDictionaryPageOffset();

    Assert.assertEquals(0, dicOffsetConverted);
  }

  @Test
  public void testBloomFilterOffset() throws IOException {
    ParquetMetadata origMetaData = createParquetMetaData(null, Encoding.PLAIN);
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Without bloom filter offset
    FileMetaData footer = converter.toParquetMetadata(1, origMetaData);
    assertNull(
        footer.rowGroups.get(0).columns.get(0).metaData.bloomFilterOffset);
    ParquetMetadata convertedMetaData = converter.fromParquetMetadata(footer);
    assertTrue(convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterOffset() < 0);

    // With bloom filter offset
    origMetaData.getBlocks().get(0).getColumns().get(0).setBloomFilterOffset(1234);
    footer = converter.toParquetMetadata(1, origMetaData);
    assertNotNull(
        footer.rowGroups.get(0).columns.get(0).metaData.bloomFilterOffset);
    assertEquals(
        1234,
        footer.rowGroups.get(0).columns.get(0).metaData.bloomFilterOffset.longValue());
    convertedMetaData = converter.fromParquetMetadata(footer);
    assertEquals(
        1234, convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterOffset());
  }

  @Test
  public void testBloomFilterLength() throws IOException {
    ParquetMetadata origMetaData = createParquetMetaData(null, Encoding.PLAIN);
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Without bloom filter length
    FileMetaData footer = converter.toParquetMetadata(1, origMetaData);
    assertNull(
        footer.rowGroups.get(0).columns.get(0).metaData.bloomFilterLength);
    ParquetMetadata convertedMetaData = converter.fromParquetMetadata(footer);
    assertTrue(convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterLength() < 0);

    // With bloom filter length
    origMetaData.getBlocks().get(0).getColumns().get(0).setBloomFilterLength(1024);
    footer = converter.toParquetMetadata(1, origMetaData);
    assertNotNull(
        footer.rowGroups.get(0).columns.get(0).metaData.bloomFilterLength);
    assertEquals(
        1024,
        footer.rowGroups.get(0).columns.get(0).metaData.bloomFilterLength.intValue());
    convertedMetaData = converter.fromParquetMetadata(footer);
    assertEquals(
        1024, convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterLength());
  }

  @Test
  public void testLogicalTypesBackwardCompatibleWithConvertedTypes() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType expected = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinaryDecimal")
        .named("Message");
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    // Set logical type field to null to test backward compatibility with files written by older API,
    // where converted_types are written to the metadata, but logicalType is missing
    parquetSchema.get(1).logicalType = null;
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
  }

  @Test
  public void testIncompatibleLogicalAndConvertedTypes() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinary")
        .named("Message");
    MessageType expected = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.jsonType())
        .named("aBinary")
        .named("Message");

    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    // Set converted type field to a different type to verify that in case of mismatch, it overrides logical type
    parquetSchema.get(1).convertedType = ConvertedType.JSON;
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, actual);
  }

  @Test
  public void testTimeLogicalTypes() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType expected = Types.buildMessage()
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(false, MILLIS))
        .named("aTimestampNonUtcMillis")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(true, MILLIS))
        .named("aTimestampUtcMillis")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(false, MICROS))
        .named("aTimestampNonUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(true, MICROS))
        .named("aTimestampUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(false, NANOS))
        .named("aTimestampNonUtcNanos")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(true, NANOS))
        .named("aTimestampUtcNanos")
        .required(PrimitiveTypeName.INT32)
        .as(timeType(false, MILLIS))
        .named("aTimeNonUtcMillis")
        .required(PrimitiveTypeName.INT32)
        .as(timeType(true, MILLIS))
        .named("aTimeUtcMillis")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(false, MICROS))
        .named("aTimeNonUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(true, MICROS))
        .named("aTimeUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(false, NANOS))
        .named("aTimeNonUtcNanos")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(true, NANOS))
        .named("aTimeUtcNanos")
        .named("Message");
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
  }

  @Test
  public void testLogicalToConvertedTypeConversion() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    assertEquals(ConvertedType.UTF8, parquetMetadataConverter.convertToConvertedType(stringType()));
    assertEquals(ConvertedType.ENUM, parquetMetadataConverter.convertToConvertedType(enumType()));

    assertEquals(ConvertedType.INT_8, parquetMetadataConverter.convertToConvertedType(intType(8, true)));
    assertEquals(ConvertedType.INT_16, parquetMetadataConverter.convertToConvertedType(intType(16, true)));
    assertEquals(ConvertedType.INT_32, parquetMetadataConverter.convertToConvertedType(intType(32, true)));
    assertEquals(ConvertedType.INT_64, parquetMetadataConverter.convertToConvertedType(intType(64, true)));
    assertEquals(ConvertedType.UINT_8, parquetMetadataConverter.convertToConvertedType(intType(8, false)));
    assertEquals(ConvertedType.UINT_16, parquetMetadataConverter.convertToConvertedType(intType(16, false)));
    assertEquals(ConvertedType.UINT_32, parquetMetadataConverter.convertToConvertedType(intType(32, false)));
    assertEquals(ConvertedType.UINT_64, parquetMetadataConverter.convertToConvertedType(intType(64, false)));
    assertEquals(ConvertedType.DECIMAL, parquetMetadataConverter.convertToConvertedType(decimalType(8, 16)));

    assertEquals(
        ConvertedType.TIMESTAMP_MILLIS,
        parquetMetadataConverter.convertToConvertedType(timestampType(true, MILLIS)));
    assertEquals(
        ConvertedType.TIMESTAMP_MICROS,
        parquetMetadataConverter.convertToConvertedType(timestampType(true, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timestampType(true, NANOS)));
    assertEquals(
        ConvertedType.TIMESTAMP_MILLIS,
        parquetMetadataConverter.convertToConvertedType(timestampType(false, MILLIS)));
    assertEquals(
        ConvertedType.TIMESTAMP_MICROS,
        parquetMetadataConverter.convertToConvertedType(timestampType(false, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timestampType(false, NANOS)));

    assertEquals(
        ConvertedType.TIME_MILLIS, parquetMetadataConverter.convertToConvertedType(timeType(true, MILLIS)));
    assertEquals(
        ConvertedType.TIME_MICROS, parquetMetadataConverter.convertToConvertedType(timeType(true, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timeType(true, NANOS)));
    assertEquals(
        ConvertedType.TIME_MILLIS, parquetMetadataConverter.convertToConvertedType(timeType(false, MILLIS)));
    assertEquals(
        ConvertedType.TIME_MICROS, parquetMetadataConverter.convertToConvertedType(timeType(false, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timeType(false, NANOS)));

    assertEquals(ConvertedType.DATE, parquetMetadataConverter.convertToConvertedType(dateType()));

    assertEquals(
        ConvertedType.INTERVAL,
        parquetMetadataConverter.convertToConvertedType(
            LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance()));
    assertEquals(ConvertedType.JSON, parquetMetadataConverter.convertToConvertedType(jsonType()));
    assertEquals(ConvertedType.BSON, parquetMetadataConverter.convertToConvertedType(bsonType()));

    assertNull(parquetMetadataConverter.convertToConvertedType(uuidType()));

    assertEquals(ConvertedType.LIST, parquetMetadataConverter.convertToConvertedType(listType()));
    assertEquals(ConvertedType.MAP, parquetMetadataConverter.convertToConvertedType(mapType()));
    assertEquals(
        ConvertedType.MAP_KEY_VALUE,
        parquetMetadataConverter.convertToConvertedType(
            LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance()));
  }

  @Test
  public void testEnumEquivalence() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    for (org.apache.parquet.column.Encoding encoding : org.apache.parquet.column.Encoding.values()) {
      assertEquals(
          encoding, parquetMetadataConverter.getEncoding(parquetMetadataConverter.getEncoding(encoding)));
    }
    for (org.apache.parquet.format.Encoding encoding : org.apache.parquet.format.Encoding.values()) {
      assertEquals(
          encoding, parquetMetadataConverter.getEncoding(parquetMetadataConverter.getEncoding(encoding)));
    }
    for (Repetition repetition : Repetition.values()) {
      assertEquals(
          repetition,
          parquetMetadataConverter.fromParquetRepetition(
              parquetMetadataConverter.toParquetRepetition(repetition)));
    }
    for (FieldRepetitionType repetition : FieldRepetitionType.values()) {
      assertEquals(
          repetition,
          parquetMetadataConverter.toParquetRepetition(
              parquetMetadataConverter.fromParquetRepetition(repetition)));
    }
    for (PrimitiveTypeName primitiveTypeName : PrimitiveTypeName.values()) {
      assertEquals(
          primitiveTypeName,
          parquetMetadataConverter.getPrimitive(parquetMetadataConverter.getType(primitiveTypeName)));
    }
    for (Type type : Type.values()) {
      assertEquals(type, parquetMetadataConverter.getType(parquetMetadataConverter.getPrimitive(type)));
    }
    for (OriginalType original : OriginalType.values()) {
      assertEquals(
          original,
          parquetMetadataConverter
              .getLogicalTypeAnnotation(
                  parquetMetadataConverter.convertToConvertedType(
                      LogicalTypeAnnotation.fromOriginalType(original, null)),
                  null)
              .toOriginalType());
    }
    for (ConvertedType converted : ConvertedType.values()) {
      assertEquals(
          converted,
          parquetMetadataConverter.convertToConvertedType(
              parquetMetadataConverter.getLogicalTypeAnnotation(converted, null)));
    }
  }

  private FileMetaData metadata(long... sizes) {
    List<SchemaElement> schema = emptyList();
    List<RowGroup> rowGroups = new ArrayList<RowGroup>();
    long offset = 0;
    for (long size : sizes) {
      ColumnChunk columnChunk = new ColumnChunk(null, offset);
      columnChunk.metaData = new ColumnMetaData(
          INT32,
          Collections.<org.apache.parquet.format.Encoding>emptyList(),
          Collections.<String>emptyList(),
          UNCOMPRESSED,
          10l,
          size * 2,
          size,
          null,
          offset);
      rowGroups.add(new RowGroup(List.of(columnChunk), size, 1));
      offset += size;
    }
    return new FileMetaData(1, schema, sizes.length, rowGroups);
  }

  private FileMetaData filter(FileMetaData md, long start, long end) {
    return filterFileMetaDataByMidpoint(
        md.deepCopy(),
        new ParquetMetadataConverter.RangeMetadataFilter(start, end));
  }

  private FileMetaData find(FileMetaData md, Long... blockStart) {
    return filterFileMetaDataByStart(
        md.deepCopy(),
        new ParquetMetadataConverter.OffsetMetadataFilter(Sets.newHashSet(blockStart)));
  }

  private FileMetaData find(FileMetaData md, long blockStart) {
    return filterFileMetaDataByStart(
        md.deepCopy(),
        new ParquetMetadataConverter.OffsetMetadataFilter(Sets.newHashSet(blockStart)));
  }

  private void verifyMD(FileMetaData md, long... offsets) {
    assertEquals(offsets.length, md.rowGroups.size());
    for (int i = 0; i < offsets.length; i++) {
      long offset = offsets[i];
      RowGroup rowGroup = md.rowGroups.get(i);
      assertEquals(offset, getOffset(rowGroup));
    }
  }

  /**
   * verifies that splits will end up being a partition of the rowgroup
   * they are all found only once
   *
   * @param md
   * @param splitWidth
   */
  private void verifyAllFilters(FileMetaData md, long splitWidth) {
    Set<Long> offsetsFound = new TreeSet<Long>();
    for (long start = 0; start < fileSize(md); start += splitWidth) {
      FileMetaData filtered = filter(md, start, start + splitWidth);
      for (RowGroup rg : filtered.rowGroups) {
        long o = getOffset(rg);
        if (offsetsFound.contains(o)) {
          fail("found the offset twice: " + o);
        } else {
          offsetsFound.add(o);
        }
      }
    }
    if (offsetsFound.size() != md.rowGroups.size()) {
      fail("missing row groups, " + "found: " + offsetsFound + "\nexpected " + md.rowGroups);
    }
  }

  private long fileSize(FileMetaData md) {
    long size = 0;
    for (RowGroup rg : md.rowGroups) {
      size += rg.totalByteSize;
    }
    return size;
  }

  @Test
  public void testFilterMetaData() {
    verifyMD(filter(metadata(50, 50, 50), 0, 50), 0);
    verifyMD(filter(metadata(50, 50, 50), 50, 100), 50);
    verifyMD(filter(metadata(50, 50, 50), 100, 150), 100);
    // picks up first RG
    verifyMD(filter(metadata(50, 50, 50), 25, 75), 0);
    // picks up no RG
    verifyMD(filter(metadata(50, 50, 50), 26, 75));
    // picks up second RG
    verifyMD(filter(metadata(50, 50, 50), 26, 76), 50);

    verifyAllFilters(metadata(50, 50, 50), 10);
    verifyAllFilters(metadata(50, 50, 50), 51);
    verifyAllFilters(metadata(50, 50, 50), 25); // corner cases are in the middle
    verifyAllFilters(metadata(50, 50, 50), 24);
    verifyAllFilters(metadata(50, 50, 50), 26);
    verifyAllFilters(metadata(50, 50, 50), 110);
    verifyAllFilters(metadata(10, 50, 500), 110);
    verifyAllFilters(metadata(10, 50, 500), 10);
    verifyAllFilters(metadata(10, 50, 500), 600);
    verifyAllFilters(metadata(11, 9, 10), 10);
    verifyAllFilters(metadata(11, 9, 10), 9);
    verifyAllFilters(metadata(11, 9, 10), 8);
  }

  @Test
  public void testFindRowGroups() {
    verifyMD(find(metadata(50, 50, 50), 0), 0);
    verifyMD(find(metadata(50, 50, 50), 50), 50);
    verifyMD(find(metadata(50, 50, 50), 100), 100);
    verifyMD(find(metadata(50, 50, 50), 0L, 50L), 0, 50);
    verifyMD(find(metadata(50, 50, 50), 0L, 50L, 100L), 0, 50, 100);
    verifyMD(find(metadata(50, 50, 50), 50L, 100L), 50, 100);
    // doesn't find an offset that isn't the start of a row group.
    verifyMD(find(metadata(50, 50, 50), 10));
  }

  @Test
  public void randomTestFilterMetaData() {
    // randomized property based testing
    // if it fails add the case above
    Random random = new Random(42);
    for (int j = 0; j < 100; j++) {
      long[] rgs = new long[random.nextInt(50)];
      for (int i = 0; i < rgs.length; i++) {
        rgs[i] = random.nextInt(10000) + 1; // No empty row groups
      }
      int splitSize = random.nextInt(10000) + 1; // 0 would lead to an infinite loop
      try {
        verifyAllFilters(metadata(rgs), splitSize);
      } catch (AssertionError e) {
        throw (AssertionError) new AssertionError(
                "fail verifyAllFilters(metadata(" + Arrays.toString(rgs) + "), " + splitSize + ")")
            .initCause(e);
      }
    }
  }

  @Test
  public void testFieldMetadataDebugLogging() {
    MessageType schema = parseMessageType("message test { optional binary some_null_field; }");
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(
            schema,
            new HashMap<>(),
            null,
            org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType.UNENCRYPTED,
            null);
    List<BlockMetaData> blockMetaDataList = new ArrayList<>();
    BlockMetaData blockMetaData = new BlockMetaData();
    blockMetaData.addColumn(createColumnChunkMetaData());
    blockMetaDataList.add(blockMetaData);
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, blockMetaDataList);
    ParquetMetadata.toJSON(metadata);
  }

  @Test
  public void testEncryptedFieldMetadataDebugLogging() {
    Configuration conf = new Configuration();
    conf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        "org.apache.parquet.crypto.SampleDecryptionPropertiesFactory");
    DecryptionPropertiesFactory decryptionPropertiesFactory = DecryptionPropertiesFactory.loadFactory(conf);
    FileDecryptionProperties decryptionProperties =
        decryptionPropertiesFactory.getFileDecryptionProperties(conf, null);

    MessageType schema = parseMessageType("message test { optional binary some_null_field; }");

    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(
            schema,
            new HashMap<>(),
            null,
            org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType.ENCRYPTED_FOOTER,
            new InternalFileDecryptor(decryptionProperties));

    List<BlockMetaData> blockMetaDataList = new ArrayList<>();
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, blockMetaDataList);
    ParquetMetadata.toJSON(metadata);
    System.out.println(ParquetMetadata.toPrettyJSON(metadata));
  }

  @Test
  public void testMetadataToJson() {
    ParquetMetadata metadata = new ParquetMetadata(null, null);
    assertEquals("{\"fileMetaData\":null,\"blocks\":null}", ParquetMetadata.toJSON(metadata));
    assertEquals(
        ("{\n" + "  \"fileMetaData\" : null,\n" + "  \"blocks\" : null\n" + "}")
            .replace("\n", System.lineSeparator()),
        ParquetMetadata.toPrettyJSON(metadata));
  }

  private ColumnChunkMetaData createColumnChunkMetaData() {
    Set<org.apache.parquet.column.Encoding> e = new HashSet<org.apache.parquet.column.Encoding>();
    PrimitiveTypeName t = PrimitiveTypeName.BINARY;
    ColumnPath p = ColumnPath.get("foo");
    CompressionCodecName c = CompressionCodecName.GZIP;
    BinaryStatistics s = new BinaryStatistics();
    ColumnChunkMetaData md = ColumnChunkMetaData.get(p, t, c, e, s, 0, 0, 0, 0, 0);
    return md;
  }

  @Test
  public void testEncodingsCache() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    List<org.apache.parquet.format.Encoding> formatEncodingsCopy1 = Arrays.asList(
        org.apache.parquet.format.Encoding.BIT_PACKED,
        org.apache.parquet.format.Encoding.RLE_DICTIONARY,
        org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    List<org.apache.parquet.format.Encoding> formatEncodingsCopy2 = Arrays.asList(
        org.apache.parquet.format.Encoding.BIT_PACKED,
        org.apache.parquet.format.Encoding.RLE_DICTIONARY,
        org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    Set<org.apache.parquet.column.Encoding> expected = new HashSet<>();
    expected.add(org.apache.parquet.column.Encoding.BIT_PACKED);
    expected.add(org.apache.parquet.column.Encoding.RLE_DICTIONARY);
    expected.add(org.apache.parquet.column.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    Set<org.apache.parquet.column.Encoding> res1 =
        parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res2 =
        parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res3 =
        parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy2);

    // make sure they are all semantically equal
    assertEquals(expected, res1);
    assertEquals(expected, res2);
    assertEquals(expected, res3);

    // make sure res1, res2, and res3 are actually the same cached object
    assertSame(res1, res2);
    assertSame(res1, res3);

    // make sure they are all unmodifiable (UnmodifiableSet is not public, so we have to compare on class name)
    assertEquals("java.util.Collections$UnmodifiableSet", res1.getClass().getName());
    assertEquals("java.util.Collections$UnmodifiableSet", res2.getClass().getName());
    assertEquals("java.util.Collections$UnmodifiableSet", res3.getClass().getName());
  }

  @Test
  public void testBinaryStatsV1() {
    testBinaryStats(StatsHelper.V1);
  }

  @Test
  public void testBinaryStatsV2() {
    testBinaryStats(StatsHelper.V2);
  }

  private void testBinaryStats(StatsHelper helper) {
    // make fake stats and verify the size check
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls(3004);
    byte[] min = new byte[904];
    byte[] max = new byte[2388];
    stats.updateStats(Binary.fromConstantByteArray(min));
    stats.updateStats(Binary.fromConstantByteArray(max));
    long totalLen = min.length + max.length;
    Assert.assertFalse("Should not be smaller than min + max size", stats.isSmallerThan(totalLen));
    Assert.assertTrue("Should be smaller than min + max size + 1", stats.isSmallerThan(totalLen + 1));

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    assertNull("Min should not be set", formatStats.min);
    assertNull("Max should not be set", formatStats.max);
    if (helper == StatsHelper.V2) {
      assertArrayEquals("Min_value should match", min, formatStats.minValue.toByteArray());
      assertArrayEquals("Max_value should match", max, formatStats.maxValue.toByteArray());
    }
    Assert.assertEquals("Num nulls should match", 3004L, formatStats.nullCount.longValue());

    // convert to empty stats because the values are too large
    stats.setMinMaxFromBytes(max, max);

    formatStats = helper.toParquetStatistics(stats);

    assertNull("Min should not be set", formatStats.min);
    assertNull("Max should not be set", formatStats.max);
    assertNull("Min_value should not be set", formatStats.minValue);
    assertNull("Max_value should not be set", formatStats.maxValue);
    assertNull("Num nulls should not be set", formatStats.nullCount);

    Statistics roundTripStats = ParquetMetadataConverter.fromParquetStatisticsInternal(
        Version.FULL_VERSION,
        formatStats,
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, ""),
        ParquetMetadataConverter.SortOrder.SIGNED);

    Assert.assertTrue(roundTripStats.isEmpty());
  }

  @Test
  public void testBinaryStatsWithTruncation() {
    int defaultTruncLen = ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
    int[] validLengths = {1, 2, 16, 64, defaultTruncLen - 1};
    for (int len : validLengths) {
      testBinaryStatsWithTruncation(len, 60, 70);
      testBinaryStatsWithTruncation(len, (int) ParquetMetadataConverter.MAX_STATS_SIZE, 190);
      testBinaryStatsWithTruncation(len, 280, (int) ParquetMetadataConverter.MAX_STATS_SIZE);
      testBinaryStatsWithTruncation(
          len, (int) ParquetMetadataConverter.MAX_STATS_SIZE, (int) ParquetMetadataConverter.MAX_STATS_SIZE);
    }

    int[] invalidLengths = {-1, 0, Integer.MAX_VALUE + 1};
    for (int len : invalidLengths) {
      try {
        testBinaryStatsWithTruncation(len, 80, 20);
        Assert.fail("Expected IllegalArgumentException but didn't happen");
      } catch (IllegalArgumentException e) {
        // expected, nothing to do
      }
    }
  }

  // The number of minLen and maxLen shouldn't matter because the comparision is controlled by prefix
  private void testBinaryStatsWithTruncation(int truncateLen, int minLen, int maxLen) {
    BinaryStatistics stats = new BinaryStatistics();
    byte[] min = generateRandomString("a", minLen).getBytes();
    byte[] max = generateRandomString("b", maxLen).getBytes();
    stats.updateStats(Binary.fromConstantByteArray(min));
    stats.updateStats(Binary.fromConstantByteArray(max));
    ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter(truncateLen);
    org.apache.parquet.format.Statistics formatStats = metadataConverter.toParquetStatistics(stats);

    if (minLen + maxLen >= ParquetMetadataConverter.MAX_STATS_SIZE) {
      assertNull(formatStats.minValue);
      assertNull(formatStats.maxValue);
    } else {
      String minString = new String(min, Charset.forName("UTF-8"));
      String minStatString = formatStats.minValue.string(Charset.forName("UTF-8"));
      assertTrue(minStatString.compareTo(minString) <= 0);
      String maxString = new String(max, Charset.forName("UTF-8"));
      String maxStatString = formatStats.maxValue.string(Charset.forName("UTF-8"));
      assertTrue(maxStatString.compareTo(maxString) >= 0);
    }
  }

  private static String generateRandomString(String prefix, int length) {
    assertTrue(prefix.length() <= length);
    StringBuilder sb = new StringBuilder(length);
    sb.append(prefix);
    for (int i = 0; i < length - prefix.length(); i++) {
      int rndCharAt = random.nextInt(DATA_FOR_RANDOM_STRING.length());
      char rndChar = DATA_FOR_RANDOM_STRING.charAt(rndCharAt);
      sb.append(rndChar);
    }
    return sb.toString();
  }

  @Test
  public void testIntegerStatsV1() {
    testIntegerStats(StatsHelper.V1);
  }

  @Test
  public void testIntegerStatsV2() {
    testIntegerStats(StatsHelper.V2);
  }

  private void testIntegerStats(StatsHelper helper) {
    // make fake stats and verify the size check
    IntStatistics stats = new IntStatistics();
    stats.incrementNumNulls(3004);
    int min = Integer.MIN_VALUE;
    int max = Integer.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    Assert.assertEquals("Min should match", min, BytesUtils.bytesToInt(formatStats.min.toByteArray()));
    Assert.assertEquals("Max should match", max, BytesUtils.bytesToInt(formatStats.max.toByteArray()));
    Assert.assertEquals("Num nulls should match", 3004L, formatStats.nullCount.longValue());
  }

  @Test
  public void testLongStatsV1() {
    testLongStats(StatsHelper.V1);
  }

  @Test
  public void testLongStatsV2() {
    testLongStats(StatsHelper.V2);
  }

  private void testLongStats(StatsHelper helper) {
    // make fake stats and verify the size check
    LongStatistics stats = new LongStatistics();
    stats.incrementNumNulls(3004);
    long min = Long.MIN_VALUE;
    long max = Long.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    Assert.assertEquals("Min should match", min, BytesUtils.bytesToLong(formatStats.min.toByteArray()));
    Assert.assertEquals("Max should match", max, BytesUtils.bytesToLong(formatStats.max.toByteArray()));
    Assert.assertEquals("Num nulls should match", 3004L, formatStats.nullCount.longValue());
  }

  @Test
  public void testFloatStatsV1() {
    testFloatStats(StatsHelper.V1);
  }

  @Test
  public void testFloatStatsV2() {
    testFloatStats(StatsHelper.V2);
  }

  private void testFloatStats(StatsHelper helper) {
    // make fake stats and verify the size check
    FloatStatistics stats = new FloatStatistics();
    stats.incrementNumNulls(3004);
    float min = Float.MIN_VALUE;
    float max = Float.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    Assert.assertEquals(
        "Min should match", min, Float.intBitsToFloat(BytesUtils.bytesToInt(formatStats.min.toByteArray())), 0.000001);
    Assert.assertEquals(
        "Max should match", max, Float.intBitsToFloat(BytesUtils.bytesToInt(formatStats.max.toByteArray())), 0.000001);
    Assert.assertEquals("Num nulls should match", 3004L, formatStats.nullCount.longValue());
  }

  @Test
  public void testDoubleStatsV1() {
    testDoubleStats(StatsHelper.V1);
  }

  @Test
  public void testDoubleStatsV2() {
    testDoubleStats(StatsHelper.V2);
  }

  private void testDoubleStats(StatsHelper helper) {
    // make fake stats and verify the size check
    DoubleStatistics stats = new DoubleStatistics();
    stats.incrementNumNulls(3004);
    double min = Double.MIN_VALUE;
    double max = Double.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    Assert.assertEquals(
        "Min should match",
        min,
        Double.longBitsToDouble(BytesUtils.bytesToLong(formatStats.min.toByteArray())),
        0.000001);
    Assert.assertEquals(
        "Max should match",
        max,
        Double.longBitsToDouble(BytesUtils.bytesToLong(formatStats.max.toByteArray())),
        0.000001);
    Assert.assertEquals("Num nulls should match", 3004L, formatStats.nullCount.longValue());
  }

  @Test
  public void testBooleanStatsV1() {
    testBooleanStats(StatsHelper.V1);
  }

  @Test
  public void testBooleanStatsV2() {
    testBooleanStats(StatsHelper.V2);
  }

  private void testBooleanStats(StatsHelper helper) {
    // make fake stats and verify the size check
    BooleanStatistics stats = new BooleanStatistics();
    stats.incrementNumNulls(3004);
    boolean min = Boolean.FALSE;
    boolean max = Boolean.TRUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    Assert.assertEquals("Min should match", min, BytesUtils.bytesToBool(formatStats.min.toByteArray()));
    Assert.assertEquals("Max should match", max, BytesUtils.bytesToBool(formatStats.max.toByteArray()));
    Assert.assertEquals("Num nulls should match", 3004L, formatStats.nullCount.longValue());
  }

  @Test
  public void testIgnoreStatsWithSignedSortOrder() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("z"));
    stats.incrementNumNulls();

    PrimitiveType binaryType =
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("b");
    Statistics convertedStats = converter.fromParquetStatistics(
        Version.FULL_VERSION, StatsHelper.V1.toParquetStatistics(stats), binaryType);

    Assert.assertFalse("Stats should not include min/max: " + convertedStats, convertedStats.hasNonNullValue());
    Assert.assertTrue("Stats should have null count: " + convertedStats, convertedStats.isNumNullsSet());
    Assert.assertEquals("Stats should have 3 nulls: " + convertedStats, 3L, convertedStats.getNumNulls());
  }

  @Test
  public void testStillUseStatsWithSignedSortOrderIfSingleValueV1() {
    testStillUseStatsWithSignedSortOrderIfSingleValue(StatsHelper.V1);
  }

  @Test
  public void testStillUseStatsWithSignedSortOrderIfSingleValueV2() {
    testStillUseStatsWithSignedSortOrderIfSingleValue(StatsHelper.V2);
  }

  private void testStillUseStatsWithSignedSortOrderIfSingleValue(StatsHelper helper) {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();

    PrimitiveType binaryType =
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("b");
    Statistics convertedStats = converter.fromParquetStatistics(
        Version.FULL_VERSION, ParquetMetadataConverter.toParquetStatistics(stats), binaryType);

    Assert.assertFalse("Stats should not be empty: " + convertedStats, convertedStats.isEmpty());
    assertArrayEquals(
        "min == max: " + convertedStats, convertedStats.getMaxBytes(), convertedStats.getMinBytes());
  }

  @Test
  public void testUseStatsWithSignedSortOrderV1() {
    testUseStatsWithSignedSortOrder(StatsHelper.V1);
  }

  @Test
  public void testUseStatsWithSignedSortOrderV2() {
    testUseStatsWithSignedSortOrder(StatsHelper.V2);
  }

  private void testUseStatsWithSignedSortOrder(StatsHelper helper) {
    // override defaults and use stats that were accumulated using signed order
    Configuration conf = new Configuration();
    conf.setBoolean("parquet.strings.signed-min-max.enabled", true);

    ParquetMetadataConverter converter = new ParquetMetadataConverter(conf);
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("z"));
    stats.incrementNumNulls();

    PrimitiveType binaryType =
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("b");
    Statistics convertedStats =
        converter.fromParquetStatistics(Version.FULL_VERSION, helper.toParquetStatistics(stats), binaryType);

    Assert.assertFalse("Stats should not be empty", convertedStats.isEmpty());
    Assert.assertTrue(convertedStats.isNumNullsSet());
    Assert.assertEquals("Should have 3 nulls", 3, convertedStats.getNumNulls());
    if (helper == StatsHelper.V1) {
      assertFalse("Min-max should be null for V1 stats", convertedStats.hasNonNullValue());
    } else {
      Assert.assertEquals(
          "Should have correct min (unsigned sort)", Binary.fromString("A"), convertedStats.genericGetMin());
      Assert.assertEquals(
          "Should have correct max (unsigned sort)", Binary.fromString("z"), convertedStats.genericGetMax());
    }
  }

  @Test
  public void testFloat16Stats() {
    Statistics stats = Statistics.createStats(
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 2, "float16")
            .withLogicalTypeAnnotation(LogicalTypeAnnotation.float16Type()));
    stats.updateStats(toBinary(0xff, 0x03));
    stats.updateStats(toBinary(0xff, 0x7b));
    String expectedMinStr = "6.097555E-5";
    String expectedMaxStr = "65504.0";
    assertEquals(expectedMinStr, stats.minAsString());
    assertEquals(expectedMaxStr, stats.maxAsString());
  }

  private Binary toBinary(int... bytes) {
    byte[] array = new byte[bytes.length];
    for (int i = 0; i < array.length; ++i) {
      array[i] = (byte) bytes[i];
    }
    return Binary.fromConstantByteArray(array);
  }

  @Test
  public void testMissingValuesFromStats() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    PrimitiveType type = Types.required(PrimitiveTypeName.INT32).named("test_int32");

    org.apache.parquet.format.Statistics formatStats = new org.apache.parquet.format.Statistics();
    Statistics<?> stats = converter.fromParquetStatistics(Version.FULL_VERSION, formatStats, type);
    assertFalse(stats.isNumNullsSet());
    assertFalse(stats.hasNonNullValue());
    assertTrue(stats.isEmpty());
    assertEquals(-1, stats.getNumNulls());

// TODO    formatStats.clear();
    formatStats.min = ByteString.of(BytesUtils.intToBytes(-100));
    formatStats.max = ByteString.of(BytesUtils.intToBytes(100));
    stats = converter.fromParquetStatistics(Version.FULL_VERSION, formatStats, type);
    assertFalse(stats.isNumNullsSet());
    assertTrue(stats.hasNonNullValue());
    assertFalse(stats.isEmpty());
    assertEquals(-1, stats.getNumNulls());
    assertEquals(-100, stats.genericGetMin());
    assertEquals(100, stats.genericGetMax());

    formatStats.clear();
    formatStats.nullCount = 2000L;
    stats = converter.fromParquetStatistics(Version.FULL_VERSION, formatStats, type);
    assertTrue(stats.isNumNullsSet());
    assertFalse(stats.hasNonNullValue());
    assertFalse(stats.isEmpty());
    assertEquals(2000, stats.getNumNulls());
  }

  @Test
  public void testSkippedV2Stats() {
    testSkippedV2Stats(
        Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(12)
            .as(OriginalType.INTERVAL)
            .named(""),
        new BigInteger("12345678"),
        new BigInteger("12345679"));
    testSkippedV2Stats(
        Types.optional(PrimitiveTypeName.INT96).named(""),
        new BigInteger("-75687987"),
        new BigInteger("45367657"));
  }

  private void testSkippedV2Stats(PrimitiveType type, Object min, Object max) {
    Statistics<?> stats = createStats(type, min, max);
    org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
    assertNull(statistics.min);
    assertNull(statistics.max);
    assertNull(statistics.minValue);
    assertNull(statistics.maxValue);
  }

  @Test
  public void testV2OnlyStats() {
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_8).named(""), 0x7F, 0x80);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_16).named(""), 0x7FFF, 0x8000);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_32).named(""), 0x7FFFFFFF, 0x80000000);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT64).as(OriginalType.UINT_64).named(""),
        0x7FFFFFFFFFFFFFFFL,
        0x8000000000000000L);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.BINARY)
            .as(OriginalType.DECIMAL)
            .precision(6)
            .named(""),
        new BigInteger("-765875"),
        new BigInteger("876856"));
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(14)
            .as(OriginalType.DECIMAL)
            .precision(7)
            .named(""),
        new BigInteger("-6769643"),
        new BigInteger("9864675"));
  }

  private void testV2OnlyStats(PrimitiveType type, Object min, Object max) {
    Statistics<?> stats = createStats(type, min, max);
    org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
    assertNull(statistics.min);
    assertNull(statistics.max);
    assertArrayEquals(stats.getMinBytes(), statistics.minValue.toByteArray());
    assertArrayEquals(stats.getMaxBytes(), statistics.maxValue.toByteArray());
  }

  @Test
  public void testV2StatsEqualMinMax() {
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_8).named(""), 93, 93);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_16).named(""), -5892, -5892);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_32).named(""), 234998934, 234998934);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT64).as(OriginalType.UINT_64).named(""),
        -2389943895984985L,
        -2389943895984985L);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.BINARY)
            .as(OriginalType.DECIMAL)
            .precision(6)
            .named(""),
        new BigInteger("823749"),
        new BigInteger("823749"));
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(14)
            .as(OriginalType.DECIMAL)
            .precision(7)
            .named(""),
        new BigInteger("-8752832"),
        new BigInteger("-8752832"));
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT96).named(""),
        new BigInteger("81032984"),
        new BigInteger("81032984"));
  }

  private void testV2StatsEqualMinMax(PrimitiveType type, Object min, Object max) {
    Statistics<?> stats = createStats(type, min, max);
    org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
    assertArrayEquals(stats.getMinBytes(), statistics.min.toByteArray());
    assertArrayEquals(stats.getMaxBytes(), statistics.max.toByteArray());
    assertArrayEquals(stats.getMinBytes(), statistics.minValue.toByteArray());
    assertArrayEquals(stats.getMaxBytes(), statistics.maxValue.toByteArray());
  }

  private static <T> Statistics<?> createStats(PrimitiveType type, T min, T max) {
    Class<?> c = min.getClass();
    if (c == Integer.class) {
      return createStatsTyped(type, (Integer) min, (Integer) max);
    } else if (c == Long.class) {
      return createStatsTyped(type, (Long) min, (Long) max);
    } else if (c == BigInteger.class) {
      return createStatsTyped(type, (BigInteger) min, (BigInteger) max);
    }
    fail("Not implemented");
    return null;
  }

  private static Statistics<?> createStatsTyped(PrimitiveType type, int min, int max) {
    Statistics<?> stats = Statistics.createStats(type);
    stats.updateStats(max);
    stats.updateStats(min);
    assertEquals(min, stats.genericGetMin());
    assertEquals(max, stats.genericGetMax());
    return stats;
  }

  private static Statistics<?> createStatsTyped(PrimitiveType type, long min, long max) {
    Statistics<?> stats = Statistics.createStats(type);
    stats.updateStats(max);
    stats.updateStats(min);
    assertEquals(min, stats.genericGetMin());
    assertEquals(max, stats.genericGetMax());
    return stats;
  }

  private static Statistics<?> createStatsTyped(PrimitiveType type, BigInteger min, BigInteger max) {
    Statistics<?> stats = Statistics.createStats(type);
    Binary minBinary = FixedBinaryTestUtils.getFixedBinary(type, min);
    Binary maxBinary = FixedBinaryTestUtils.getFixedBinary(type, max);
    stats.updateStats(maxBinary);
    stats.updateStats(minBinary);
    assertEquals(minBinary, stats.genericGetMin());
    assertEquals(maxBinary, stats.genericGetMax());
    return stats;
  }

  private static ParquetMetadata createParquetMetaData(Encoding dicEncoding, Encoding dataEncoding) {
    return createParquetMetaData(dicEncoding, dataEncoding, true);
  }

  private static ParquetMetadata createParquetMetaData(
      Encoding dicEncoding, Encoding dataEncoding, boolean includeEncodingStats) {
    MessageType schema = parseMessageType("message schema { optional int32 col (INT_32); }");
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(schema, new HashMap<>(), null);
    List<BlockMetaData> blockMetaDataList = new ArrayList<>();
    BlockMetaData blockMetaData = new BlockMetaData();
    EncodingStats es = null;
    if (includeEncodingStats) {
      EncodingStats.Builder builder = new EncodingStats.Builder();
      if (dicEncoding != null) {
        builder.addDictEncoding(dicEncoding).build();
      }
      builder.addDataEncoding(dataEncoding);
      es = builder.build();
    }
    Set<org.apache.parquet.column.Encoding> e = new HashSet<org.apache.parquet.column.Encoding>();
    if (!includeEncodingStats) {
      if (dicEncoding != null) {
        e.add(dicEncoding);
      }
      e.add(dataEncoding);
    }
    PrimitiveTypeName t = PrimitiveTypeName.INT32;
    ColumnPath p = ColumnPath.get("col");
    CompressionCodecName c = CompressionCodecName.UNCOMPRESSED;
    BinaryStatistics s = new BinaryStatistics();
    ColumnChunkMetaData md = ColumnChunkMetaData.get(p, t, c, es, e, s, 20, 30, 0, 0, 0);
    blockMetaData.addColumn(md);
    blockMetaDataList.add(blockMetaData);
    return new ParquetMetadata(fileMetaData, blockMetaDataList);
  }

  private enum StatsHelper {
    // Only min and max are filled (min_value and max_value are not)
    V1() {
      @Override
      public org.apache.parquet.format.Statistics toParquetStatistics(Statistics<?> stats) {
        org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
        statistics.minValue = null;
        statistics.maxValue = null;
        return statistics;
      }
    },
    // min_value and max_value are filled (min and max might be filled as well)
    V2() {
      @Override
      public org.apache.parquet.format.Statistics toParquetStatistics(Statistics<?> stats) {
        return ParquetMetadataConverter.toParquetStatistics(stats);
      }
    };

    public abstract org.apache.parquet.format.Statistics toParquetStatistics(Statistics<?> stats);
  }

  @Test
  public void testColumnOrders() throws IOException {
    MessageType schema = parseMessageType("message test {"
        + "  optional binary binary_col;" // Normal column with type defined column order -> typeDefined
        + "  optional group map_col (MAP) {"
        + "    repeated group map (MAP_KEY_VALUE) {"
        + "        required binary key (UTF8);" // Key to be hacked to have unknown column order -> undefined
        + "        optional group list_col (LIST) {"
        + "          repeated group list {"
        + "            optional int96 array_element;" // INT96 element with type defined column order ->
        // undefined
        + "          }"
        + "        }"
        + "    }"
        + "  }"
        + "}");
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(schema, new HashMap<>(), null);
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ArrayList<>());
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData formatMetadata = converter.toParquetMetadata(1, metadata);

    List<org.apache.parquet.format.ColumnOrder> columnOrders = formatMetadata.columnOrders;
    assertEquals(3, columnOrders.size());
    for (org.apache.parquet.format.ColumnOrder columnOrder : columnOrders) {
      assertTrue(columnOrder instanceof org.apache.parquet.format.ColumnOrder.TypeOrder);
    }

    // Simulate that thrift got a union type that is not in the generated code
    // (when the file contains a not-yet-supported column order)
    columnOrders.set(1, null);

    MessageType resultSchema =
        converter.fromParquetMetadata(formatMetadata).getFileMetaData().getSchema();
    List<ColumnDescriptor> columns = resultSchema.getColumns();
    assertEquals(3, columns.size());
    assertEquals(
        ColumnOrder.typeDefined(), columns.get(0).getPrimitiveType().columnOrder());
    assertEquals(ColumnOrder.undefined(), columns.get(1).getPrimitiveType().columnOrder());
    assertEquals(ColumnOrder.undefined(), columns.get(2).getPrimitiveType().columnOrder());
  }

  @Test
  public void testOffsetIndexConversion() {
    for (boolean withSizeStats : new boolean[] {false, true}) {
      OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
      builder.add(1000, 10000, 0, withSizeStats ? Optional.of(11L) : Optional.empty());
      builder.add(22000, 12000, 100, withSizeStats ? Optional.of(22L) : Optional.empty());
      OffsetIndex offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(
          ParquetMetadataConverter.toParquetOffsetIndex(builder.build(100000)));
      assertEquals(2, offsetIndex.getPageCount());
      assertEquals(101000, offsetIndex.getOffset(0));
      assertEquals(10000, offsetIndex.getCompressedPageSize(0));
      assertEquals(0, offsetIndex.getFirstRowIndex(0));
      assertEquals(122000, offsetIndex.getOffset(1));
      assertEquals(12000, offsetIndex.getCompressedPageSize(1));
      assertEquals(100, offsetIndex.getFirstRowIndex(1));
      if (withSizeStats) {
        assertEquals(Optional.of(11L), offsetIndex.getUnencodedByteArrayDataBytes(0));
        assertEquals(Optional.of(22L), offsetIndex.getUnencodedByteArrayDataBytes(1));
      } else {
        assertFalse(offsetIndex.getUnencodedByteArrayDataBytes(0).isPresent());
        assertFalse(offsetIndex.getUnencodedByteArrayDataBytes(1).isPresent());
      }
    }
  }

  @Test
  public void testColumnIndexConversion() {
    for (boolean withSizeStats : new boolean[] {false, true}) {
      PrimitiveType type = Types.required(PrimitiveTypeName.INT64).named("test_int64");
      ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
      Statistics<?> stats = Statistics.createStats(type);
      stats.incrementNumNulls(16);
      stats.updateStats(-100l);
      stats.updateStats(100l);
      builder.add(
          stats,
          withSizeStats ? new SizeStatistics(type, 0, LongArrayList.of(1, 2), LongArrayList.of(6, 5)) : null);
      stats = Statistics.createStats(type);
      stats.incrementNumNulls(111);
      builder.add(
          stats,
          withSizeStats ? new SizeStatistics(type, 0, LongArrayList.of(3, 4), LongArrayList.of(4, 3)) : null);
      stats = Statistics.createStats(type);
      stats.updateStats(200l);
      stats.updateStats(500l);
      builder.add(
          stats,
          withSizeStats ? new SizeStatistics(type, 0, LongArrayList.of(5, 6), LongArrayList.of(2, 1)) : null);
      org.apache.parquet.format.ColumnIndex parquetColumnIndex =
          ParquetMetadataConverter.toParquetColumnIndex(type, builder.build());
      ColumnIndex columnIndex = ParquetMetadataConverter.fromParquetColumnIndex(type, parquetColumnIndex);
      assertEquals(BoundaryOrder.ASCENDING, columnIndex.getBoundaryOrder());
      assertTrue(Arrays.asList(false, true, false).equals(columnIndex.getNullPages()));
      assertTrue(Arrays.asList(16l, 111l, 0l).equals(columnIndex.getNullCounts()));
      assertTrue(Arrays.asList(
              ByteBuffer.wrap(BytesUtils.longToBytes(-100l)),
              ByteBuffer.allocate(0),
              ByteBuffer.wrap(BytesUtils.longToBytes(200l)))
          .equals(columnIndex.getMinValues()));
      assertTrue(Arrays.asList(
              ByteBuffer.wrap(BytesUtils.longToBytes(100l)),
              ByteBuffer.allocate(0),
              ByteBuffer.wrap(BytesUtils.longToBytes(500l)))
          .equals(columnIndex.getMaxValues()));

      assertNull(
          "Should handle null column index",
          ParquetMetadataConverter.toParquetColumnIndex(
              Types.required(PrimitiveTypeName.INT32).named("test_int32"), null));
      assertNull(
          "Should ignore unsupported types",
          ParquetMetadataConverter.toParquetColumnIndex(
              Types.required(PrimitiveTypeName.INT96).named("test_int96"), columnIndex));
      assertNull(
          "Should ignore unsupported types",
          ParquetMetadataConverter.fromParquetColumnIndex(
              Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                  .length(12)
                  .as(OriginalType.INTERVAL)
                  .named("test_interval"),
              parquetColumnIndex));

      if (withSizeStats) {
        assertEquals(LongArrayList.of(1, 2, 3, 4, 5, 6), columnIndex.getRepetitionLevelHistogram());
        assertEquals(LongArrayList.of(6, 5, 4, 3, 2, 1), columnIndex.getDefinitionLevelHistogram());
      } else {
        assertEquals(LongArrayList.of(), columnIndex.getRepetitionLevelHistogram());
        assertEquals(LongArrayList.of(), columnIndex.getDefinitionLevelHistogram());
      }
    }
  }

  @Test
  public void testMapLogicalType() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType expected = Types.buildMessage()
        .requiredGroup()
        .as(mapType())
        .repeatedGroup()
        .as(LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance())
        .required(PrimitiveTypeName.BINARY)
        .as(stringType())
        .named("key")
        .required(PrimitiveTypeName.INT32)
        .named("value")
        .named("key_value")
        .named("testMap")
        .named("Message");

    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    assertEquals(5, parquetSchema.size());
    assertEquals(new SchemaElement(null, null, null, "Message", 1), parquetSchema.get(0));
    assertEquals(
        new SchemaElement(null, null, FieldRepetitionType.REQUIRED, "testMap", 1, ConvertedType.MAP, null, null, null, new LogicalType.MAP(new MapType())),
        parquetSchema.get(1));
    // PARQUET-1879 ensure that LogicalType is not written (null) but ConvertedType is MAP_KEY_VALUE for
    // backwards-compatibility
    assertEquals(
        new SchemaElement(null, null, FieldRepetitionType.REPEATED, "key_value", 2, ConvertedType.MAP_KEY_VALUE),
        parquetSchema.get(2));
    assertEquals(
        new SchemaElement(Type.BYTE_ARRAY, null, FieldRepetitionType.REQUIRED, "key", null, ConvertedType.UTF8, null, null, null, new LogicalType.STRING(new StringType())),
        parquetSchema.get(3));
    assertEquals(
        new SchemaElement(Type.INT32, null, FieldRepetitionType.REQUIRED, "value"),
        parquetSchema.get(4));

    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
  }

  @Test
  public void testMapLogicalTypeReadWrite() throws Exception {
    MessageType messageType = Types.buildMessage()
        .requiredGroup()
        .as(mapType())
        .repeatedGroup()
        .as(LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance())
        .required(PrimitiveTypeName.BINARY)
        .as(stringType())
        .named("key")
        .required(PrimitiveTypeName.INT64)
        .named("value")
        .named("key_value")
        .named("testMap")
        .named("example");

    verifyMapMessageType(messageType, "key_value");
  }

  @Test
  public void testMapConvertedTypeReadWrite() throws Exception {
    List<SchemaElement> oldConvertedTypeSchemaElements = new ArrayList<>();
    oldConvertedTypeSchemaElements.add(new SchemaElement(null, null, null, "example", 1));
    oldConvertedTypeSchemaElements.add(new SchemaElement(null, null, FieldRepetitionType.REQUIRED, "testMap", 1, ConvertedType.MAP));
    oldConvertedTypeSchemaElements.add(new SchemaElement(null, null, FieldRepetitionType.REPEATED, "map", 2, ConvertedType.MAP_KEY_VALUE));
    oldConvertedTypeSchemaElements.add(new SchemaElement(Type.BYTE_ARRAY, null, FieldRepetitionType.REQUIRED, "key", null, ConvertedType.UTF8));
    oldConvertedTypeSchemaElements.add(new SchemaElement(Type.INT64, null, FieldRepetitionType.REQUIRED, "value"));

    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType messageType = parquetMetadataConverter.fromParquetSchema(oldConvertedTypeSchemaElements, null);

    verifyMapMessageType(messageType, "map");
  }

  @Test
  public void testVariantLogicalType() {
    byte specVersion = 1;
    MessageType expected = Types.buildMessage()
            .requiredGroup()
            .as(variantType(specVersion))
            .required(PrimitiveTypeName.BINARY)
            .named("metadata")
            .required(PrimitiveTypeName.BINARY)
            .named("value")
            .named("v")
            .named("example");

    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
    LogicalTypeAnnotation logicalType = schema.getType("v").getLogicalTypeAnnotation();
    assertEquals(LogicalTypeAnnotation.variantType(specVersion), logicalType);
    assertEquals(specVersion, ((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) logicalType).getSpecVersion());
  }

  private void verifyMapMessageType(final MessageType messageType, final String keyValueName) throws IOException {
    Path file = new Path(temporaryFolder.newFolder("verifyMapMessageType").getPath(), keyValueName + ".parquet");

    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(file).withType(messageType).build()) {
      final Group group = new SimpleGroup(messageType);
      final Group mapGroup = group.addGroup("testMap");

      for (int index = 0; index < 5; index++) {
        final Group keyValueGroup = mapGroup.addGroup(keyValueName);
        keyValueGroup.add("key", Binary.fromString("key" + index));
        keyValueGroup.add("value", 100L + index);
      }

      writer.write(group);
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      Group group = reader.read();

      assertNotNull(group);

      Group testMap = group.getGroup("testMap", 0);
      assertNotNull(testMap);
      assertEquals(5, testMap.getFieldRepetitionCount(keyValueName));

      for (int index = 0; index < 5; index++) {
        assertEquals(
            "key" + index, testMap.getGroup(keyValueName, index).getString("key", 0));
        assertEquals(100L + index, testMap.getGroup(keyValueName, index).getLong("value", 0));
      }
    }
  }

  @Test
  public void testSizeStatisticsConversion() {
    PrimitiveType type = Types.required(PrimitiveTypeName.BINARY).named("test");
    List<Long> repLevelHistogram = Arrays.asList(1L, 2L, 3L, 4L, 5L);
    List<Long> defLevelHistogram = Arrays.asList(6L, 7L, 8L, 9L, 10L);
    SizeStatistics sizeStatistics = ParquetMetadataConverter.fromParquetSizeStatistics(
        ParquetMetadataConverter.toParquetSizeStatistics(
            new SizeStatistics(type, 1024, repLevelHistogram, defLevelHistogram)),
        type);
    assertEquals(type, sizeStatistics.getType());
    assertEquals(Optional.of(1024L), sizeStatistics.getUnencodedByteArrayDataBytes());
    assertEquals(repLevelHistogram, sizeStatistics.getRepetitionLevelHistogram());
    assertEquals(defLevelHistogram, sizeStatistics.getDefinitionLevelHistogram());
  }

  @Test
  public void testGeometryLogicalType() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    // Create schema with geometry type
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType("EPSG:4326"))
        .named("geomField")
        .named("Message");

    // Convert to parquet schema and back
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);

    // Verify the logical type is preserved
    assertEquals(schema, actual);

    PrimitiveType primitiveType = actual.getType("geomField").asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    assertTrue(logicalType instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation);
    assertEquals("EPSG:4326", ((LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) logicalType).getCrs());
  }

  @Test
  public void testGeographyLogicalType() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    // Create schema with geography type
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geographyType("EPSG:4326", EdgeInterpolationAlgorithm.SPHERICAL))
        .named("geogField")
        .named("Message");

    // Convert to parquet schema and back
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);

    // Verify the logical type is preserved
    assertEquals(schema, actual);

    PrimitiveType primitiveType = actual.getType("geogField").asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    assertTrue(logicalType instanceof LogicalTypeAnnotation.GeographyLogicalTypeAnnotation);

    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyType =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) logicalType;
    assertEquals("EPSG:4326", geographyType.getCrs());
    assertEquals(EdgeInterpolationAlgorithm.SPHERICAL, geographyType.getAlgorithm());
  }

  @Test
  public void testGeometryLogicalTypeWithMissingCrs() {
    // Create a Geometry logical type without specifying CRS
    GeometryType geometryType = new GeometryType();
    LogicalType logicalType = new LogicalType.GEOMETRY(geometryType);

    // Convert to LogicalTypeAnnotation
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    assertNotNull("Geometry annotation should not be null", annotation);
    assertTrue(
        "Should be a GeometryLogicalTypeAnnotation",
        annotation instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation);

    LogicalTypeAnnotation.GeometryLogicalTypeAnnotation geometryAnnotation =
        (LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) annotation;

    // Default behavior should use null or empty CRS
    assertNull("CRS should be null or empty when not specified", geometryAnnotation.getCrs());
  }

  @Test
  public void testGeographyLogicalTypeWithMissingParameters() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create a Geography logical type without CRS and algorithm
    GeographyType geographyType = new GeographyType();
    LogicalType logicalType = new LogicalType.GEOGRAPHY(geographyType);

    // Convert to LogicalTypeAnnotation
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    assertNotNull("Geography annotation should not be null", annotation);
    assertTrue(
        "Should be a GeographyLogicalTypeAnnotation",
        annotation instanceof LogicalTypeAnnotation.GeographyLogicalTypeAnnotation);

    // Check that optional parameters are handled correctly
    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyAnnotation =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) annotation;
    assertNull("CRS should be null when not specified", geographyAnnotation.getCrs());
    // Most implementations default to LINEAR when algorithm is not specified
    assertNull("Algorithm should be null when not specified", geographyAnnotation.getAlgorithm());

    // Now test the round-trip conversion
    LogicalType roundTripType = converter.convertToLogicalType(annotation);
    assertTrue("setField should be GEOGRAPHY", roundTripType instanceof LogicalType.GEOGRAPHY);
    assertNull(
        "Round trip CRS should still be null",
        ((LogicalType.GEOGRAPHY) roundTripType).getValue().crs);
    assertNull(
        "Round trip Algorithm should be null",
        ((LogicalType.GEOGRAPHY) roundTripType).getValue().algorithm);
  }

  @Test
  public void testGeographyLogicalTypeWithAlgorithmButNoCrs() {
    // Create a Geography logical type with algorithm but no CRS
    GeographyType geographyType = new GeographyType();
    geographyType.algorithm = org.apache.parquet.format.EdgeInterpolationAlgorithm.SPHERICAL;
    LogicalType logicalType = new LogicalType.GEOGRAPHY(geographyType);

    // Convert to LogicalTypeAnnotation
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    Assert.assertNotNull("Geography annotation should not be null", annotation);
    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyAnnotation =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) annotation;

    // CRS should be null/empty but algorithm should be set
    assertNull("CRS should be null or empty", geographyAnnotation.getCrs());
    assertEquals(
        "Algorithm should be SPHERICAL",
        EdgeInterpolationAlgorithm.SPHERICAL,
        geographyAnnotation.getAlgorithm());
  }

  @Test
  public void testGeospatialStatisticsConversion() {
    // Create a ParquetMetadataConverter
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create a valid BoundingBox with all fields set
    org.apache.parquet.column.statistics.geospatial.BoundingBox bbox =
        new org.apache.parquet.column.statistics.geospatial.BoundingBox(
            1.0, 2.0, // xmin, xmax
            3.0, 4.0, // ymin, ymax
            5.0, 6.0, // zmin, zmax
            7.0, 8.0 // mmin, mmax
            );

    // Create GeospatialTypes with some example type values
    Set<Integer> types = new HashSet<>(Arrays.asList(1, 2, 3));
    GeospatialTypes geospatialTypes = new GeospatialTypes(types);

    // Create GeospatialStatistics with the bbox and types
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics origStats =
        new org.apache.parquet.column.statistics.geospatial.GeospatialStatistics(bbox, geospatialTypes);

    // Convert to Thrift format
    GeospatialStatistics thriftStats = converter.toParquetGeospatialStatistics(origStats);

    // Verify conversion to Thrift
    assertNotNull("Thrift GeospatialStatistics should not be null", thriftStats);
    assertNotNull("BoundingBox should not be null", thriftStats.bbox);
    assertNotNull("Geospatial types should not be null", thriftStats.geospatialTypes);

    // Check BoundingBox values
    BoundingBox thriftBbox = thriftStats.bbox;
    assertEquals(1.0, thriftBbox.xmin, 0.0001);
    assertEquals(2.0, thriftBbox.xmax, 0.0001);
    assertEquals(3.0, thriftBbox.ymin, 0.0001);
    assertEquals(4.0, thriftBbox.ymax, 0.0001);
    assertEquals(5.0, thriftBbox.zmin, 0.0001);
    assertEquals(6.0, thriftBbox.zmax, 0.0001);
    assertEquals(7.0, thriftBbox.mmin, 0.0001);
    assertEquals(8.0, thriftBbox.mmax, 0.0001);

    // Check geospatial types
    List<Integer> thriftTypes = thriftStats.geospatialTypes;
    assertEquals(3, thriftTypes.size());
    assertTrue(thriftTypes.contains(1));
    assertTrue(thriftTypes.contains(2));
    assertTrue(thriftTypes.contains(3));

    // Create primitive geometry type for conversion back
    LogicalTypeAnnotation geometryAnnotation = LogicalTypeAnnotation.geometryType("EPSG:4326");
    PrimitiveType geometryType =
        Types.required(PrimitiveTypeName.BINARY).as(geometryAnnotation).named("geometry");

    // Convert back from Thrift format
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics convertedStats =
        ParquetMetadataConverter.fromParquetStatistics(thriftStats, geometryType);

    // Verify conversion from Thrift
    assertNotNull("Converted GeospatialStatistics should not be null", convertedStats);
    assertNotNull("BoundingBox should not be null", convertedStats.getBoundingBox());
    assertNotNull("GeospatialTypes should not be null", convertedStats.getGeospatialTypes());

    // Check BoundingBox values
    org.apache.parquet.column.statistics.geospatial.BoundingBox convertedBbox = convertedStats.getBoundingBox();
    assertEquals(1.0, convertedBbox.getXMin(), 0.0001);
    assertEquals(2.0, convertedBbox.getXMax(), 0.0001);
    assertEquals(3.0, convertedBbox.getYMin(), 0.0001);
    assertEquals(4.0, convertedBbox.getYMax(), 0.0001);
    assertEquals(5.0, convertedBbox.getZMin(), 0.0001);
    assertEquals(6.0, convertedBbox.getZMax(), 0.0001);
    assertEquals(7.0, convertedBbox.getMMin(), 0.0001);
    assertEquals(8.0, convertedBbox.getMMax(), 0.0001);

    // Check geospatial types
    Set<Integer> convertedTypes = convertedStats.getGeospatialTypes().getTypes();
    assertEquals(3, convertedTypes.size());
    assertTrue(convertedTypes.contains(1));
    assertTrue(convertedTypes.contains(2));
    assertTrue(convertedTypes.contains(3));
  }

  @Test
  public void testGeospatialStatisticsWithNullBoundingBox() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create GeospatialStatistics with null bbox but valid types
    Set<Integer> types = new HashSet<>(Arrays.asList(1, 2, 3));
    GeospatialTypes geospatialTypes = new GeospatialTypes(types);
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics origStats =
        new org.apache.parquet.column.statistics.geospatial.GeospatialStatistics(null, geospatialTypes);

    // Convert to Thrift format
    GeospatialStatistics thriftStats = converter.toParquetGeospatialStatistics(origStats);

    // Verify conversion to Thrift
    assertNotNull("Thrift GeospatialStatistics should not be null", thriftStats);
    assertNull("BoundingBox should be null", thriftStats.bbox);
    assertNotNull("Geospatial types should not be null", thriftStats.geospatialTypes);

    // Create primitive geometry type for conversion back
    LogicalTypeAnnotation geometryAnnotation = LogicalTypeAnnotation.geometryType("EPSG:4326");
    PrimitiveType geometryType =
        Types.required(PrimitiveTypeName.BINARY).as(geometryAnnotation).named("geometry");

    // Convert back from Thrift format
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics convertedStats =
        ParquetMetadataConverter.fromParquetStatistics(thriftStats, geometryType);

    // Verify conversion from Thrift
    assertNotNull("Converted GeospatialStatistics should not be null", convertedStats);
    assertNull("BoundingBox should be null", convertedStats.getBoundingBox());
    assertNotNull("GeospatialTypes should not be null", convertedStats.getGeospatialTypes());
  }

  @Test
  public void testInvalidBoundingBox() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create an invalid BoundingBox with NaN values
    org.apache.parquet.column.statistics.geospatial.BoundingBox invalidBbox =
        new org.apache.parquet.column.statistics.geospatial.BoundingBox(
            Double.NaN,
            2.0, // xmin is NaN (invalid)
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0);

    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics origStats =
        new org.apache.parquet.column.statistics.geospatial.GeospatialStatistics(invalidBbox, null);

    // Convert to Thrift format - should return null for invalid bbox
    GeospatialStatistics thriftStats = converter.toParquetGeospatialStatistics(origStats);
    assertNull("Should return null for invalid BoundingBox", thriftStats);
  }

  @Test
  public void testEdgeInterpolationAlgorithmConversion() {
    // Test conversion from Parquet to Thrift enum
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm parquetAlgo = EdgeInterpolationAlgorithm.SPHERICAL;
    org.apache.parquet.format.EdgeInterpolationAlgorithm thriftAlgo =
        ParquetMetadataConverter.fromParquetEdgeInterpolationAlgorithm(parquetAlgo);

    // convert the Thrift enum to the column schema enum
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm expected =
        org.apache.parquet.column.schema.EdgeInterpolationAlgorithm.SPHERICAL;
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm actual =
        ParquetMetadataConverter.toParquetEdgeInterpolationAlgorithm(thriftAlgo);
    assertEquals(expected, actual);

    // Test with null
    assertNull(ParquetMetadataConverter.fromParquetEdgeInterpolationAlgorithm(null));
    assertNull(ParquetMetadataConverter.toParquetEdgeInterpolationAlgorithm(null));
  }
}
