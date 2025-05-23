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
package org.apache.parquet.parser;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.schema.OriginalType.BSON;
import static org.apache.parquet.schema.OriginalType.DATE;
import static org.apache.parquet.schema.OriginalType.DECIMAL;
import static org.apache.parquet.schema.OriginalType.INTERVAL;
import static org.apache.parquet.schema.OriginalType.INT_16;
import static org.apache.parquet.schema.OriginalType.INT_32;
import static org.apache.parquet.schema.OriginalType.INT_64;
import static org.apache.parquet.schema.OriginalType.INT_8;
import static org.apache.parquet.schema.OriginalType.JSON;
import static org.apache.parquet.schema.OriginalType.LIST;
import static org.apache.parquet.schema.OriginalType.MAP;
import static org.apache.parquet.schema.OriginalType.MAP_KEY_VALUE;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.TIME_MILLIS;
import static org.apache.parquet.schema.OriginalType.UINT_16;
import static org.apache.parquet.schema.OriginalType.UINT_32;
import static org.apache.parquet.schema.OriginalType.UINT_64;
import static org.apache.parquet.schema.OriginalType.UINT_8;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.junit.Assert.assertEquals;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.MessageTypeBuilder;
import org.junit.Test;

public class TestParquetParser {
  @Test
  public void testPaperExample() {
    String example = "message Document {\n" + "  required int64 DocId;\n"
        + "  optional group Links {\n"
        + "    repeated int64 Backward;\n"
        + "    repeated int64 Forward; }\n"
        + "  repeated group Name {\n"
        + "    repeated group Language {\n"
        + "      required binary Code;\n"
        + "      required binary Country; }\n"
        + "    optional binary Url; }}";
    MessageType parsed = parseMessageType(example);
    MessageType manuallyMade = new MessageType(
        "Document",
        new PrimitiveType(REQUIRED, INT64, "DocId"),
        new GroupType(
            OPTIONAL,
            "Links",
            new PrimitiveType(REPEATED, INT64, "Backward"),
            new PrimitiveType(REPEATED, INT64, "Forward")),
        new GroupType(
            REPEATED,
            "Name",
            new GroupType(
                REPEATED,
                "Language",
                new PrimitiveType(REQUIRED, BINARY, "Code"),
                new PrimitiveType(REQUIRED, BINARY, "Country")),
            new PrimitiveType(OPTIONAL, BINARY, "Url")));
    assertEquals(manuallyMade, parsed);

    MessageType parsedThenReparsed = parseMessageType(parsed.toString());

    assertEquals(manuallyMade, parsedThenReparsed);

    parsed = parseMessageType(
        "message m { required group a {required binary b;} required group c { required int64 d; }}");
    manuallyMade = new MessageType(
        "m",
        new GroupType(REQUIRED, "a", new PrimitiveType(REQUIRED, BINARY, "b")),
        new GroupType(REQUIRED, "c", new PrimitiveType(REQUIRED, INT64, "d")));

    assertEquals(manuallyMade, parsed);

    parsedThenReparsed = parseMessageType(parsed.toString());

    assertEquals(manuallyMade, parsedThenReparsed);
  }

  @Test
  public void testEachPrimitiveType() {
    MessageTypeBuilder builder = buildMessage();
    StringBuilder schema = new StringBuilder();
    schema.append("message EachType {\n");
    for (PrimitiveTypeName type : PrimitiveTypeName.values()) {
      // add a schema entry, e.g., "  required int32 int32_;\n"
      if (type == FIXED_LEN_BYTE_ARRAY) {
        schema.append("  required fixed_len_byte_array(3) fixed_;");
        builder.required(FIXED_LEN_BYTE_ARRAY).length(3).named("fixed_");
      } else {
        schema.append("  required ")
            .append(type)
            .append(" ")
            .append(type)
            .append("_;\n");
        builder.required(type).named(type.toString() + "_");
      }
    }
    schema.append("}\n");
    MessageType expected = builder.named("EachType");

    MessageType parsed = parseMessageType(schema.toString());

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testSTRINGAnnotation() {
    String message = "message StringMessage {\n" + "  required binary string (STRING);\n" + "}\n";

    MessageType parsed = parseMessageType(message);
    MessageType expected =
        buildMessage().required(BINARY).as(stringType()).named("string").named("StringMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testUTF8Annotation() {
    String message = "message StringMessage {\n" + "  required binary string (UTF8);\n" + "}\n";

    MessageType parsed = parseMessageType(message);
    MessageType expected =
        buildMessage().required(BINARY).as(UTF8).named("string").named("StringMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testIDs() {
    String message = "message Message {\n" + "  required binary string (UTF8) = 6;\n"
        + "  required int32 i=1;\n"
        + "  required binary s2= 3;\n"
        + "  required binary s3 =4;\n"
        + "}\n";

    MessageType parsed = parseMessageType(message);
    MessageType expected = buildMessage()
        .required(BINARY)
        .as(OriginalType.UTF8)
        .id(6)
        .named("string")
        .required(INT32)
        .id(1)
        .named("i")
        .required(BINARY)
        .id(3)
        .named("s2")
        .required(BINARY)
        .id(4)
        .named("s3")
        .named("Message");

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testMAPAnnotations() {
    // this is primarily to test group annotations
    String message = "message Message {\n" + "  optional group aMap (MAP) {\n"
        + "    repeated group map (MAP_KEY_VALUE) {\n"
        + "      required binary key (UTF8);\n"
        + "      required int32 value;\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    MessageType parsed = parseMessageType(message);
    MessageType expected = buildMessage()
        .optionalGroup()
        .as(MAP)
        .repeatedGroup()
        .as(MAP_KEY_VALUE)
        .required(BINARY)
        .as(UTF8)
        .named("key")
        .required(INT32)
        .named("value")
        .named("map")
        .named("aMap")
        .named("Message");

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testLISTAnnotation() {
    // this is primarily to test group annotations
    String message = "message Message {\n" + "  required group aList (LIST) {\n"
        + "    repeated binary string (UTF8);\n"
        + "  }\n"
        + "}\n";

    MessageType parsed = parseMessageType(message);
    MessageType expected = buildMessage()
        .requiredGroup()
        .as(LIST)
        .repeated(BINARY)
        .as(UTF8)
        .named("string")
        .named("aList")
        .named("Message");

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testDecimalFixedAnnotation() {
    String message =
        "message DecimalMessage {\n" + "  required FIXED_LEN_BYTE_ARRAY(4) aDecimal (DECIMAL(9,2));\n" + "}\n";

    MessageType parsed = parseMessageType(message);
    MessageType expected = buildMessage()
        .required(FIXED_LEN_BYTE_ARRAY)
        .length(4)
        .as(DECIMAL)
        .precision(9)
        .scale(2)
        .named("aDecimal")
        .named("DecimalMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testDecimalBinaryAnnotation() {
    String message = "message DecimalMessage {\n" + "  required binary aDecimal (DECIMAL(9,2));\n" + "}\n";

    MessageType parsed = parseMessageType(message);
    MessageType expected = buildMessage()
        .required(BINARY)
        .as(DECIMAL)
        .precision(9)
        .scale(2)
        .named("aDecimal")
        .named("DecimalMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testTimeAnnotations() {
    String message = "message TimeMessage {" + "  required int32 date (DATE);"
        + "  required int32 time (TIME_MILLIS);"
        + "  required int64 timestamp (TIMESTAMP_MILLIS);"
        + "  required FIXED_LEN_BYTE_ARRAY(12) interval (INTERVAL);"
        + "  required int32 newTime (TIME(MILLIS,true));"
        + "  required int64 nanoTime (TIME(NANOS,true));"
        + "  required int64 newTimestamp (TIMESTAMP(MILLIS,false));"
        + "  required int64 nanoTimestamp (TIMESTAMP(NANOS,false));"
        + "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .required(INT32)
        .as(DATE)
        .named("date")
        .required(INT32)
        .as(TIME_MILLIS)
        .named("time")
        .required(INT64)
        .as(TIMESTAMP_MILLIS)
        .named("timestamp")
        .required(FIXED_LEN_BYTE_ARRAY)
        .length(12)
        .as(INTERVAL)
        .named("interval")
        .required(INT32)
        .as(timeType(true, MILLIS))
        .named("newTime")
        .required(INT64)
        .as(timeType(true, NANOS))
        .named("nanoTime")
        .required(INT64)
        .as(timestampType(false, MILLIS))
        .named("newTimestamp")
        .required(INT64)
        .as(timestampType(false, NANOS))
        .named("nanoTimestamp")
        .named("TimeMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testIntAnnotations() {
    String message = "message IntMessage {" + "  required int32 i8 (INT_8);"
        + "  required int32 i16 (INT_16);"
        + "  required int32 i32 (INT_32);"
        + "  required int64 i64 (INT_64);"
        + "  required int32 u8 (UINT_8);"
        + "  required int32 u16 (UINT_16);"
        + "  required int32 u32 (UINT_32);"
        + "  required int64 u64 (UINT_64);"
        + "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .required(INT32)
        .as(INT_8)
        .named("i8")
        .required(INT32)
        .as(INT_16)
        .named("i16")
        .required(INT32)
        .as(INT_32)
        .named("i32")
        .required(INT64)
        .as(INT_64)
        .named("i64")
        .required(INT32)
        .as(UINT_8)
        .named("u8")
        .required(INT32)
        .as(UINT_16)
        .named("u16")
        .required(INT32)
        .as(UINT_32)
        .named("u32")
        .required(INT64)
        .as(UINT_64)
        .named("u64")
        .named("IntMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testIntegerAnnotations() {
    String message = "message IntMessage {" + "  required int32 i8 (INTEGER(8,true));"
        + "  required int32 i16 (INTEGER(16,true));"
        + "  required int32 i32 (INTEGER(32,true));"
        + "  required int64 i64 (INTEGER(64,true));"
        + "  required int32 u8 (INTEGER(8,false));"
        + "  required int32 u16 (INTEGER(16,false));"
        + "  required int32 u32 (INTEGER(32,false));"
        + "  required int64 u64 (INTEGER(64,false));"
        + "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .required(INT32)
        .as(intType(8, true))
        .named("i8")
        .required(INT32)
        .as(intType(16, true))
        .named("i16")
        .required(INT32)
        .as(intType(32, true))
        .named("i32")
        .required(INT64)
        .as(intType(64, true))
        .named("i64")
        .required(INT32)
        .as(intType(8, false))
        .named("u8")
        .required(INT32)
        .as(intType(16, false))
        .named("u16")
        .required(INT32)
        .as(intType(32, false))
        .named("u32")
        .required(INT64)
        .as(intType(64, false))
        .named("u64")
        .named("IntMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testEmbeddedAnnotations() {
    String message = "message EmbeddedMessage {" + "  required binary json (JSON);"
        + "  required binary bson (BSON);"
        + "}\n";

    MessageType parsed = MessageTypeParser.parseMessageType(message);
    MessageType expected = Types.buildMessage()
        .required(BINARY)
        .as(JSON)
        .named("json")
        .required(BINARY)
        .as(BSON)
        .named("bson")
        .named("EmbeddedMessage");

    assertEquals(expected, parsed);
    MessageType reparsed = MessageTypeParser.parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }

  @Test
  public void testVARIANTAnnotation() {
    String message = "message Message {\n"
            + "  required group aVariant (VARIANT(2)) {\n"
            + "     required binary metadata;\n"
            + "     required binary value;\n"
            + "  }\n"
            + "}\n";

    MessageType expected = buildMessage()
            .requiredGroup()
            .as(LogicalTypeAnnotation.variantType((byte) 2))
            .required(BINARY)
            .named("metadata")
            .required(BINARY)
            .named("value")
            .named("aVariant")
            .named("Message");

    MessageType parsed = parseMessageType(message);

    assertEquals(expected, parsed);
    MessageType reparsed = parseMessageType(parsed.toString());
    assertEquals(expected, reparsed);
  }
}
