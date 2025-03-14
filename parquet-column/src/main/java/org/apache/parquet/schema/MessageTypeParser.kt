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
package org.apache.parquet.schema

import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeToken
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types.GroupBuilder
import java.util.*

/**
 * Parses a schema from a textual format similar to that described in the Dremel paper.
 */
object MessageTypeParser {
    /**
     * @param input the text representation of the schema to parse
     * @return the corresponding object representation
     */
    @JvmStatic
    fun parseMessageType(input: String): MessageType {
        return parse(input)
    }

    private fun parse(schemaString: String): MessageType {
        val st = Tokenizer(schemaString)
        val builder = Types.buildMessage()

        val t = st.nextToken()
        check(t, "message", "start with 'message'", st)
        val name = st.nextToken()
        addGroupTypeFields(st.nextToken(), st, builder)
        return builder.named(name)
    }

    private fun addGroupTypeFields(t: String, st: Tokenizer, builder: GroupBuilder<*>) {
        var t = t
        check(t, "{", "start of message", st)
        while ((st.nextToken().also { t = it }) != "}") {
            addType(t, st, builder)
        }
    }

    private fun addType(t: String, st: Tokenizer, builder: GroupBuilder<*>) {
        val repetition = asRepetition(t, st)

        // Read type.
        val type = st.nextToken()
        if ("group".equals(type, ignoreCase = true)) {
            addGroupType(st, repetition, builder)
        } else {
            addPrimitiveType(st, asPrimitive(type, st), repetition, builder)
        }
    }

    private fun addGroupType(st: Tokenizer, r: Repetition, builder: GroupBuilder<*>) {
        val childBuilder: GroupBuilder<*> = builder.group(r)
        var t: String
        val name = st.nextToken()

        // Read annotation, if any.
        t = st.nextToken()
        var originalType: OriginalType? = null
        if (t.equals("(", ignoreCase = true)) {
            originalType = OriginalType.valueOf(st.nextToken())
            childBuilder.`as`(originalType)
            check(st.nextToken(), ")", "original type ended by )", st)
            t = st.nextToken()
        }
        if (t == "=") {
            childBuilder.id(st.nextToken().toInt())
            t = st.nextToken()
        }
        try {
            addGroupTypeFields(t, st, childBuilder)
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("problem reading type: type = group, name = $name, original type = $originalType", e)
        }

        childBuilder.named(name)
    }

    private fun addPrimitiveType(st: Tokenizer, type: PrimitiveTypeName, r: Repetition, builder: GroupBuilder<*>) {
        val childBuilder: Types.PrimitiveBuilder<*> = builder.primitive(type, r)
        var t: String

        if (type === PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            t = st.nextToken()
            // Read type length if the type is fixed_len_byte_array.
            require(t.equals("(", ignoreCase = true)) { "expecting (length) for field of type fixed_len_byte_array" }
            childBuilder.length(st.nextToken().toInt())
            check(st.nextToken(), ")", "type length ended by )", st)
        }

        val name = st.nextToken()

        // Read annotation, if any.
        t = st.nextToken()
        var originalType: OriginalType? = null
        if (t.equals("(", ignoreCase = true)) {
            t = st.nextToken()
            if (isLogicalType(t)) {
                val logicalType = LogicalTypeToken.valueOf(t)
                t = st.nextToken()
                val tokens = ArrayList<String>()
                if ("(" == t) {
                    while (")" != t) {
                        if (!("," == t || "(" == t)) {
                            tokens.add(t)
                        }
                        t = st.nextToken()
                    }
                    t = st.nextToken()
                }
                val logicalTypeAnnotation = logicalType.fromString(tokens)
                childBuilder.`as`(logicalTypeAnnotation)
            } else {
                // Try to parse as old logical type, called OriginalType
                originalType = OriginalType.valueOf(t)
                childBuilder.`as`(originalType)
                if (OriginalType.DECIMAL == originalType) {
                    t = st.nextToken()
                    // parse precision and scale
                    if (t.equals("(", ignoreCase = true)) {
                        childBuilder.precision(st.nextToken().toInt())
                        t = st.nextToken()
                        if (t.equals(",", ignoreCase = true)) {
                            childBuilder.scale(st.nextToken().toInt())
                            t = st.nextToken()
                        }
                        check(t, ")", "decimal type ended by )", st)
                        t = st.nextToken()
                    }
                } else {
                    t = st.nextToken()
                }
            }
            check(t, ")", "logical type ended by )", st)
            t = st.nextToken()
        }
        if (t == "=") {
            childBuilder.id(st.nextToken().toInt())
            t = st.nextToken()
        }
        check(t, ";", "field ended by ';'", st)

        try {
            childBuilder.named(name)
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("problem reading type: type = $type, name = $name, original type = $originalType", e)
        }
    }

    private fun isLogicalType(t: String): Boolean {
        return LogicalTypeToken.entries.any { it.name == t }
    }

    private fun asPrimitive(t: String, st: Tokenizer): PrimitiveTypeName {
        try {
            return PrimitiveTypeName.valueOf(t.uppercase())
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException(
                "expected one of ${PrimitiveTypeName.entries.toTypedArray().contentToString()} got $t at ${st.locationString}", e)
        }
    }

    private fun asRepetition(t: String, st: Tokenizer): Repetition {
        try {
            return Repetition.valueOf(t.uppercase())
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("expected one of ${Repetition.entries.toTypedArray().contentToString()} got $t at ${st.locationString}", e)
        }
    }

    private fun check(t: String, expected: String, message: String, tokenizer: Tokenizer) {
        require(t.equals(expected, ignoreCase = true)) { "$message: expected '$expected' but got '$t' at ${tokenizer.locationString}" }
    }

    private class Tokenizer(schemaString: String) {
        private val st = StringTokenizer(schemaString, " ,;{}()\n\t=", true)

        private var line = 0
        private val currentLine = StringBuilder()

        val locationString: String
            get() = "line $line: $currentLine"

        fun nextToken(): String {
            while (st.hasMoreTokens()) {
                val t = st.nextToken()
                if (t == "\n") {
                    ++line
                    currentLine.setLength(0)
                } else {
                    currentLine.append(t)
                }
                if (!isWhitespace(t)) {
                    return t
                }
            }
            throw IllegalArgumentException("unexpected end of schema")
        }

        fun isWhitespace(t: String): Boolean {
            return t == " " || t == "\t" || t == "\n"
        }
    }
}
