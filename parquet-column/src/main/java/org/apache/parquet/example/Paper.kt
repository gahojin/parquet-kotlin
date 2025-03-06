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
package org.apache.parquet.example

import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

/**
 * Examples from the Dremel Paper
 */
object Paper {
    @JvmField
    val schema: MessageType = MessageType(
        "Document",
        PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "DocId"),
        GroupType(
            Repetition.OPTIONAL,
            "Links",
            PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "Backward"),
            PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "Forward"),
        ),
        GroupType(
            Repetition.REPEATED,
            "Name",
            GroupType(
                Repetition.REPEATED,
                "Language",
                PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "Code"),
                PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "Country"),
            ),
            PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "Url"),
        ),
    )

    @JvmField
    val schema2: MessageType = MessageType(
        "Document",
        PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "DocId"),
        GroupType(
            Repetition.REPEATED,
            "Name",
            GroupType(
                Repetition.REPEATED,
                "Language",
                PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "Country"),
            ),
        ),
    )

    @JvmField
    val schema3: MessageType = MessageType(
        "Document",
        PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.INT64, "DocId"),
        GroupType(
            Repetition.OPTIONAL,
            "Links",
            PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "Backward"),
            PrimitiveType(Repetition.REPEATED, PrimitiveTypeName.INT64, "Forward"),
        ),
    )

    @JvmField
    val r1: SimpleGroup = SimpleGroup(schema)
    @JvmField
    val r2: SimpleGroup = SimpleGroup(schema)

    //// r1
    // DocId: 10
    // Links
    //  Forward: 20
    //  Forward: 40
    //  Forward: 60
    // Name
    //  Language
    //    Code: 'en-us'
    //    Country: 'us'
    //  Language
    //    Code: 'en'
    //  Url: 'http://A'
    // Name
    //  Url: 'http://B'
    // Name
    //  Language
    //    Code: 'en-gb'
    //    Country: 'gb'
    init {
        r1.add("DocId", 10L)
        r1.addGroup("Links").append("Forward", 20L).append("Forward", 40L).append("Forward", 60L)
        var name = r1.addGroup("Name").apply {
            addGroup("Language").append("Code", "en-us").append("Country", "us")
            addGroup("Language").append("Code", "en")
            append("Url", "http://A")
        }
        name = r1.addGroup("Name").apply {
            append("Url", "http://B")
        }
        name = r1.addGroup("Name").apply {
            addGroup("Language").append("Code", "en-gb").append("Country", "gb")
        }
    }

    //// r2
    // DocId: 20
    // Links
    // Backward: 10
    // Backward: 30
    // Forward:  80
    // Name
    // Url: 'http://C'
    init {
        r2.add("DocId", 20L)
        r2.addGroup("Links").append("Backward", 10L).append("Backward", 30L).append("Forward", 80L)
        r2.addGroup("Name").append("Url", "http://C")
    }

    @JvmField
    val pr1: SimpleGroup = SimpleGroup(schema2)
    @JvmField
    val pr2: SimpleGroup = SimpleGroup(schema2)

    //// r1
    // DocId: 10
    // Name
    //  Language
    //    Country: 'us'
    //  Language
    // Name
    // Name
    //  Language
    //    Country: 'gb'
    init {
        pr1.add("DocId", 10L)
        var name = pr1.addGroup("Name").apply {
            addGroup("Language").append("Country", "us")
            addGroup("Language")
        }
        name = pr1.addGroup("Name")
        name = pr1.addGroup("Name").apply {
            addGroup("Language").append("Country", "gb")
        }
    }

    //// r2
    // DocId: 20
    // Name
    init {
        pr2.add("DocId", 20L)
        pr2.addGroup("Name")
    }
}
