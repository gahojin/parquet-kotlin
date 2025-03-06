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

import org.apache.parquet.io.InvalidRecordException
import java.util.*

/**
 * Represents the declared type for a field in a schema.
 * The Type object represents both the actual underlying type of the object
 * (eg a primitive or group) as well as its attributes such as whether it is
 * repeated, required, or optional.
 */
abstract class Type {
    /**
     * represents a field ID.

     */
    class ID(
        /** For bean serialization, used by Cascading 3. */
        private val id: Int,
    ) {
        fun intValue(): Int = id

        override fun equals(obj: Any?): Boolean {
            return (obj is ID) && obj.id == id
        }

        override fun hashCode(): Int = id

        override fun toString(): String = id.toString()
    }

    /**
     * Constraint on the repetition of a field
     */
    enum class Repetition {
        /**
         * exactly 1
         */
        REQUIRED {
            override fun isMoreRestrictiveThan(other: Repetition?): Boolean {
                return other !== REQUIRED
            }
        },

        /**
         * 0 or 1
         */
        OPTIONAL {
            override fun isMoreRestrictiveThan(other: Repetition?): Boolean {
                return other === REPEATED
            }
        },

        /**
         * 0 or more
         */
        REPEATED {
            override fun isMoreRestrictiveThan(other: Repetition?): Boolean {
                return false
            }
        };

        /**
         * @param other a repetition to test
         * @return true if it is strictly more restrictive than other
         */
        abstract fun isMoreRestrictiveThan(other: Repetition?): Boolean

        companion object {
            /**
             * @param repetitions repetitions to traverse
             * @return the least restrictive repetition of all repetitions provided
             */
            @JvmStatic
            fun leastRestrictive(vararg repetitions: Repetition?): Repetition {
                var hasOptional = false

                for (repetition in repetitions) {
                    if (repetition === REPEATED) {
                        return REPEATED
                    } else if (repetition === OPTIONAL) {
                        hasOptional = true
                    }
                }

                if (hasOptional) {
                    return OPTIONAL
                }

                return REQUIRED
            }
        }
    }

    /**
     * @return the name of the type
     */
    val name: String

    /**
     * @return the repetition constraint
     */
    val repetition: Repetition
    val logicalTypeAnnotation: LogicalTypeAnnotation?

    /**
     * @return the id of the field (if defined)
     */
    val id: ID?

    /** the original type (LIST, MAP, ...) */
    val originalType: OriginalType?
        get() = logicalTypeAnnotation?.toOriginalType()

    /**
     * @return if this is a primitive type
     */
    abstract val isPrimitive: Boolean

    /**
     * @param name       the name of the type
     * @param repetition OPTIONAL, REPEATED, REQUIRED
     */
    @Deprecated("")
    constructor(name: String?, repetition: Repetition?) : this(name, repetition, null as LogicalTypeAnnotation?, null)

    /**
     * @param name         the name of the type
     * @param repetition   OPTIONAL, REPEATED, REQUIRED
     * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
     */
    @Deprecated("")
    constructor(name: String?, repetition: Repetition?, originalType: OriginalType?) : this(
        name,
        repetition,
        originalType,
        null
    )

    /**
     * @param name         the name of the type
     * @param repetition   OPTIONAL, REPEATED, REQUIRED
     * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
     * @param id           (optional) the id of the fields.
     */
    internal constructor(name: String?, repetition: Repetition?, originalType: OriginalType?, id: ID?) : this(
        name,
        repetition,
        originalType,
        null,
        id
    )

    internal constructor(
        name: String?,
        repetition: Repetition?,
        originalType: OriginalType?,
        decimalMetadata: DecimalMetadata?,
        id: ID?
    ) : super() {
        this.name = Objects.requireNonNull<String>(name, "name cannot be null")
        this.repetition = Objects.requireNonNull<Repetition>(repetition, "repetition cannot be null")
        this.logicalTypeAnnotation =
            if (originalType == null) null else LogicalTypeAnnotation.fromOriginalType(originalType, decimalMetadata)
        this.id = id
    }

    @JvmOverloads
    internal constructor(
        name: String?,
        repetition: Repetition?,
        logicalTypeAnnotation: LogicalTypeAnnotation?,
        id: ID? = null
    ) : super() {
        this.name = Objects.requireNonNull<String>(name, "name cannot be null")
        this.repetition = Objects.requireNonNull<Repetition>(repetition, "repetition cannot be null")
        this.logicalTypeAnnotation = logicalTypeAnnotation
        this.id = id
    }

    /**
     * @param id an integer id
     * @return the same type with the id field set
     */
    abstract fun withId(id: Int): Type

    /**
     * @param rep repetition level to test
     * @return if repetition of the type is rep
     */
    fun isRepetition(rep: Repetition): Boolean {
        return repetition === rep
    }

    /**
     * @return this if it's a group type
     * @throws ClassCastException if not
     */
    fun asGroupType(): GroupType {
        if (this.isPrimitive) {
            throw ClassCastException("$this is not a group")
        }
        return this as GroupType
    }

    /**
     * @return this if it's a primitive type
     * @throws ClassCastException if not
     */
    fun asPrimitiveType(): PrimitiveType {
        if (!this.isPrimitive) {
            throw ClassCastException("$this is not primitive")
        }
        return this as PrimitiveType
    }

    /**
     * Writes a string representation to the provided StringBuilder
     *
     * @param sb     the StringBuilder to write itself to
     * @param indent indentation level
     */
    abstract fun writeToStringBuilder(sb: StringBuilder, indent: String)

    /**
     * Visits this type with the given visitor
     *
     * @param visitor the visitor to visit this type
     */
    abstract fun accept(visitor: TypeVisitor)

    @Deprecated("")
    protected abstract fun typeHashCode(): Int

    @Deprecated("")
    protected abstract fun typeEquals(other: Type): Boolean

    override fun hashCode(): Int {
        var c = repetition.hashCode()
        c = 31 * c + name.hashCode()
        if (logicalTypeAnnotation != null) {
            c = 31 * c + logicalTypeAnnotation.hashCode()
        }
        if (id != null) {
            c = 31 * c + id.hashCode()
        }
        return c
    }

    protected open fun equals(other: Type): Boolean {
        return name == other.name
                && repetition === other.repetition && eqOrBothNull(repetition, other.repetition)
                && eqOrBothNull(id, other.id)
                && eqOrBothNull(logicalTypeAnnotation, other.logicalTypeAnnotation)
    }

    override fun equals(other: Any?): Boolean {
        if (other !is Type) {
            return false
        }
        return equals(other)
    }

    protected fun eqOrBothNull(o1: Any?, o2: Any?): Boolean {
        return (o1 == null && o2 == null) || (o1 != null && o1 == o2)
    }

    protected abstract fun getMaxRepetitionLevel(path: Array<String>, i: Int): Int

    protected abstract fun getMaxDefinitionLevel(path: Array<String>, i: Int): Int

    protected abstract fun getType(path: Array<String>, i: Int): Type

    protected abstract fun getPaths(depth: Int): List<Array<String>>

    protected abstract fun containsPath(path: Array<String>, depth: Int): Boolean

    /**
     * @param toMerge the type to merge into this one
     * @return the union result of merging toMerge into this
     */
    protected abstract fun union(toMerge: Type): Type

    /**
     * @param toMerge the type to merge into this one
     * @param strict  should schema primitive types match
     * @return the union result of merging toMerge into this
     */
    protected abstract fun union(toMerge: Type, strict: Boolean): Type

    override fun toString(): String {
        val sb = StringBuilder()
        writeToStringBuilder(sb, "")
        return sb.toString()
    }

    open fun checkContains(subType: Type) {
        if (name != subType.name || repetition !== subType.repetition) {
            throw InvalidRecordException("$subType found: expected $this")
        }
    }

    /**
     * @param path      a list of groups to convert
     * @param converter logic to convert the tree
     * @param <T>       the type returned by the converter
     * @return the converted tree
     */
    protected abstract fun <T> convert(path: List<@JvmSuppressWildcards GroupType>, converter: TypeConverter<@JvmSuppressWildcards T>): T
}
