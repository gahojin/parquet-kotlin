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
package org.apache.parquet.filter2

import com.squareup.kotlinpoet.*
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import kotlin.io.path.Path

object IncrementallyUpdatedFilterPredicateGenerator {
    private const val BASE_PACKAGE_NAME = "org.apache.parquet"
    private const val PACKAGE_NAME = "${BASE_PACKAGE_NAME}.filter2.recordlevel"
    private const val PREDICATE_PACKAGE_NAME = "${BASE_PACKAGE_NAME}.filter2.predicate"
    private const val CLASS_NAME: String = "IncrementallyUpdatedFilterPredicateBuilder"

    private val containsPredicateType = ClassName(PACKAGE_NAME, CLASS_NAME, "ContainsPredicate")
    private val binaryType = ClassName("${BASE_PACKAGE_NAME}.io.api", "Binary")
    private val primitiveColumnIOClassName = ClassName("${BASE_PACKAGE_NAME}.io", "PrimitiveColumnIO")
    private val incrementallyUpdatedFilterPredicateType = ClassName(PACKAGE_NAME, "IncrementallyUpdatedFilterPredicate")
    private val valueInspectorType = incrementallyUpdatedFilterPredicateType.nestedClass("ValueInspector")
    private val delegatingValueInspectorType = incrementallyUpdatedFilterPredicateType.nestedClass("DelegatingValueInspector")
    private val filterPredicateType = ClassName(PREDICATE_PACKAGE_NAME, "FilterPredicate")
    private val operatorsType = ClassName(PREDICATE_PACKAGE_NAME, "Operators")
    private val containsType = operatorsType.nestedClass("Contains")
    private val userDefinedType = operatorsType.nestedClass("UserDefined")
    private val logicalNotUserDefinedType = operatorsType.nestedClass("LogicalNotUserDefined")
    private val userDefinedPredicateType = ClassName(PREDICATE_PACKAGE_NAME, "UserDefinedPredicate")

    private class TypeInfo(
        val className: String,
        val primitiveName: TypeName,
        val supportsInequality: Boolean,
    )

    private val TYPES = listOf(
        TypeInfo("Integer", INT, true),
        TypeInfo("java.lang.Long", LONG, true),
        TypeInfo("java.lang.Boolean", BOOLEAN, false),
        TypeInfo("java.lang.Float", FLOAT, true),
        TypeInfo("java.lang.Double", DOUBLE, true),
        TypeInfo("Binary", binaryType, true),
    )

    fun main(args: Array<String>) {
        val basePath = Path(args[0])

        val t = TypeVariableName("T")
        val comparableT = TypeVariableName("T", COMPARABLE.parameterizedBy(t))
        val userDefinedPredicateU = TypeVariableName("U", userDefinedPredicateType.parameterizedBy(t))

        val schemaClass = TypeSpec.classBuilder(CLASS_NAME)
            .superclass(ClassName(PACKAGE_NAME, "IncrementallyUpdatedFilterPredicateBuilderBase"))
            .primaryConstructor(
                FunSpec.constructorBuilder()
                    .addParameter("leaves", LIST.parameterizedBy(primitiveColumnIOClassName))
                    .build())
            .addSuperclassConstructorParameter("leaves")
            .addVisitBegin(operatorsType.nestedClass("Eq"), TYPES) {
                addEqNotEqCase(it, isEq = true, false)
            }
            .addVisitBegin(operatorsType.nestedClass("NotEq"), TYPES) {
                addEqNotEqCase(it, isEq = false, false)
            }
            .addVisitBegin(operatorsType.nestedClass("In"), TYPES) {
                addInNotInCase(it, isEq = true, false)
            }
            .addVisitBegin(operatorsType.nestedClass("NotIn"), TYPES) {
                addInNotInCase(it, isEq = false, false)
            }
            .addContainsBegin {
                addVisitBegin(operatorsType.nestedClass("Contains")) {
                    addContainsCase()
                }
            }
            .addVisitBegin(operatorsType.nestedClass("Lt"), TYPES) {
                addInequalityCase(it, "<", false)
            }
            .addVisitBegin(operatorsType.nestedClass("LtEq"), TYPES) {
                addInequalityCase(it, "<=", false)
            }
            .addVisitBegin(operatorsType.nestedClass("Gt"), TYPES) {
                addInequalityCase(it, ">", false)
            }
            .addVisitBegin(operatorsType.nestedClass("GtEq"), TYPES) {
                addInequalityCase(it, ">=", false)
            }
            .addFunction(FunSpec.builder("visit")
                .addTypeVariable(comparableT)
                .addTypeVariable(userDefinedPredicateU)
                .addModifiers(KModifier.OVERRIDE)
                .returns(incrementallyUpdatedFilterPredicateType)
                .addParameter(ParameterSpec.builder("pred", userDefinedType.parameterizedBy(comparableT, userDefinedPredicateU))
                    .build())
                .addUdpBegin {
                    for (info in TYPES) {
                        addUdpCase(info, false)
                    }
                }
                .build())
            .addFunction(FunSpec.builder("visit")
                .addTypeVariable(comparableT)
                .addTypeVariable(userDefinedPredicateU)
                .addModifiers(KModifier.OVERRIDE)
                .returns(incrementallyUpdatedFilterPredicateType)
                .addParameter(ParameterSpec.builder("notPred", logicalNotUserDefinedType.parameterizedBy(comparableT, userDefinedPredicateU))
                    .build())
                .addStatement("val pred = notPred.userDefined")
                .addUdpBegin {
                    for (info in TYPES) {
                        addUdpCase(info, true)
                    }
                }
                .build())
            .build()

        val schemaFile = FileSpec.builder(PACKAGE_NAME, CLASS_NAME)
            .addFileComment("This class is auto-generated by ${PACKAGE_NAME}.${CLASS_NAME}\nDo not manually edit!")
            .addType(schemaClass)
            .addAnnotation(AnnotationSpec.builder(Suppress::class)
                .addMember("%S, %S", "UNCHECKED_CAST", "RedundantVisibilityModifier")
                .build())
            .build()

        schemaFile.writeTo(basePath)
    }

    private fun TypeSpec.Builder.addVisitBegin(inVar: ClassName, types: List<TypeInfo>, block: FunSpec.Builder.(TypeInfo) -> Unit) = apply {
        val t = TypeVariableName("T")
        val comparableT = TypeVariableName("T", COMPARABLE.parameterizedBy(t))

        addFunction(FunSpec.builder("visit")
            .addModifiers(KModifier.OVERRIDE)
            .addTypeVariable(comparableT)
            .addParameter("pred", inVar.parameterizedBy(t))
            .returns(incrementallyUpdatedFilterPredicateType)
            .addCode("""
                |val columnPath = pred.column.columnPath
                |""".trimMargin())
            .apply {
                if (types.isNotEmpty()) {
                    beginControlFlow("""
                        |val clazz = pred.column.columnType
                        |val valueInspector = when (clazz)""".trimMargin(), valueInspectorType)
                    types.forEach { block(it) }
                    addCode("""
                        |else -> throw IllegalArgumentException("Encountered unknown type ${'$'}clazz")
                        |""".trimMargin())
                    endControlFlow()
                }
            }
            .addVisitEnd()
            .build())
    }

    private fun TypeSpec.Builder.addVisitBegin(inVar: ClassName, block: FunSpec.Builder.() -> Unit) = apply {
        val t = TypeVariableName("T")
        val comparableT = TypeVariableName("T", COMPARABLE.parameterizedBy(t))

        addFunction(FunSpec.builder("visit")
            .addModifiers(KModifier.OVERRIDE)
            .addTypeVariable(comparableT)
            .addParameter("pred", inVar.parameterizedBy(t))
            .returns(incrementallyUpdatedFilterPredicateType)
            .addCode("""
                |val columnPath = pred.column.columnPath
                |""".trimMargin())
            .apply(block)
            .addCode("""
                |addValueInspector(columnPath, valueInspector)
                |return valueInspector
                |""".trimMargin())
            .build())
    }

    private fun FunSpec.Builder.addVisitEnd() = apply {
        addCode("""
            |addValueInspector(columnPath, valueInspector)
            |return valueInspector
            |""".trimMargin())
    }

    private fun FunSpec.Builder.addEqNotEqCase(info: TypeInfo, isEq: Boolean, expectMultipleResults: Boolean) = apply {
        beginControlFlow("%L::class.java ->", info.className)

        // Predicates for repeated fields don't need to support null values
        if (expectMultipleResults) {
            addCode("""
                |val target = pred.value as %L
                |""".trimMargin(), info.className)
        } else {
            beginControlFlow("(pred.value as? %L)?.let { target ->", info.className)
        }
        addCode("""
            |val comparator = getComparator<%L>(columnPath)
            |%L
            |""".trimMargin(), info.className, TypeSpec.anonymousClassBuilder()
                .superclass(valueInspectorType)
                .addFunction(FunSpec.builder("updateNull")
                    .addModifiers(KModifier.OVERRIDE)
                    .addStatement("setResult(%L)", !isEq)
                    .build())
                .addFunction(FunSpec.builder("update")
                    .addParameter("value", info.primitiveName)
                    .addModifiers(KModifier.OVERRIDE)
                    .apply {
                        if (expectMultipleResults) {
                            addCode("if (!isKnown && ${compareEquality(info, "target", isEq)}) { setResult(true) }")
                        } else {
                            addCode("setResult(${compareEquality(info, "target", isEq)})")
                        }
                    }
                    .build())
                .build())

        if (!expectMultipleResults) {
            nextControlFlow("?: run")

            addCode("%L", TypeSpec.anonymousClassBuilder()
                .superclass(valueInspectorType)
                .addFunction(FunSpec.builder("updateNull")
                    .addModifiers(KModifier.OVERRIDE)
                    .addStatement("setResult(${isEq})")
                    .build())
                .addFunction(FunSpec.builder("update")
                    .addModifiers(KModifier.OVERRIDE)
                    .addParameter("value", info.primitiveName)
                    .addStatement("setResult(${!isEq})")
                    .build())
                .build())
            endControlFlow()
        }

        endControlFlow()
    }

    private fun FunSpec.Builder.addInequalityCase(info: TypeInfo, op: String, expectMultipleResults: Boolean) {
        if (!info.supportsInequality) {
            beginControlFlow("%L::class.java ->", info.className)
            addStatement("throw IllegalArgumentException(\"Operator $op not supported for %L\")", info.className)
            endControlFlow()
            return
        }

        beginControlFlow("%L::class.java ->", info.className)
        addCode("""
            |val target = pred.value as %L
            |val comparator = getComparator<%L>(columnPath)
            |%L
            |""".trimMargin(), info.className, info.className, TypeSpec.anonymousClassBuilder()
                .superclass(valueInspectorType)
                .addFunction(FunSpec.builder("updateNull")
                    .addModifiers(KModifier.OVERRIDE)
                    .addStatement("setResult(false)")
                    .build())
                .addFunction(FunSpec.builder("update")
                    .addModifiers(KModifier.OVERRIDE)
                    .addParameter("value", info.primitiveName)
                    .apply {
                        if (expectMultipleResults) {
                            addCode("if (!isKnown && ${compareOp(info, "target", op)}) { setResult(true) }")
                        } else {
                            addCode("setResult(${compareOp(info, "target", op)})")
                        }
                    }
                    .build())
               .build())
        endControlFlow()
    }

    private fun FunSpec.Builder.addInNotInCase(info: TypeInfo, isEq: Boolean, expectMultipleResults: Boolean) {
        beginControlFlow("%L::class.java ->", info.className)
        beginControlFlow("if (pred.values.contains(null))")
            .addCode("%L\n", TypeSpec.anonymousClassBuilder()
                .superclass(valueInspectorType)
                .addFunction(FunSpec.builder("updateNull")
                    .addModifiers(KModifier.OVERRIDE)
                    .addStatement("setResult($isEq)")
                    .build())
                .addFunction(FunSpec.builder("update")
                    .addModifiers(KModifier.OVERRIDE)
                    .addParameter("value", info.primitiveName)
                    .addStatement("setResult(${!isEq})")
                    .build())
                .build())
            .nextControlFlow("else")
                .addCode("""
                    |val target = pred.values as Set<%L>
                    |val comparator = getComparator<%L>(columnPath)
                    |%L
                    |""".trimMargin(), info.className, info.className, TypeSpec.anonymousClassBuilder()
                        .superclass(valueInspectorType)
                        .addFunction(FunSpec.builder("updateNull")
                            .addModifiers(KModifier.OVERRIDE)
                            .addStatement("setResult(${!isEq})")
                            .build())
                        .addFunction(FunSpec.builder("update")
                            .addModifiers(KModifier.OVERRIDE)
                            .addParameter("value", info.primitiveName)
                            .apply {
                                if (expectMultipleResults) {
                                    addStatement("if (isKnown) return")
                                }
                                beginControlFlow("for (i in target)")
                                    .beginControlFlow("if (${compareEquality(info, "i", isEq)})")
                                        .addStatement("setResult(true)")
                                        .addStatement("return")
                                        .endControlFlow()
                                    .endControlFlow()
                                if (!expectMultipleResults) {
                                    addCode("setResult(false)")
                                }
                            }
                            .build())
                        .build())
                .endControlFlow()
            .endControlFlow()
    }

    private fun FunSpec.Builder.addUdpBegin(block: FunSpec.Builder.() -> Unit) = apply {
        beginControlFlow("""
            |val columnPath = pred.column.columnPath
            |val clazz = pred.column.columnType
            |val udp = pred.userDefinedPredicate
            |val valueInspector = when (clazz)""".trimMargin(), valueInspectorType)
        block()
        addCode("""
            |else -> throw IllegalArgumentException("Encountered unknown type ${'$'}clazz")
            |""".trimMargin())
        endControlFlow()
        addVisitEnd()
    }

    private fun TypeSpec.Builder.addContainsInspectorVisitor(op: ClassName, block: FunSpec.Builder.(TypeInfo) -> Unit) = apply {
        val t = TypeVariableName("T")
        val comparableType = TypeVariableName("T", COMPARABLE.parameterizedBy(t))

        addFunction(FunSpec.builder("visit")
            .addModifiers(KModifier.OVERRIDE)
            .addTypeVariable(comparableType)
            .addParameter("pred", op.parameterizedBy(t))
            .returns(containsPredicateType)
            .beginControlFlow("""
                |val columnPath = pred.column.columnPath
                |val clazz = pred.column.columnType
                |val valueInspector = when (clazz)""".trimMargin(), valueInspectorType)
            .apply {
                for (info in TYPES) {
                    block(info)
                }
            }
            .addCode("""
                |else -> throw IllegalArgumentException("Encountered unknown type ${'$'}clazz")
                |""".trimMargin())
            .endControlFlow()
            .addStatement("return ContainsSinglePredicate(valueInspector, false)")
            .build())
    }

    private fun TypeSpec.Builder.addContainsBegin(block: TypeSpec.Builder.() -> TypeSpec.Builder) = apply {
        val containsPredicateClass = TypeSpec.classBuilder("ContainsPredicate")
            .addModifiers(KModifier.ABSTRACT)
            .superclass(delegatingValueInspectorType)
            .primaryConstructor(FunSpec.constructorBuilder()
                .addParameter("delegates", valueInspectorType, KModifier.VARARG)
                .build())
            .addSuperclassConstructorParameter("*delegates")
            .addFunction(FunSpec.builder("not")
                .addModifiers(KModifier.ABSTRACT)
                .returns(containsPredicateType)
                .build())
            .build()

        val containsSinglePredicateClass = TypeSpec.classBuilder("ContainsSinglePredicate")
            .superclass(containsPredicateType)
            .addSuperclassConstructorParameter("inspector")
            .primaryConstructor(FunSpec.constructorBuilder()
                .addParameter("inspector", valueInspectorType)
                .addParameter("isNot", BOOLEAN)
                .build())
            .addProperty(PropertySpec.builder("isNot", BOOLEAN)
                .initializer("isNot")
                .addModifiers(KModifier.PRIVATE)
                .build())
            .addFunction(FunSpec.builder("not")
                .addModifiers(KModifier.OVERRIDE)
                .returns(containsPredicateType)
                .addStatement("return ContainsSinglePredicate(delegates.iterator().next(), true)")
                .build())
            .addFunction(FunSpec.builder("onUpdate")
                .addModifiers(KModifier.OVERRIDE)
                .addStatement("if (isKnown) return")
                .beginControlFlow("for (delegate in delegates)")
                    .beginControlFlow("if (delegate.isKnown && delegate.result)")
                        .addStatement("setResult(!isNot)")
                        .addStatement("return")
                        .endControlFlow()
                    .endControlFlow()
                .build())
            .addFunction(FunSpec.builder("onNull")
                .addModifiers(KModifier.OVERRIDE)
                .addStatement("setResult(isNot)")
                .build())
            .build()

        val containsAndPredicateClass = TypeSpec.classBuilder("ContainsAndPredicate")
            .superclass(containsPredicateType)
            .primaryConstructor(
                FunSpec.constructorBuilder()
                    .addParameter("left", valueInspectorType)
                    .addParameter("right", valueInspectorType)
                    .build())
            .addSuperclassConstructorParameter("left")
            .addSuperclassConstructorParameter("right")
            .addFunction(FunSpec.builder("not")
                .addModifiers(KModifier.OVERRIDE)
                .returns(containsPredicateType)
                .addStatement("return ContainsAndPredicate((delegates.iterator().next() as %T).not(), (delegates.iterator().next() as %T).not())",
                    containsPredicateType, containsPredicateType)
                .build())
            .addFunction(FunSpec.builder("onUpdate")
                .addModifiers(KModifier.OVERRIDE)
                .addStatement("if (isKnown) return")
                .addStatement("var allKnown = true")
                .beginControlFlow("for (delegate in delegates)")
                    .beginControlFlow("if (delegate.isKnown && !delegate.result)")
                        .addStatement("setResult(false)")
                        .addStatement("return")
                        .endControlFlow()
                    .addStatement("allKnown = allKnown && delegate.isKnown")
                    .endControlFlow()
                .beginControlFlow("if (allKnown)")
                    .addStatement("setResult(true)")
                    .endControlFlow()
                .build())
            .addFunction(FunSpec.builder("onNull")
                .addModifiers(KModifier.OVERRIDE)
                .beginControlFlow("for (delegate in delegates)")
                    .beginControlFlow("if (!delegate.result)")
                        .addStatement("setResult(false)")
                        .addStatement("return")
                        .endControlFlow()
                    .endControlFlow()
                .addStatement("setResult(true)")
                .build())
            .build()

        val containsOrPredicateClass = TypeSpec.classBuilder("ContainsOrPredicate")
            .superclass(containsPredicateType)
            .primaryConstructor(FunSpec.constructorBuilder()
                .addParameter("left", valueInspectorType)
                .addParameter("right", valueInspectorType)
                .build())
            .addSuperclassConstructorParameter("left")
            .addSuperclassConstructorParameter("right")
            .addFunction(FunSpec.builder("not")
                .addModifiers(KModifier.OVERRIDE)
                .returns(containsPredicateType)
                .addStatement("return ContainsOrPredicate((delegates.iterator().next() as %T).not(), (delegates.iterator().next() as %T).not())",
                    containsPredicateType, containsPredicateType)
                .build())
            .addFunction(FunSpec.builder("onUpdate")
                .addModifiers(KModifier.OVERRIDE)
                .addStatement("if (isKnown) return")
                .beginControlFlow("for (delegate in delegates)")
                    .beginControlFlow("if (delegate.isKnown && delegate.result)")
                        .addStatement("setResult(true)")
                        .endControlFlow()
                    .endControlFlow()
                .build())
            .addFunction(FunSpec.builder("onNull")
                .addModifiers(KModifier.OVERRIDE)
                .beginControlFlow("for (delegate in delegates)")
                    .beginControlFlow("if (delegate.result)")
                        .addStatement("setResult(true)")
                        .addStatement("return")
                        .endControlFlow()
                    .endControlFlow()
                .addStatement("setResult(false)")
                .build())
            .build()

        val t = TypeVariableName("T")
        val comparableT = TypeVariableName("T", COMPARABLE.parameterizedBy(t))
        val containsT = containsType.parameterizedBy(t)
        val userDefinedPredicateU = TypeVariableName("U", userDefinedPredicateType.parameterizedBy(t))

        val containsInspectorVisitorClass = TypeSpec.classBuilder("ContainsInspectorVisitor")
            .addModifiers(KModifier.PRIVATE, KModifier.INNER)
            .addSuperinterface(filterPredicateType.nestedClass("Visitor").parameterizedBy(containsPredicateType))
            .addFunction(FunSpec.builder("visit")
                .addModifiers(KModifier.OVERRIDE)
                .addParameter("contains", containsT)
                .addTypeVariable(comparableT)
                .returns(containsPredicateType)
                .addStatement("return contains.filter(this, { l, r -> ContainsAndPredicate(l, r) }, { l, r -> ContainsOrPredicate(l, r) }, { it.not() })")
                .build())
            .addContainsInspectorVisitor(operatorsType.nestedClass("Eq")) {
                addEqNotEqCase(it, isEq = true, true)
            }
            .addContainsInspectorVisitor(operatorsType.nestedClass("NotEq")) {
                addEqNotEqCase(it, isEq = false, true)
            }
            .addContainsInspectorVisitor(operatorsType.nestedClass("Lt")) {
                addInequalityCase(it, "<", true)
            }
            .addContainsInspectorVisitor(operatorsType.nestedClass("LtEq")) {
                addInequalityCase(it, "<=", true)
            }
            .addContainsInspectorVisitor(operatorsType.nestedClass("Gt")) {
                addInequalityCase(it, ">", true)
            }
            .addContainsInspectorVisitor(operatorsType.nestedClass("GtEq")) {
                addInequalityCase(it, ">=", true)
            }
            .addContainsInspectorVisitor(operatorsType.nestedClass("In")) {
                addInNotInCase(it, isEq = true, true)
            }
            .addContainsInspectorVisitor(operatorsType.nestedClass("NotIn")) {
                addInNotInCase(it, isEq = false, true)
            }
            .addFunction(FunSpec.builder("visit")
                .addModifiers(KModifier.OVERRIDE)
                .addParameter("pred", operatorsType.nestedClass("And"))
                .returns(containsPredicateType)
                .addStatement("throw UnsupportedOperationException(\"Operators.And not supported for Contains predicate\")")
                .build())
            .addFunction(FunSpec.builder("visit")
                .addModifiers(KModifier.OVERRIDE)
                .addParameter("pred", operatorsType.nestedClass("Or"))
                .returns(containsPredicateType)
                .addStatement("throw UnsupportedOperationException(\"Operators.Or not supported for Contains predicate\")")
                .build())
            .addFunction(FunSpec.builder("visit")
                .addModifiers(KModifier.OVERRIDE)
                .addParameter("pred", operatorsType.nestedClass("Not"))
                .returns(containsPredicateType)
                .addStatement("throw UnsupportedOperationException(\"Operators.Not not supported for Contains predicate\")")
                .build())
            .addFunction(FunSpec.builder("visit")
                .addModifiers(KModifier.OVERRIDE)
                .addTypeVariable(comparableT)
                .addTypeVariable(userDefinedPredicateU)
                .addParameter("pred", userDefinedType.parameterizedBy(comparableT, TypeVariableName("U")))
                .returns(containsPredicateType)
                .addStatement("throw UnsupportedOperationException(\"LogicalNotUserDefined not supported for Contains predicate\")")
                .build())
            .addFunction(FunSpec.builder("visit")
                .addModifiers(KModifier.OVERRIDE)
                .addTypeVariable(comparableT)
                .addTypeVariable(userDefinedPredicateU)
                .addParameter("notPred", logicalNotUserDefinedType.parameterizedBy(comparableT, TypeVariableName("U")))
                .returns(containsPredicateType)
                .addStatement("throw UnsupportedOperationException(\"LogicalNotUserDefined not supported for Contains predicate\")")
                .build())
            .build()

        addType(containsPredicateClass)
        addType(containsSinglePredicateClass)
        addType(containsAndPredicateClass)
        addType(containsOrPredicateClass)
        addType(containsInspectorVisitorClass)

        block()
    }

    private fun FunSpec.Builder.addContainsCase() = apply {
        addCode("""
            |val valueInspector = ContainsInspectorVisitor().visit(pred)
            |""".trimMargin())
    }

    private fun FunSpec.Builder.addUdpCase(info: TypeInfo, invert: Boolean) {
        beginControlFlow("%L::class.java ->", info.className)
        addCode("%L", TypeSpec.anonymousClassBuilder()
                .superclass(valueInspectorType)
                .addFunction(FunSpec.builder("updateNull")
                    .addModifiers(KModifier.OVERRIDE)
                    .addStatement("setResult(${if (invert) "!" else ""}udp.acceptsNullValue())")
                    .build())
                .addFunction(FunSpec.builder("update")
                    .addModifiers(KModifier.OVERRIDE)
                    .addParameter("value", info.primitiveName)
                    .addStatement("setResult(${if (invert) "!" else ""}udp.keep(value as T))")
                    .build())
                .build())
        endControlFlow()
    }

    private fun compareOp(info: TypeInfo, target: String, op: String): String {
        return "comparator.compare(value as ${info.className}, $target) $op 0"
    }

    private fun compareEquality(info: TypeInfo, target: String, eq: Boolean): String {
        return compareOp(info, target, if (eq) "==" else "!=")
    }
}
