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
package org.apache.parquet.encoding.bitpacking

import com.squareup.kotlinpoet.*
import com.squareup.kotlinpoet.ParameterizedTypeName.Companion.parameterizedBy
import java.nio.ByteBuffer
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.math.abs

object ByteBasedBitPackingGenerator {
  private const val PACKAGE_NAME = "org.apache.parquet.column.values.bitpacking"
  private const val CLASS_NAME_PREFIX_FOR_INT: String = "ByteBitPacking"
  private const val CLASS_NAME_PREFIX_FOR_LONG: String = "ByteBitPackingForLong"
  private val BYTE_BUFFER = ByteBuffer::class.asClassName()

  fun main(args: Array<String>) {
    val basePath = Path(args[0])

    // Int for Big Endian
    generateScheme(isLong = false, msbFirst = true, basePath)

    // Int for Little Endian
    generateScheme(isLong = false, msbFirst = false, basePath)

    // Long for Big Endian
    generateScheme(isLong = true, msbFirst = true, basePath)

    // Long for Little Endian
    generateScheme(isLong = true, msbFirst = false, basePath)
  }

  private fun generateScheme(isLong: Boolean, msbFirst: Boolean, basePath: Path) {
    val baseClassName = if (isLong) CLASS_NAME_PREFIX_FOR_LONG else CLASS_NAME_PREFIX_FOR_INT
    val className = if (msbFirst) "${baseClassName}BE" else "${baseClassName}LE"
    val maxBits = if (isLong) MAX_BITS_FOR_LONG else MAX_BITS_FOR_INT
    val nameSuffix = if (isLong) "ForLong" else ""

    val packerClass = ClassName(PACKAGE_NAME, "BytePacker$nameSuffix")
    val factoryClass = ClassName(PACKAGE_NAME, "BytePacker${nameSuffix}Factory")

    val schemaClass = TypeSpec.classBuilder(className)
      .addModifiers(KModifier.PUBLIC, KModifier.ABSTRACT)
      .addType(TypeSpec.companionObjectBuilder()
        .addProperty(PropertySpec.builder("packers", ARRAY.parameterizedBy(packerClass))
          .addModifiers(KModifier.PRIVATE)
          // private val packers: Array<BytePackersXXX> = arrayOf(...)
          .initializer(CodeBlock.builder()
            .add("arrayOf(\n").apply {
              indent()
              for (i in 0..maxBits) {
                addStatement("Packer$i,")
              }
              unindent()
            }
            .add(")")
            .build()
          )
          .build())
        .addProperty(PropertySpec.builder("factory", factoryClass)
          .addAnnotation(JvmField::class)
          .addModifiers(KModifier.PUBLIC)
          // val factory: BytePackerXXXFactory = object : BytePackerXXXFactory { }
          .initializer("%L", TypeSpec.anonymousClassBuilder()
            .addSuperinterface(factoryClass)
            .addFunction(FunSpec.builder("newBytePacker$nameSuffix")
              .addModifiers(KModifier.PUBLIC, KModifier.OVERRIDE)
              .addParameter("width", Int::class)
              .returns(packerClass)
              .addStatement("return packers[width]")
              .build()
            )
            .build()
          )
          .build()
        )
        .build()
      )
      .addTypes((0..maxBits).map { width ->
        TypeSpec.objectBuilder(ClassName(PACKAGE_NAME, "Packer$width"))
          .superclass(packerClass)
          .addModifiers(KModifier.PRIVATE)
          .addSuperclassConstructorParameter("%L", width).apply {
            generatePack(width, 1, isLong, msbFirst)
            generatePack(width, 4, isLong, msbFirst)

            generateUnpack(width, 1, isLong, msbFirst, true)
            generateUnpack(width, 1, isLong, msbFirst, false)
            generateUnpack(width, 4, isLong, msbFirst, true)
            generateUnpack(width, 4, isLong, msbFirst, false)
          }
          .build()
      })
      .build()

    val schemaFile = FileSpec.builder(PACKAGE_NAME, className)
      .addFileComment("This class is auto-generated by ${PACKAGE_NAME}.${className}\nDo not manually edit!")
      .addImport("java.nio", "ByteBuffer")
      .addType(schemaClass)
      .build()

    schemaFile.writeTo(basePath)
  }

  private fun TypeSpec.Builder.generatePack(
    width: Int,
    batch: Int,
    isLong: Boolean,
    msbFirst: Boolean,
  ) {
    val mask = genMask(width, isLong)
    val maskSuffix = if (isLong) "L" else ""
    val variableType = if (isLong) LONG_ARRAY else INT_ARRAY

    addFunction(FunSpec.builder("pack${batch * 8}Values")
      .addModifiers(KModifier.PUBLIC, KModifier.OVERRIDE)
      .addParameter("input", variableType)
      .addParameter("inputPos", INT)
      .addParameter("output", BYTE_ARRAY)
      .addParameter("outputPos", INT)
      .apply {
        for (index in 0..<width * batch) {
          addStatement(buildString {
            append("output[$index + outputPos] = (")

            val startIndex = index * 8 / width
            val endIndex = ((index + 1) * 8 + width - 1) / width
            for (valueIndex in startIndex..<endIndex) {
              val shiftMask = getShift(width, isLong, msbFirst, index, valueIndex)
              val shiftString = when {
                shiftMask.shift > 0 -> " ushr ${shiftMask.shift}"
                shiftMask.shift < 0 -> " shl ${abs(shiftMask.shift)}"
                else -> ""
              }
              val joinString = if (valueIndex < endIndex - 1) " or " else ""
              append("(input[$valueIndex + inputPos] and $mask$maskSuffix$shiftString)$joinString")
            }
            append(" and 0xFF).toByte()")
          })
        }
      }
      .build())
  }

  private fun TypeSpec.Builder.generateUnpack(
    width: Int,
    batch: Int,
    isLong: Boolean,
    msbFirst: Boolean,
    useByteArray: Boolean,
  ) {
    val variableType = if (isLong) LONG_ARRAY else INT_ARRAY
    val bufferDataType = if (useByteArray) BYTE_ARRAY else BYTE_BUFFER

    addFunction(FunSpec.builder("unpack${batch * 8}Values")
      .addModifiers(KModifier.PUBLIC, KModifier.OVERRIDE)
      .addParameter("input", bufferDataType)
      .addParameter("inputPos", INT)
      .addParameter("output", variableType)
      .addParameter("outputPos", INT)
      .apply {
        if (useByteArray) {
          addAnnotation(AnnotationSpec.builder(Deprecated::class)
            .addMember("\"\"")
            .build())
        }

        if (width <= 0) {
          return@apply
        }
        val maskSuffix = if (isLong) "L" else ""
        for (valueIndex in 0..<(batch * 8)) {
          addStatement(buildString {
            append("output[$valueIndex + outputPos] =")

            val startIndex = valueIndex * width / 8
            val endIndex = (((valueIndex + 1) * width) + 7) / 8
            for (byteIndex in startIndex..<endIndex) {
              val shiftMask = getShift(width, isLong, msbFirst, byteIndex, valueIndex)
              val shiftString = when {
                shiftMask.shift < 0 -> " ushr ${abs(shiftMask.shift)}"
                shiftMask.shift > 0 -> " shl ${shiftMask.shift}"
                else -> ""
              }
              append(" (input[$byteIndex + inputPos]")
              if (isLong) append(".toLong()") else append(".toInt()")
              val joinString = if (byteIndex < endIndex - 1) " or" else ""
              append("$shiftString and ${shiftMask.mask}$maskSuffix)$joinString")
            }
          })
        }
      }
      .build())
  }

  private fun getShift(
    width: Int,
    isLong: Boolean,
    msbFirst: Boolean,
    byteIndex: Int,
    valueIndex: Int,
  ): ShiftMask {
    // relative positions of the start and end of the value to the start and end of the byte
    val valueStartBitIndex: Int = (valueIndex * width) - (8 * (byteIndex))
    val valueEndBitIndex: Int = ((valueIndex + 1) * width) - (8 * (byteIndex + 1))

    // boundaries of the current byte that will receive them
    val byteStartBitWanted: Int
    val byteEndBitWanted: Int

    val shift: Int
    val widthWanted: Int

    if (msbFirst) {
      val valueEndBitWanted = maxOf(valueEndBitIndex, 0)
      byteStartBitWanted = minOf(8, 7 - valueStartBitIndex)
      byteEndBitWanted = abs(minOf(valueEndBitIndex, 0))
      shift = valueEndBitWanted - byteEndBitWanted
      widthWanted = minOf(7, byteStartBitWanted) - minOf(7, byteEndBitWanted) + 1
    } else {
      val valueStartBitWanted = width - 1 - maxOf(valueEndBitIndex, 0)
      byteStartBitWanted = 7 + minOf(valueEndBitIndex, 0)
      byteEndBitWanted = 7 - minOf(8, 7 - valueStartBitIndex)
      shift = valueStartBitWanted - byteStartBitWanted
      widthWanted = maxOf(0, byteStartBitWanted) - maxOf(0, byteEndBitWanted) + 1
    }

    val maskWidth = widthWanted + maxOf(0, shift)

    return ShiftMask(shift, genMask(maskWidth, isLong))
  }

  data class ShiftMask(
    val shift: Int,
    val mask: Long,
  )
}
