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
package org.apache.parquet.io

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.ints.IntIterator
import org.apache.parquet.column.ColumnWriteStore
import org.apache.parquet.column.ColumnWriter
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.filter.UnboundRecordFilter
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter
import org.apache.parquet.filter2.compat.FilterCompat.UnboundRecordFilterCompat
import org.apache.parquet.filter2.recordlevel.FilteringRecordMaterializer
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateBuilder
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.io.api.RecordMaterializer
import org.apache.parquet.schema.MessageType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.BitSet

/**
 * Message level of the IO structure
 */
class MessageColumnIO internal constructor(
    messageType: MessageType,
    private val validating: Boolean,
    private val createdBy: String?,
) : GroupColumnIO(messageType, null, 0) {
    private var leaves: List<PrimitiveColumnIO> = emptyList()

    override val type: MessageType
        get() = super.type as MessageType

    /**
     * @param columns            a page read store with the column data
     * @param recordMaterializer a record materializer
     * @param filter             a record filter
     * @param <T>                the type of records returned by the reader
     * @return a record reader
     */
    @Deprecated("use getRecordReader(PageReadStore, RecordMaterializer, Filter)")
    fun <T> getRecordReader(
        columns: PageReadStore,
        recordMaterializer: RecordMaterializer<T>,
        filter: UnboundRecordFilter,
    ): RecordReader<T> {
        return getRecordReader<T>(columns, recordMaterializer, FilterCompat.get(filter))
    }

    @JvmOverloads
    fun <T> getRecordReader(
        columns: PageReadStore,
        recordMaterializer: RecordMaterializer<T>,
        filter: FilterCompat.Filter = FilterCompat.NOOP,
    ): RecordReader<T> {
        if (leaves.isEmpty()) {
            return EmptyRecordReader<T>(recordMaterializer)
        }

        return filter.accept<RecordReader<T>>(object : FilterCompat.Visitor<RecordReader<T>> {
            override fun visit(filterPredicateCompat: FilterPredicateCompat): RecordReader<T> {
                val predicate = filterPredicateCompat.filterPredicate
                val builder = IncrementallyUpdatedFilterPredicateBuilder(leaves)
                val streamingPredicate = builder.build(predicate)
                val filteringRecordMaterializer = FilteringRecordMaterializer<T>(
                    recordMaterializer, leaves, builder.valueInspectorsByColumn, streamingPredicate,
                )

                return RecordReaderImplementation<T>(
                    this@MessageColumnIO,
                    filteringRecordMaterializer,
                    validating,
                    ColumnReadStoreImpl(columns, filteringRecordMaterializer.rootConverter, type, createdBy),
                )
            }

            override fun visit(unboundRecordFilterCompat: UnboundRecordFilterCompat): RecordReader<T> {
                return FilteredRecordReader<T>(
                    this@MessageColumnIO,
                    recordMaterializer,
                    validating,
                    ColumnReadStoreImpl(columns, recordMaterializer.rootConverter, type, createdBy),
                    unboundRecordFilterCompat.unboundRecordFilter,
                    columns.rowCount,
                )
            }

            override fun visit(noOpFilter: NoOpFilter): RecordReader<T> {
                return RecordReaderImplementation<T>(
                    this@MessageColumnIO,
                    recordMaterializer,
                    validating,
                    ColumnReadStoreImpl(columns, recordMaterializer.rootConverter, type, createdBy),
                )
            }
        })
    }

    /**
     * To improve null writing performance, we cache null values on group nodes. We flush nulls when a
     * non-null value hits the group node.
     *
     * Intuitively, when a group node hits a null value, all the leaves underneath it should be null.
     * A direct way of doing it is to write nulls for all the leaves underneath it when a group node
     * is null. This approach is not optimal, consider following case:
     *
     * - When the schema is really wide where for each group node, there are thousands of leaf
     * nodes underneath it.
     * - When the data being written is really sparse, group nodes could hit nulls frequently.
     *
     * With the direct approach, if a group node hit null values a thousand times, and there are a
     * thousand nodes underneath it.
     * For each null value, it iterates over a thousand leaf writers to write null values and it
     * will do it for a thousand null values.
     *
     * In the above case, each leaf writer maintains it's own buffer of values, calling thousands of
     * them in turn is very bad for memory locality. Instead each group node can remember the null values
     * encountered and flush only when a non-null value hits the group node. In this way, when we flush
     * null values, we only iterate through all the leaves 1 time and multiple cached null values are
     * flushed to each leaf in a tight loop. This implementation has following characteristics.
     *
     * 1. When a group node hits a null value, it adds the repetition level of the null value to
     * the groupNullCache. The definition level of the cached nulls should always be the same as
     * the definition level of the group node so there is no need to store it.
     *
     * 2. When a group node hits a non null value and it has null value cached, it should flush null
     * values and start from his children group nodes first. This make sure the order of null values
     * being flushed is correct.
     */
    private inner class MessageColumnIORecordConsumer(
        private val columns: ColumnWriteStore,
    ) : RecordConsumer() {
        private var currentColumnIO: ColumnIO? = null
        private var currentLevel = 0

        private inner class FieldsMarker {
            private val visitedIndexes = BitSet()

            override fun toString(): String {
                return "VisitedIndex{visitedIndexes=$visitedIndexes}"
            }

            fun reset(fieldsCount: Int) {
                visitedIndexes.clear(0, fieldsCount)
            }

            fun markWritten(i: Int) {
                visitedIndexes.set(i)
            }

            fun isWritten(i: Int): Boolean {
                return visitedIndexes.get(i)
            }
        }

        // track at each level of depth, which fields are written, so nulls can be inserted for the unwritten fields
        private val fieldsWritten: Array<FieldsMarker>
        private val r: IntArray
        private val columnWriters: Map<Int, ColumnWriter>

        /**
         * Maintain a map of groups and all the leaf nodes underneath it. It's used to optimize writing null for a group node.
         * Instead of using recursion calls, all the leaves can be called directly without traversing the sub tree of the group node
         */
        private val groupToLeafWriter = hashMapOf<GroupColumnIO, MutableList<ColumnWriter>>()

        /*
         * Cache nulls for each group node. It only stores the repetition level, since the definition level
         * should always be the definition level of the group node.
         */
        private val groupNullCache = hashMapOf<GroupColumnIO, IntArrayList>()
        private var emptyField = true

        val columnWriter: ColumnWriter?
            get() = columnWriters[(currentColumnIO as PrimitiveColumnIO).id]

        init {
            var maxDepth = 0
            columnWriters = getLeaves().associateBy({ it.id }) {
                columns.getColumnWriter(it.columnDescriptor).also { w ->
                    maxDepth = maxOf(maxDepth, it.fieldPath.size)
                    buildGroupToLeafWriterMap(it, w)
                }
            }

            fieldsWritten = Array<FieldsMarker>(maxDepth) { FieldsMarker() }
            r = IntArray(maxDepth)
        }

        fun buildGroupToLeafWriterMap(primitive: PrimitiveColumnIO, writer: ColumnWriter) {
            var parent = primitive.parent
            while (parent != null) {
                getLeafWriters(parent).add(writer)
                parent = parent.parent
            }
        }

        fun getLeafWriters(group: GroupColumnIO): MutableList<ColumnWriter> {
            return groupToLeafWriter.getOrPut(group) { mutableListOf() }
        }

        fun printState() {
            if (DEBUG) {
                log("${currentLevel}, ${fieldsWritten[currentLevel]}: ${currentColumnIO?.fieldPath.contentToString()} r:${r[currentLevel]}")
                val currentColumnIO = currentColumnIO
                if (currentColumnIO == null || r[currentLevel] > currentColumnIO.repetitionLevel) {
                    // sanity check
                    throw InvalidRecordException("${r[currentLevel]}(r) > ${currentColumnIO?.repetitionLevel} ( schema r)")
                }
            }
        }

        fun log(message: Any?, vararg parameters: Any?) {
            if (DEBUG) {
                val indent = "  ".repeat(currentLevel)
                if (parameters.isEmpty()) {
                    LOG.debug(indent + message)
                } else {
                    LOG.debug(indent + message, *parameters)
                }
            }
        }

        override fun startMessage() {
            if (DEBUG) log("< MESSAGE START >")
            currentColumnIO = this@MessageColumnIO
            r[0] = 0
            fieldsWritten[0].reset(childrenCount)
            if (DEBUG) printState()
        }

        override fun endMessage() {
            writeNullForMissingFieldsAtCurrentLevel()

            // We need to flush the cached null values before ending the record to ensure that everything is sent to the
            // writer before the current page would be closed
            if (columns.isColumnFlushNeeded) {
                flush()
            }

            columns.endRecord()
            if (DEBUG) log("< MESSAGE END >")
            if (DEBUG) printState()
        }

        override fun startField(field: String, index: Int) {
            try {
                if (DEBUG) log("startField({}, {})", field, index)
                currentColumnIO = (currentColumnIO as GroupColumnIO).getChild(index)
                emptyField = true
                if (DEBUG) printState()
            } catch (e: RuntimeException) {
                throw ParquetEncodingException("error starting field $field at $index", e)
            }
        }

        override fun endField(field: String, index: Int) {
            if (DEBUG) log("endField({}, {})", field, index)
            currentColumnIO = currentColumnIO?.parent
            if (emptyField) {
                throw ParquetEncodingException("empty fields are illegal, the field should be ommited completely instead")
            }
            fieldsWritten[currentLevel].markWritten(index)
            r[currentLevel] = if (currentLevel == 0) 0 else r[currentLevel - 1]
            if (DEBUG) printState()
        }

        fun writeNullForMissingFieldsAtCurrentLevel() {
            val currentColumnIO = currentColumnIO as GroupColumnIO
            val currentFieldsCount = currentColumnIO.childrenCount
            for (i in 0..<currentFieldsCount) {
                if (!fieldsWritten[currentLevel].isWritten(i)) {
                    try {
                        val undefinedField = currentColumnIO.getChild(i)
                        val d = currentColumnIO.definitionLevel
                        if (DEBUG) log("${undefinedField.fieldPath.contentToString()}.writeNull(${r[currentLevel]},$d)")
                        writeNull(undefinedField, r[currentLevel], d)
                    } catch (e: RuntimeException) {
                        throw ParquetEncodingException(
                            "error while writing nulls for fields of indexes $i . current index: ${fieldsWritten[currentLevel]}", e)
                    }
                }
            }
        }

        fun writeNull(undefinedField: ColumnIO, r: Int, d: Int) {
            if (undefinedField.type.isPrimitive) {
                columnWriters[(undefinedField as PrimitiveColumnIO).id]!!.writeNull(r, d)
            } else {
                val groupColumnIO = undefinedField as GroupColumnIO
                // only cache the repetition level, the definition level should always be the definition level of the
                // parent node
                cacheNullForGroup(groupColumnIO, r)
            }
        }

        fun cacheNullForGroup(group: GroupColumnIO, r: Int) {
            var nulls = groupNullCache.getOrPut(group) { IntArrayList() }
            nulls.add(r)
        }

        fun writeNullToLeaves(group: GroupColumnIO) {
            val nullCache = groupNullCache.get(group)
            if (nullCache == null || nullCache.isEmpty()) return

            val parentDefinitionLevel = group.parent!!.definitionLevel
            for (leafWriter in groupToLeafWriter.get(group)!!) {
                val iter: IntIterator = nullCache.iterator()
                while (iter.hasNext()) {
                    val repetitionLevel = iter.nextInt()
                    leafWriter.writeNull(repetitionLevel, parentDefinitionLevel)
                }
            }
            nullCache.clear()
        }

        fun setRepetitionLevel() {
            r[currentLevel] = currentColumnIO!!.repetitionLevel
            if (DEBUG) log("r: {}", r[currentLevel])
        }

        override fun startGroup() {
            if (DEBUG) log("startGroup()")
            val group = currentColumnIO as GroupColumnIO

            // current group is not null, need to flush all the nulls that were cached before
            if (hasNullCache(group)) {
                flushCachedNulls(group)
            }

            ++currentLevel
            r[currentLevel] = r[currentLevel - 1]

            val fieldsCount = (currentColumnIO as GroupColumnIO).childrenCount
            fieldsWritten[currentLevel].reset(fieldsCount)
            if (DEBUG) printState()
        }

        fun hasNullCache(group: GroupColumnIO?): Boolean {
            return !groupNullCache[group].isNullOrEmpty()
        }

        fun flushCachedNulls(group: GroupColumnIO) {
            // flush children first
            for (i in 0..<group.childrenCount) {
                val child = group.getChild(i)
                if (child is GroupColumnIO) {
                    flushCachedNulls(child)
                }
            }
            // then flush itself
            writeNullToLeaves(group)
        }

        override fun endGroup() {
            if (DEBUG) log("endGroup()")
            emptyField = false
            writeNullForMissingFieldsAtCurrentLevel()
            --currentLevel

            setRepetitionLevel()
            if (DEBUG) printState()
        }

        override fun addInteger(value: Int) {
            if (DEBUG) log("addInt({})", value)
            emptyField = false
            this.columnWriter!!.write(value, r[currentLevel], currentColumnIO!!.definitionLevel)

            setRepetitionLevel()
            if (DEBUG) printState()
        }

        override fun addLong(value: Long) {
            if (DEBUG) log("addLong({})", value)
            emptyField = false
            this.columnWriter!!.write(value, r[currentLevel], currentColumnIO!!.definitionLevel)

            setRepetitionLevel()
            if (DEBUG) printState()
        }

        override fun addBoolean(value: Boolean) {
            if (DEBUG) log("addBoolean({})", value)
            emptyField = false
            this.columnWriter!!.write(value, r[currentLevel], currentColumnIO!!.definitionLevel)

            setRepetitionLevel()
            if (DEBUG) printState()
        }

        override fun addBinary(value: Binary) {
            if (DEBUG) log("addBinary({} bytes)", value.length())
            emptyField = false
            this.columnWriter!!.write(value, r[currentLevel], currentColumnIO!!.definitionLevel)

            setRepetitionLevel()
            if (DEBUG) printState()
        }

        override fun addFloat(value: Float) {
            if (DEBUG) log("addFloat({})", value)
            emptyField = false
            this.columnWriter!!.write(value, r[currentLevel], currentColumnIO!!.definitionLevel)

            setRepetitionLevel()
            if (DEBUG) printState()
        }

        override fun addDouble(value: Double) {
            if (DEBUG) log("addDouble({})", value)
            emptyField = false
            this.columnWriter!!.write(value, r[currentLevel], currentColumnIO!!.definitionLevel)

            setRepetitionLevel()
            if (DEBUG) printState()
        }

        /**
         * Flush null for all groups
         */
        override fun flush() {
            flushCachedNulls(this@MessageColumnIO)
        }
    }

    fun getRecordWriter(columns: ColumnWriteStore): RecordConsumer {
        var recordWriter: RecordConsumer = MessageColumnIORecordConsumer(columns)
        if (DEBUG) recordWriter = RecordConsumerLoggingWrapper(recordWriter)
        return if (validating) ValidatingRecordConsumer(recordWriter, type) else recordWriter
    }

    fun setLevels() {
        setLevels(
            0,
            0,
            emptyArray(),
            IntArray(0),
            listOf(this),
            listOf(this)
        )
    }

    fun setLeaves(leaves: MutableList<PrimitiveColumnIO>) {
        this.leaves = leaves
    }

    fun getLeaves(): List<PrimitiveColumnIO> {
        return leaves
    }

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(MessageColumnIO::class.java)

        private val DEBUG: Boolean = LOG.isDebugEnabled
    }
}
