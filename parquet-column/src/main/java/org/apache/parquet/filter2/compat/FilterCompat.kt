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
package org.apache.parquet.filter2.compat

import org.apache.parquet.filter.UnboundRecordFilter
import org.apache.parquet.filter2.predicate.ContainsRewriter
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Parquet currently has two ways to specify a filter for dropping records at read time.
 * The first way, that only supports filtering records during record assembly, is found
 * in [org.apache.parquet.filter]. The new API (found in [org.apache.parquet.filter2]) supports
 * also filtering entire rowgroups of records without reading them at all.
 *
 * This class defines a common interface that both of these filters share,
 * [Filter]. A Filter can be either an [UnboundRecordFilter] from the old API, or
 * a [FilterPredicate] from the new API, or a sentinel no-op filter.
 *
 * Having this common interface simplifies passing a filter through the read path of parquet's
 * codebase.
 */
object FilterCompat {
    private val LOG: Logger = LoggerFactory.getLogger(FilterCompat::class.java)

    // sentinel no op filter that signals "do no filtering"
    @JvmField val NOOP: Filter = NoOpFilter

    /**
     * Given a FilterPredicate, return a Filter that wraps it.
     * This method also logs the filter being used and rewrites
     * the predicate to not include the not() operator.
     *
     * @param filterPredicate a filter predicate
     * @return a filter for the given predicate
     */
    @JvmStatic
    fun get(filterPredicate: FilterPredicate): Filter {
        LOG.info("Filtering using predicate: {}", filterPredicate)

        // rewrite the predicate to not include the not() operator
        val collapsedPredicate = LogicalInverseRewriter.rewrite(filterPredicate)

        if (filterPredicate != collapsedPredicate) {
            LOG.info("Predicate has been collapsed to: {}", collapsedPredicate)
        }

        val rewrittenContainsPredicate = ContainsRewriter.rewrite(collapsedPredicate)
        if (collapsedPredicate != rewrittenContainsPredicate) {
            LOG.info("Contains() Predicate has been rewritten to: {}", rewrittenContainsPredicate)
        }

        return FilterPredicateCompat(rewrittenContainsPredicate)
    }

    /**
     * Given an UnboundRecordFilter, return a Filter that wraps it.
     *
     * @param unboundRecordFilter an unbound record filter
     * @return a Filter for the given record filter (from the old API)
     */
    @JvmStatic
    fun get(unboundRecordFilter: UnboundRecordFilter): Filter {
        return UnboundRecordFilterCompat(unboundRecordFilter)
    }

    /**
     * Given either a FilterPredicate or the class of an UnboundRecordFilter, or neither (but not both)
     * return a Filter that wraps whichever was provided.
     *
     * Either filterPredicate or unboundRecordFilterClass must be null, or an exception is thrown.
     *
     * If both are null, the no op filter will be returned.
     *
     * @param filterPredicate     a filter predicate, or null
     * @param unboundRecordFilter an unbound record filter, or null
     * @return a Filter wrapping either the predicate or the unbound record filter (from the old API)
     */
    @JvmStatic
    fun get(filterPredicate: FilterPredicate?, unboundRecordFilter: UnboundRecordFilter?): Filter {
        require(filterPredicate == null || unboundRecordFilter == null) {
            "Cannot provide both a FilterPredicate and an UnboundRecordFilter"
        }

        return when {
            filterPredicate != null -> get(filterPredicate)
            unboundRecordFilter != null -> get(unboundRecordFilter)
            else -> NoOpFilter
        }
    }

    /**
     * Returns whether filtering is required based on the specified filter. It is used to avoid any significant steps to
     * prepare filtering if [.NOOP] is used.
     *
     * @param filter the filter to be checked
     * @return `false` if the filter is `null` or is a no-op filter, `true` otherwise.
     */
    @JvmStatic
    fun isFilteringRequired(filter: Filter?): Boolean {
        return filter != null && filter !is NoOpFilter
    }

    /**
     * Anyone wanting to use a [Filter] need only implement this interface,
     * per the visitor pattern.
     */
    interface Visitor<T> {
        fun visit(filterPredicateCompat: FilterPredicateCompat): T
        fun visit(unboundRecordFilterCompat: UnboundRecordFilterCompat): T
        fun visit(noOpFilter: NoOpFilter): T
    }

    interface Filter {
        fun <R> accept(visitor: Visitor<R>): R
    }

    // wraps a FilterPredicate
    class FilterPredicateCompat internal constructor(
        val filterPredicate: FilterPredicate,
    ) : Filter {
        override fun <R> accept(visitor: Visitor<R>): R {
            return visitor.visit(this)
        }
    }

    // wraps an UnboundRecordFilter
    class UnboundRecordFilterCompat internal constructor(
        val unboundRecordFilter: UnboundRecordFilter,
    ) : Filter {
        override fun <R> accept(visitor: Visitor<R>): R {
            return visitor.visit(this)
        }
    }

    // sentinel no op filter
    object NoOpFilter : Filter {
        override fun <R> accept(visitor: Visitor<R>): R {
            return visitor.visit(this)
        }
    }
}
