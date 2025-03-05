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
package org.apache.parquet.glob

/**
 * A GlobNode represents a tree structure for describing a parsed glob pattern.
 *
 * GlobNode uses the visitor pattern for tree traversal.
 *
 * See [org.apache.parquet.Strings.expandGlob]
 */
sealed interface GlobNode {
    fun <R> accept(visitor: Visitor<R>): R

    interface Visitor<T> {
        fun visit(atom: Atom): T

        fun visit(oneOf: OneOf): T

        fun visit(seq: GlobNodeSequence): T
    }

    /**
     * An Atom is just a String, it's a concrete String that is either part
     * of the top-level pattern, or one of the choices in a OneOf clause, or an
     * element in a GlobNodeSequence. In this sense it's the base case or leaf node
     * of a GlobNode tree.
     *
     * For example, in pre{x,y{a,b}}post pre, x, y, z, b, and post are all Atoms.
     */
    class Atom(private val s: String) : GlobNode {
        fun get() = s

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            return s == (other as? Atom)?.s
        }

        override fun hashCode() = s.hashCode()

        override fun toString() = "Atom($s)"

        override fun <R> accept(visitor: Visitor<R>): R {
            return visitor.visit(this)
        }
    }

    /**
     * A OneOf represents a {} clause in a glob pattern, which means
     * "one of the elements of this set must be satisfied", for example:
     * in pre{x,y} {x,y} is a OneOf, and in  or pre{x, {a,b}}post both {x, {a,b}}
     * and {a,b} are OneOfs.
     */
    class OneOf(val children: List<GlobNode>) : GlobNode {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            return children == (other as? OneOf)?.children
        }

        override fun hashCode() = children.hashCode()

        override fun toString() = "OneOf$children"

        override fun <R> accept(visitor: Visitor<R>): R {
            return visitor.visit(this)
        }
    }

    /**
     * A GlobNodeSequence is an ordered collection of GlobNodes that must be satisfied in order,
     * and represents structures like pre{x,y}post or {x,y}{a,b}. In {test, pre{x,y}post}, pre{x,y}post is a
     * GlobNodeSequence. Unlike a OneOf, GlobNodeSequence's children have an ordering that is meaningful and
     * the requirements of its children must each be satisfied.
     */
    class GlobNodeSequence(val children: List<GlobNode>) : GlobNode {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            return children == (other as? OneOf)?.children
        }

        override fun hashCode() = children.hashCode()

        override fun toString() = "GlobNodeSequence$children"

        override fun <R> accept(visitor: Visitor<R>): R {
            return visitor.visit(this)
        }
    }
}
