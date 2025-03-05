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

import org.apache.parquet.glob.GlobNode.GlobNodeSequence
import org.apache.parquet.glob.GlobNode.OneOf

/**
 * Implementation of [org.apache.parquet.Strings.expandGlob]
 */
object GlobExpander {
    /**
     * Expands a string with braces ("{}") into all of its possible permutations.
     * We call anything inside of {} braces a "one-of" group.
     *
     * The only special characters in this glob syntax are '}', '{' and ','
     *
     * The top-level pattern must not contain any commas, but a "one-of" group separates
     * its elements with commas, and a one-of group may contain sub one-of groups.
     *
     * For example:
     * ```
     * start{a,b,c}end -> startaend, startbend, startcend
     * start{a,{b,c},d} -> startaend, startbend, startcend, startdend
     * {a,b,c} -> a, b, c
     * start{a, b{x,y}} -> starta, startbx, startby
     * ```
     *
     * @param globPattern a string in the format described above
     * @return a list of all the strings that would satisfy globPattern, including duplicates
     */
    @JvmStatic
    fun expand(globPattern: String): List<String> {
        return GlobExpanderImpl.expand(GlobParser.parse(globPattern))
    }

    /**
     * Transforms a tree of [GlobNode] into a list of all the strings that satisfy
     * this tree.
     */
    private class GlobExpanderImpl : GlobNode.Visitor<List<String>> {
        override fun visit(atom: GlobNode.Atom): List<String> {
            // atoms are the base case, just return a singleton list
            return listOf(atom.get())
        }

        override fun visit(oneOf: OneOf): List<String> {
            // in the case of OneOf, we just need to take all of
            // the possible values the OneOf represents and
            // union them together
            return sequence {
                for (n in oneOf.children) {
                    yieldAll(n.accept(this@GlobExpanderImpl))
                }
            }.toList()
        }

        override fun visit(seq: GlobNodeSequence): List<String> {
            // in the case of a sequence, for each child
            // we need to expand the child into all of its
            // possibilities, then do a cross product of
            // all the children, in order.

            var results: List<String> = ArrayList()
            for (n in seq.children) {
                results = crossOrTakeNonEmpty(results, n.accept(this))
            }
            return results
        }

        companion object {
            private val INSTANCE = GlobExpanderImpl()

            fun expand(node: GlobNode): List<String> {
                return node.accept(INSTANCE)
            }

            /**
             * Computes the cross product of two lists by adding each string in list1 to each string in list2.
             * If one of the lists is empty, a copy of the other list is returned.
             * If both are empty, an empty list is returned.
             */
            fun crossOrTakeNonEmpty(list1: List<String>, list2: List<String>): List<String> {
                if (list1.isEmpty()) {
                    val result = ArrayList<String>(list2.size)
                    result.addAll(list2)
                    return result
                }

                if (list2.isEmpty()) {
                    val result = ArrayList<String>(list1.size)
                    result.addAll(list1)
                    return result
                }

                val result = ArrayList<String>(list1.size * list2.size)
                for (s1 in list1) {
                    for (s2 in list2) {
                        result.add(s1 + s2)
                    }
                }
                return result
            }
        }
    }
}
