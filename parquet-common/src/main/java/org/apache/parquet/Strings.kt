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
package org.apache.parquet

import org.apache.parquet.glob.GlobExpander.expand
import org.apache.parquet.glob.WildcardPath

object Strings {
    /**
     * Returns true if s.isEmpty() or s == null
     *
     * @param s a string that may be null or empty
     * @return true if the string s is null or is empty
     */
    @JvmStatic
    fun isNullOrEmpty(s: String?): Boolean {
        return s.isNullOrEmpty()
    }

    /**
     * Expands a string with braces ("{}") into all of its possible permutations.
     * We call anything inside of {} braces a "one-of" group.
     *
     *
     * The only special characters in this glob syntax are '}', '{' and ','
     *
     *
     * The top-level pattern must not contain any commas, but a "one-of" group separates
     * its elements with commas, and a one-of group may contain sub one-of groups.
     *
     *
     * For example:
     * start{a,b,c}end -&gt; startaend, startbend, startcend
     * start{a,{b,c},d} -&gt; startaend, startbend, startcend, startdend
     * {a,b,c} -&gt; a, b, c
     * start{a, b{x,y}} -&gt; starta, startbx, startby
     *
     * @param globPattern a string in the format described above
     * @return a list of all the strings that would satisfy globPattern, including duplicates
     */
    @JvmStatic
    fun expandGlob(globPattern: String): List<String> {
        return expand(globPattern)
    }

    /**
     * Expands a string according to [.expandGlob], and then constructs a [WildcardPath]
     * for each expanded result which can be used to match strings as described in [WildcardPath].
     *
     * @param globPattern a String to be passed to [.expandGlob]
     * @param delim       the delimeter used by [WildcardPath]
     * @return a list of wildcard paths, one for each expanded result
     */
    @JvmStatic
    fun expandGlobToWildCardPaths(globPattern: String, delim: Char): List<WildcardPath> {
        return expandGlob(globPattern).map {
            WildcardPath(globPattern, it, delim)
        }
    }
}
