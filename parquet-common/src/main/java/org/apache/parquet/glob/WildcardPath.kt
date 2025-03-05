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

import java.util.regex.Pattern

/**
 * Holds a String with wildcards ('*'), and can answer whether a given string matches this WildcardPath.
 * For example:
 * "foo.*.baz" or "foo*baz.bar*"
 *
 * The '*' in "foo*bar" is treated the same way that java regex treats "(.*)",
 * and all WildcardPath's are considered to match child paths.
 * For example, "foo.bar" will match "foo.bar.baz". It will not match "foo.barbaz" however.
 * To match "foo.barbaz" the pattern "foo.bar*" could be used, which would also match "foo.barbaz.x"
 *
 * Only '*' is  considered a special character.
 * All other characters are not treated as special characters, including '{', '}', '.', and '/'
 * with one exception -- the delimiter character is used for matching against child paths as explained above.
 *
 * It is assumed that {} globs have already been expanded before constructing
 * this object.
 */
class WildcardPath(
    private val parentGlobPath: String,
    private val wildcardPath: String,
    delim: Char,
) {
    private val pattern: Pattern = Pattern.compile(buildRegex(wildcardPath, delim))

    fun matches(path: String) = pattern.matcher(path).matches()

    override fun toString(): String {
        return "WildcardPath(parentGlobPath: '$parentGlobPath', pattern: '$wildcardPath')"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        return wildcardPath == (other as? WildcardPath)?.wildcardPath
    }

    override fun hashCode() = wildcardPath.hashCode()

    companion object {
        private const val STAR_REGEX = "(.*)"
        private const val MORE_NESTED_FIELDS_TEMPLATE = "((%s).*)?"

        fun buildRegex(wildcardPath: String, delim: Char): String {
            if (wildcardPath.isEmpty()) {
                return wildcardPath
            }

            val delimStr = Pattern.quote(delim.toString())
            val splits = wildcardPath.split("\\*".toRegex()).toTypedArray() // -1 means keep trailing empty strings
            return buildString {
                splits.withIndex().forEach { (i, str) ->
                    if ((i == 0 || i == splits.size - 1) && str.isEmpty()) {
                        // there was a * at the beginning or end of the string, so add a regex wildcard
                        append(STAR_REGEX)
                        return@forEach
                    }

                    if (str.isEmpty()) {
                        // means there was a double asterisk, we've already
                        // handled this just keep going.
                        return@forEach
                    }

                    // don't treat this part of the string as a regex, escape
                    // the entire thing
                    append(Pattern.quote(str))

                    if (i < splits.size - 1) {
                        // this isn't the last split, so add a *
                        append(STAR_REGEX)
                    }
                }
                // x.y.z should match "x.y.z" and also "x.y.z.foo.bar"
                append(String.format(MORE_NESTED_FIELDS_TEMPLATE, delimStr))
            }
        }
    }
}
