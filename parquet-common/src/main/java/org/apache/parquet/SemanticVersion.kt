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

import java.util.regex.Pattern

/**
 * Very basic semver parser, only pays attention to major, minor, and patch numbers.
 * Attempts to do a little bit of validation that the version string is valid, but
 * is not a full implementation of the semver spec.
 *
 * NOTE: compareTo only respects major, minor, patch, and whether this is a
 * prerelease version. All prerelease versions are considered equivalent.
 */
class SemanticVersion private constructor(
    // this is part of the public API and can't be renamed. it is misleading
    // because it actually signals that there is an unknown component
    @JvmField val major: Int,
    @JvmField val minor: Int,
    @JvmField val patch: Int,
    @JvmField val prerelease: Boolean,
    @JvmField val unknown: String?,
    @JvmField val pre: Prerelease?,
    @JvmField val buildInfo: String?,
) : Comparable<SemanticVersion> {

    constructor(major: Int, minor: Int, patch: Int) : this(
        major = major,
        minor = minor,
        patch = patch,
        prerelease = false,
        unknown = null,
        pre = null,
        buildInfo = null,
    )

    constructor(major: Int, minor: Int, patch: Int, hasUnknown: Boolean) : this(
        major = major,
        minor = minor,
        patch = patch,
        prerelease = hasUnknown,
        unknown = null,
        pre = null,
        buildInfo = null,
    )

    constructor(major: Int, minor: Int, patch: Int, unknown: String?, pre: String?, buildInfo: String?) : this(
        major = major,
        minor = minor,
        patch = patch,
        prerelease = !unknown.isNullOrEmpty(),
        unknown = unknown,
        pre = pre?.let { Prerelease(it) },
        buildInfo = buildInfo,
    )

    init {
        require(major >= 0) { "major must be >= 0" }
        require(minor >= 0) { "minor must be >= 0" }
        require(patch >= 0) { "patch must be >= 0" }
    }

    override fun compareTo(other: SemanticVersion): Int {
        var cmp = compareIntegers(major, other.major)
        if (cmp != 0) {
            return cmp
        }

        cmp = compareIntegers(minor, other.minor)
        if (cmp != 0) {
            return cmp
        }

        cmp = compareIntegers(patch, other.patch)
        if (cmp != 0) {
            return cmp
        }

        cmp = compareBooleans(other.prerelease, prerelease)
        if (cmp != 0) {
            return cmp
        }

        return if (pre != null) {
            if (other.pre != null) {
                pre.compareTo(other.pre)
            } else {
                -1
            }
        } else if (other.pre != null) {
            1
        } else 0
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        val that = (other as? SemanticVersion) ?: return false
        return compareTo(that) == 0
    }

    override fun hashCode(): Int {
        var result = major
        result = 31 * result + minor
        result = 31 * result + patch
        return result
    }

    override fun toString() = buildString {
        append(major).append(".").append(minor).append(".").append(patch)
        if (prerelease) {
            append(unknown)
        }
        pre?.also { append(it.original) }
        buildInfo?.also { append(it) }
    }

    private class NumberOrString(private val original: String) : Comparable<NumberOrString> {
        private val isNumeric: Boolean = NUMERIC.matcher(original).matches()
        private var number = if (isNumeric) original.toInt() else -1

        override fun compareTo(other: NumberOrString): Int {
            // Numeric identifiers always have lower precedence than non-numeric identifiers.
            val cmp = compareBooleans(other.isNumeric, isNumeric)
            if (cmp != 0) {
                return cmp
            }

            if (isNumeric) {
                // identifiers consisting of only digits are compared numerically
                return compareIntegers(number, other.number)
            }

            // identifiers with letters or hyphens are compared lexically in ASCII sort order
            return original.compareTo(other.original)
        }

        override fun toString() = original

        companion object {
            private val NUMERIC: Pattern = Pattern.compile("\\d+")
        }
    }

    class Prerelease(internal val original: String) : Comparable<Prerelease> {
        private val identifiers = DOT.split(original).map { NumberOrString(it) }

        override fun compareTo(other: Prerelease): Int {
            // A larger set of pre-release fields has a higher precedence than a
            // smaller set, if all of the preceding identifiers are equal
            val size = minOf(identifiers.size.toDouble(), other.identifiers.size.toDouble()).toInt()
            var i = 0
            while (i < size) {
                val cmp = identifiers[i].compareTo(other.identifiers[i])
                if (cmp != 0) {
                    return cmp
                }
                i += 1
            }
            return compareIntegers(identifiers.size, other.identifiers.size)
        }

        override fun toString() = original

        companion object {
            private val DOT: Pattern = Pattern.compile("\\.")
        }
    }

    class SemanticVersionParseException : Exception {
        constructor() : super()

        constructor(message: String?) : super(message)

        constructor(message: String?, cause: Throwable?) : super(message, cause)

        constructor(cause: Throwable?) : super(cause)
    }

    companion object {
        // this is slightly more permissive than the semver format:
        // * it allows a pattern after patch and before -prerelease or +buildinfo
        private const val FORMAT =  // major  . minor  .patch   ???       - prerelease.x + build info
            "^(\\d+)\\.(\\d+)\\.(\\d+)([^-+]*)?(?:-([^+]*))?(?:\\+(.*))?$"
        private val PATTERN: Pattern = Pattern.compile(FORMAT)

        @JvmStatic
        @Throws(SemanticVersionParseException::class)
        fun parse(version: String?): SemanticVersion {
            val matcher = PATTERN.matcher(requireNotNull(version))

            if (!matcher.matches()) {
                throw SemanticVersionParseException("$version does not match format $FORMAT")
            }

            val major: Int
            val minor: Int
            val patch: Int
            try {
                major = matcher.group(1).toInt()
                minor = matcher.group(2).toInt()
                patch = matcher.group(3).toInt()
            } catch (e: NumberFormatException) {
                throw SemanticVersionParseException(e)
            }

            val unknown = matcher.group(4)
            val prerelease = matcher.group(5)
            val buildInfo = matcher.group(6)

            if (major < 0 || minor < 0 || patch < 0) {
                throw SemanticVersionParseException("major($major), minor($minor), and patch($patch) must all be >= 0")
            }

            return SemanticVersion(major, minor, patch, unknown, prerelease, buildInfo)
        }

        private fun compareIntegers(x: Int, y: Int): Int {
            return if (x < y) -1 else (if (x == y) 0 else 1)
        }

        private fun compareBooleans(x: Boolean, y: Boolean): Int {
            return if (x == y) 0 else (if (x) 1 else -1)
        }
    }
}
