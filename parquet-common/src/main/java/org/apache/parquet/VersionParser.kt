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

import org.apache.parquet.SemanticVersion.SemanticVersionParseException
import org.apache.parquet.Strings.isNullOrEmpty
import java.util.regex.Pattern

/**
 * Parses a parquet Version string
 * Tolerates missing semver and buildhash
 * (semver and build hash may be null)
 */
object VersionParser {
    // example: parquet-mr version 1.8.0rc2-SNAPSHOT (build ddb469afac70404ea63b72ed2f07a911a8592ff7)
    const val FORMAT: String = "(.*?)\\s+version\\s*(?:([^(]*?)\\s*(?:\\(\\s*build\\s*([^)]*?)\\s*\\))?)?"
    @JvmField val PATTERN: Pattern = Pattern.compile(FORMAT)

    @JvmStatic
    @Throws(VersionParseException::class)
    fun parse(createdBy: String): ParsedVersion {
        val matcher = PATTERN.matcher(createdBy)

        if (!matcher.matches()) {
            throw VersionParseException("Could not parse created_by: $createdBy using format: $FORMAT")
        }

        val application = matcher.group(1)
        val semver = matcher.group(2)
        val appBuildHash = matcher.group(3)

        if (isNullOrEmpty(application)) {
            throw VersionParseException("application cannot be null or empty")
        }

        return ParsedVersion(application, semver, appBuildHash)
    }

    class ParsedVersion(
        @JvmField val application: String,
        version: String?,
        appBuildHash: String?,
    ) {
        @JvmField
        val version: String? = if (isNullOrEmpty(version)) null else version

        @JvmField
        val appBuildHash: String? = if (isNullOrEmpty(appBuildHash)) null else appBuildHash

        private val hasSemver: Boolean
        val semanticVersion: SemanticVersion?

        init {
            var sv: SemanticVersion?
            var hasSemver: Boolean
            try {
                sv = SemanticVersion.parse(version)
                hasSemver = true
            } catch (_: RuntimeException) {
                sv = null
                hasSemver = false
            } catch (_: SemanticVersionParseException) {
                sv = null
                hasSemver = false
            }
            this.semanticVersion = sv
            this.hasSemver = hasSemver
        }

        fun hasSemanticVersion(): Boolean {
            return hasSemver
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            val tmp = other as? ParsedVersion ?: return false

            return appBuildHash == tmp.appBuildHash &&
                    application == tmp.application &&
                    version == tmp.version
        }

        override fun hashCode(): Int {
            var result = application?.hashCode() ?: 0
            result = 31 * result + (version?.hashCode() ?: 0)
            result = 31 * result + (appBuildHash?.hashCode() ?: 0)
            return result
        }

        override fun toString(): String {
            return ("ParsedVersion(" + "application="
                    + application + ", semver="
                    + version + ", appBuildHash="
                    + appBuildHash + ')')
        }
    }

    class VersionParseException(message: String?) : Exception(message)
}
