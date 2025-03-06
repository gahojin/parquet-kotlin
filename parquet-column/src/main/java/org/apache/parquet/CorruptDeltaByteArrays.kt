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

import org.apache.parquet.VersionParser.ParsedVersion
import org.apache.parquet.VersionParser.VersionParseException
import org.apache.parquet.VersionParser.parse
import org.apache.parquet.column.Encoding
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object CorruptDeltaByteArrays {
    private val LOG: Logger = LoggerFactory.getLogger(CorruptStatistics::class.java)

    private val PARQUET_246_FIXED_VERSION = SemanticVersion(1, 8, 0)

    @JvmStatic
    fun requiresSequentialReads(version: ParsedVersion?, encoding: Encoding?): Boolean {
        if (encoding !== Encoding.DELTA_BYTE_ARRAY) {
            return false
        }

        if (version == null) {
            return true
        }

        if ("parquet-mr" != version.application) {
            // assume other applications don't have this bug
            return false
        }

        if (!version.hasSemanticVersion()) {
            LOG.warn(
                "Requiring sequential reads because created_by did not contain a valid version (see PARQUET-246): {}",
                version.version,
            )
            return true
        }

        return requiresSequentialReads(version.semanticVersion, encoding)
    }

    @JvmStatic
    fun requiresSequentialReads(semver: SemanticVersion?, encoding: Encoding?): Boolean {
        if (encoding !== Encoding.DELTA_BYTE_ARRAY) {
            return false
        }

        if (semver == null) {
            return true
        }

        if (semver < PARQUET_246_FIXED_VERSION) {
            LOG.info(
                "Requiring sequential reads because this file was created " + "prior to {}. See PARQUET-246",
                PARQUET_246_FIXED_VERSION,
            )
            return true
        }

        // this file was created after the fix
        return false
    }

    @JvmStatic
    fun requiresSequentialReads(createdBy: String?, encoding: Encoding?): Boolean {
        if (encoding !== Encoding.DELTA_BYTE_ARRAY) {
            return false
        }

        if (createdBy.isNullOrEmpty()) {
            LOG.info("Requiring sequential reads because file version is empty. See PARQUET-246")
            return true
        }

        try {
            return requiresSequentialReads(parse(createdBy), encoding)
        } catch (e: RuntimeException) {
            warnParseError(createdBy, e)
            return true
        } catch (e: VersionParseException) {
            warnParseError(createdBy, e)
            return true
        }
    }

    private fun warnParseError(createdBy: String?, e: Throwable?) {
        LOG.warn(
            "Requiring sequential reads because created_by could not be " + "parsed (see PARQUET-246): {}", createdBy, e
        )
    }
}
