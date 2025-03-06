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

import org.apache.parquet.SemanticVersion.Companion.parse
import org.apache.parquet.SemanticVersion.SemanticVersionParseException
import org.apache.parquet.VersionParser.VersionParseException
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * There was a bug (PARQUET-251) that caused the statistics metadata
 * for binary columns to be corrupted in the write path.
 *
 * This class is used to detect whether a file was written with this bug,
 * and thus it's statistics should be ignored / not trusted.
 */
object CorruptStatistics {
    private val alreadyLogged = AtomicBoolean(false)

    private val LOG: Logger = LoggerFactory.getLogger(CorruptStatistics::class.java)

    // the version in which the bug described by jira: PARQUET-251 was fixed
    // the bug involved writing invalid binary statistics, so stats written prior to this
    // fix must be ignored / assumed invalid
    private val PARQUET_251_FIXED_VERSION = SemanticVersion(1, 8, 0)
    private val CDH_5_PARQUET_251_FIXED_START = SemanticVersion(1, 5, 0, null, "cdh5.5.0", null)
    private val CDH_5_PARQUET_251_FIXED_END = SemanticVersion(1, 5, 0)

    /**
     * Decides if the statistics from a file created by createdBy (the created_by field from parquet format)
     * should be ignored because they are potentially corrupt.
     *
     * @param createdBy  the created-by string from a file footer
     * @param columnType the type of the column that this is checking
     * @return true if the statistics may be invalid and should be ignored, false otherwise
     */
    @JvmStatic
    fun shouldIgnoreStatistics(createdBy: String?, columnType: PrimitiveTypeName): Boolean {
        if (columnType !== PrimitiveTypeName.BINARY && columnType !== PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            // the bug only applies to binary columns
            return false
        }

        if (createdBy.isNullOrEmpty()) {
            // created_by is not populated, which could have been caused by
            // parquet-mr during the same time as PARQUET-251, see PARQUET-297
            warnOnce("Ignoring statistics because created_by is null or empty! See PARQUET-251 and PARQUET-297")
            return true
        }

        try {
            val version = VersionParser.parse(createdBy)

            if ("parquet-mr" != version.application) {
                // assume other applications don't have this bug
                return false
            }

            if (version.version.isNullOrEmpty()) {
                warnOnce("Ignoring statistics because created_by did not contain a semver (see PARQUET-251): $createdBy")
                return true
            }

            val semver = parse(version.version)

            if (semver < PARQUET_251_FIXED_VERSION
                && !(semver >= CDH_5_PARQUET_251_FIXED_START
                        && semver < CDH_5_PARQUET_251_FIXED_END)
            ) {
                warnOnce("Ignoring statistics because this file was created prior to $PARQUET_251_FIXED_VERSION, see PARQUET-251")
                return true
            }

            // this file was created after the fix
            return false
        } catch (e: RuntimeException) {
            // couldn't parse the created_by field, log what went wrong, don't trust the stats,
            // but don't make this fatal.
            warnParseErrorOnce(createdBy, e)
            return true
        } catch (e: SemanticVersionParseException) {
            warnParseErrorOnce(createdBy, e)
            return true
        } catch (e: VersionParseException) {
            warnParseErrorOnce(createdBy, e)
            return true
        }
    }

    private fun warnParseErrorOnce(createdBy: String?, e: Throwable?) {
        if (!alreadyLogged.getAndSet(true)) {
            LOG.warn("Ignoring statistics because created_by could not be parsed (see PARQUET-251): $createdBy", e)
        }
    }

    private fun warnOnce(message: String) {
        if (!alreadyLogged.getAndSet(true)) {
            LOG.warn(message)
        }
    }
}
