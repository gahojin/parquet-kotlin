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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.schema

@Deprecated("""
    use [org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation]
    with proper precision and scale parameters instead"""
)
class DecimalMetadata(val precision: Int, val scale: Int) {
    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || javaClass != o.javaClass) return false

        val that = o as DecimalMetadata

        if (precision != that.precision) return false
        if (scale != that.scale) return false

        return true
    }

    override fun hashCode(): Int {
        var result = precision
        result = 31 * result + scale
        return result
    }
}
