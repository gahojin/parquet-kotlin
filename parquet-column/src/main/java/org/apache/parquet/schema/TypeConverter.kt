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
package org.apache.parquet.schema

/**
 * to convert a MessageType tree
 *
 * @param <T> the resulting Type
 * @see Type.convert
 */
interface TypeConverter<T> {
    /**
     * @param path          the path to that node
     * @param primitiveType the type to convert
     * @return the result of conversion
     */
    fun convertPrimitiveType(
        path: @JvmSuppressWildcards List<GroupType>,
        primitiveType: PrimitiveType,
    ): T

    /**
     * @param path      the path to that node
     * @param groupType the type to convert
     * @param children  its children already converted
     * @return the result of conversion
     */
    fun convertGroupType(
        path: @JvmSuppressWildcards List<GroupType>?,
        groupType: GroupType,
        children: @JvmSuppressWildcards List<T>,
    ): T

    /**
     * @param messageType the type to convert
     * @param children    its children already converted
     * @return the result of conversion
     */
    fun convertMessageType(
        messageType: MessageType,
        children: @JvmSuppressWildcards List<T>,
    ): T
}
