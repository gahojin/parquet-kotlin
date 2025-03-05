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
package org.apache.parquet.util

import org.apache.parquet.Exceptions.throwIfInstance
import org.apache.parquet.util.DynMethods.StaticMethod
import java.lang.reflect.Constructor
import java.lang.reflect.InvocationTargetException
import java.security.AccessController
import java.security.PrivilegedAction

object DynConstructors {
    private fun formatProblems(problems: Map<String, Throwable>) = buildString {
        var first = true
        for (problem in problems.entries) {
            if (first) {
                first = false
            } else {
                append('\n')
            }
            append("\tMissing ")
            append(problem.key)
            append(" [")
            append(problem.value.javaClass.getName())
            append(": ")
            append(problem.value.message)
            append(']')
        }
    }

    private fun methodName(targetClass: Class<*>, vararg types: Class<*>) = buildString {
        append(targetClass.getName())
        append('(')
        var first = true
        for (type in types) {
            if (first) {
                first = false
            } else {
                append(",")
            }
            append(type.getName())
        }
        append(')')
    }

    class Ctor<C> internal constructor(
        private val ctor: Constructor<C>,
        val constructedClass: Class<out C>,
    ) : DynMethods.UnboundMethod {
        override val isStatic: Boolean = true
        override val isNoop: Boolean = false

        @Throws(Exception::class)
        fun newInstanceChecked(vararg args: Any?): C {
            try {
                return ctor.newInstance(*args)
            } catch (e: InstantiationException) {
                throw e
            } catch (e: IllegalAccessException) {
                throw e
            } catch (e: InvocationTargetException) {
                throwIfInstance<Exception>(e.cause, Exception::class.java)
                throwIfInstance<RuntimeException>(e.cause, RuntimeException::class.java)
                throw RuntimeException(e.cause)
            }
        }

        fun newInstance(vararg args: Any?): C {
            try {
                return newInstanceChecked(*args)
            } catch (e: Exception) {
                throwIfInstance<RuntimeException>(e, RuntimeException::class.java)
                throw RuntimeException(e)
            }
        }

        @Suppress("UNCHECKED_CAST")
        override fun <R> invoke(target: Any?, vararg args: Any?): R {
            require(target == null) { "Invalid call to constructor: target must be null" }
            return newInstance(*args) as R
        }

        @Suppress("UNCHECKED_CAST")
        @Throws(Exception::class)
        override fun <R> invokeChecked(target: Any?, vararg args: Any?): R? {
            require(target == null) { "Invalid call to constructor: target must be null" }
            return newInstanceChecked(*args) as R?
        }

        override fun bind(receiver: Any?): DynMethods.BoundMethod {
            error("Cannot bind constructors")
        }

        override fun asStatic() = StaticMethod(this)

        override fun toString(): String {
            return "${javaClass.getSimpleName()}(constructor=$ctor, class=$constructedClass)"
        }
    }

    class Builder {
        private val baseClass: Class<*>?
        private var loader: ClassLoader? = Thread.currentThread().getContextClassLoader()
        private var ctor: Ctor<*>? = null
        private val problems: MutableMap<String, Throwable> = hashMapOf()

        @JvmOverloads
        constructor(baseClass: Class<*>? = null) {
            this.baseClass = baseClass
        }

        /**
         * Set the [ClassLoader] used to lookup classes by name.
         *
         *
         * If not set, the current thread's ClassLoader is used.
         *
         * @param loader a ClassLoader
         * @return this Builder for method chaining
         */
        fun loader(loader: ClassLoader?): Builder {
            this.loader = loader
            return this
        }

        fun impl(className: String, vararg types: Class<*>): Builder {
            // don't do any work if an implementation has been found
            ctor ?: try {
                val targetClass = Class.forName(className, true, loader)
                impl(targetClass, *types)
            } catch (e: NoClassDefFoundError) {
                // cannot load this implementation
                problems.put(className, e)
            } catch (e: ClassNotFoundException) {
                problems.put(className, e)
            }

            return this
        }

        fun <T> impl(targetClass: Class<T>, vararg types: Class<*>): Builder {
            // don't do any work if an implementation has been found
            ctor ?: try {
                ctor = Ctor<T>(targetClass.getConstructor(*types), targetClass)
            } catch (e: NoSuchMethodException) {
                // not the right implementation
                problems.put(methodName(targetClass, *types), e)
            }
            return this
        }

        fun hiddenImpl(className: String, vararg types: Class<*>): Builder {
            // don't do any work if an implementation has been found
            ctor ?: try {
                val targetClass = Class.forName(className, true, loader)
                hiddenImpl(targetClass, *types)
            } catch (e: NoClassDefFoundError) {
                // cannot load this implementation
                problems.put(className, e)
            } catch (e: ClassNotFoundException) {
                problems.put(className, e)
            }
            return this
        }

        @Suppress("UNCHECKED_CAST")
        @Deprecated("java.security.AccessController is deprecated")
        @JvmOverloads
        fun <T> hiddenImpl(targetClass: Class<T> = baseClass as Class<T>, vararg types: Class<*>): Builder {
            // don't do any work if an implementation has been found
            ctor ?: try {
                val hidden = targetClass.getDeclaredConstructor(*types)
                AccessController.doPrivileged<Void?>(MakeAccessible(hidden))
                ctor = Ctor(hidden, targetClass)
            } catch (e: NoSuchMethodException) {
                // unusable or not the right implementation
                problems.put(methodName(targetClass, *types), e)
            } catch (e: SecurityException) {
                problems.put(methodName(targetClass, *types), e)
            }
            return this
        }

        @Suppress("UNCHECKED_CAST")
        @Throws(NoSuchMethodException::class)
        fun <C> buildChecked(): Ctor<C> {
            return (ctor as? Ctor<C>) ?: throw NoSuchMethodException("Cannot find constructor for $baseClass\n${formatProblems(problems)}")
        }

        @Suppress("UNCHECKED_CAST")
        fun <C> build(): Ctor<C> {
            return (ctor as? Ctor<C>) ?: throw RuntimeException("Cannot find constructor for $baseClass\n${formatProblems(problems)}")
        }
    }

    private class MakeAccessible(private val hidden: Constructor<*>) : PrivilegedAction<Void?> {
        override fun run(): Void? {
            hidden.setAccessible(true)
            return null
        }
    }
}
