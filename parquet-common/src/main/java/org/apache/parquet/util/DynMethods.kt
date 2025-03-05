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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.security.AccessController
import java.security.PrivilegedAction

object DynMethods {
    private val LOG: Logger = LoggerFactory.getLogger(DynMethods::class.java)

    /**
     * Convenience wrapper class around [Method].
     *
     * Allows callers to invoke the wrapped method with all Exceptions wrapped by
     * RuntimeException, or with a single Exception catch block.
     */
    interface UnboundMethod {
        @Throws(Exception::class)
        fun <R> invokeChecked(target: Any?, vararg args: Any?): R?
        fun <R> invoke(target: Any?, vararg args: Any?): R?

        /**
         * Returns this method as a BoundMethod for the given receiver.
         *
         * @param receiver an Object to receive the method invocation
         * @return a [BoundMethod] for this method and the receiver
         * @throws IllegalStateException    if the method is static
         * @throws IllegalArgumentException if the receiver's class is incompatible
         */
        fun bind(receiver: Any?): BoundMethod

        /**
         * Returns this method as a StaticMethod.
         *
         * @return a [StaticMethod] for this method
         * @throws IllegalStateException if the method is not static
         */
        fun asStatic(): StaticMethod

        /** whether the method is a static method */
        val isStatic: Boolean

        /** whether the method is a noop */
        val isNoop: Boolean

        companion object {
            /**
             * Singleton [UnboundMethod], performs no operation and returns null.
             */
            internal val NOOP: UnboundMethod = object : UnboundMethod {
                @Throws(Exception::class)
                override fun <R> invokeChecked(target: Any?, vararg args: Any?): R? = null

                override fun <R> invoke(target: Any?, vararg args: Any?): R? = null

                override fun bind(receiver: Any?): BoundMethod {
                    return BoundMethod(this, receiver)
                }

                override fun asStatic(): StaticMethod {
                    return StaticMethod(this)
                }

                override val isStatic: Boolean = true
                override val isNoop: Boolean = true

                override fun toString(): String {
                    return "DynMethods.UnboundMethod(NOOP)"
                }
            }
        }
    }

    private class UnboundMethodImpl(
        private val method: Method,
        private val name: String,
    ): UnboundMethod {
        private val argLength: Int = if (method.isVarArgs == false) method.parameterTypes.size else -1

        @Suppress("UNCHECKED_CAST")
        @Throws(Exception::class)
        override fun <R> invokeChecked(target: Any?, vararg args: Any?): R? {
            return try {
                if (argLength < 0) {
                    method.invoke(target, *args) as R?
                } else {
                    method.invoke(target, *args.copyOf(argLength)) as R?
                }
            } catch (e: InvocationTargetException) {
                throwIfInstance<Exception>(e.cause, Exception::class.java)
                throwIfInstance<RuntimeException>(e.cause, RuntimeException::class.java)
                throw RuntimeException(e.cause)
            }
        }

        override fun <R> invoke(target: Any?, vararg args: Any?): R? {
            try {
                return invokeChecked<R?>(target, *args)
            } catch (e: Exception) {
                throwIfInstance<RuntimeException>(e, RuntimeException::class.java)
                throw RuntimeException(e)
            }
        }

        override fun bind(receiver: Any?): BoundMethod {
            requireNotNull(receiver)
            check(!isStatic) { "Cannot bind static method ${method.toGenericString()}" }
            require(method.declaringClass.isAssignableFrom(receiver.javaClass)) {
                "Cannot bind ${method.toGenericString()} to instance of ${receiver.javaClass}"
            }

            return BoundMethod(this, receiver)
        }

        /** whether the method is a static method */
        override val isStatic: Boolean
            get() = Modifier.isStatic(method.modifiers)

        /** whether the method is a noop */
        override val isNoop = this === UnboundMethod.NOOP

        /**
         * Returns this method as a StaticMethod.
         *
         * @return a [StaticMethod] for this method
         * @throws IllegalStateException if the method is not static
         */
        override fun asStatic(): StaticMethod {
            check(isStatic) { "Method is not static" }
            return StaticMethod(this)
        }

        override fun toString(): String {
            return "DynMethods.UnboundMethod(name=$name method=${method.toGenericString()})"
        }
    }

    class BoundMethod internal constructor(
        private val method: UnboundMethod,
        private val receiver: Any?,
    ) {
        @Throws(Exception::class)
        fun <R> invokeChecked(vararg args: Any?): R? {
            return method.invokeChecked(receiver, *args)
        }

        fun <R> invoke(vararg args: Any?): R? {
            return method.invoke(receiver, *args)
        }
    }

    class StaticMethod internal constructor(
        private val method: UnboundMethod,
    ) {
        @Throws(Exception::class)
        fun <R> invokeChecked(vararg args: Any?): R? {
            return method.invokeChecked<R>(null, *args)
        }

        fun <R> invoke(vararg args: Any?): R? {
            return method.invoke<R>(null, *args)
        }
    }

    class Builder(private val name: String) {
        private var loader: ClassLoader = Thread.currentThread().getContextClassLoader()
        private var method: UnboundMethod? = null

        /**
         * Set the [ClassLoader] used to lookup classes by name.
         *
         * If not set, the current thread's ClassLoader is used.
         *
         * @param loader a ClassLoader
         * @return this Builder for method chaining
         */
        fun loader(loader: ClassLoader): Builder {
            this.loader = loader
            return this
        }

        /**
         * If no implementation has been found, adds a NOOP method.
         *
         * Note: calls to impl will not match after this method is called!
         *
         * @return this Builder for method chaining
         */
        fun orNoop(): Builder {
            if (method == null) {
                method = UnboundMethod.NOOP
            }
            return this
        }

        /**
         * Checks for an implementation, first finding the given class by name.
         *
         * @param className  name of a class
         * @param methodName name of a method (different from constructor)
         * @param argClasses argument classes for the method
         * @return this Builder for method chaining
         */
        fun impl(className: String?, methodName: String, vararg argClasses: Class<*>?): Builder {
            // don't do any work if an implementation has been found
            if (method != null) {
                return this
            }

            try {
                val targetClass = Class.forName(className, true, loader)
                impl(targetClass, methodName, *argClasses)
            } catch (e: ClassNotFoundException) {
                // class not found on supplied classloader.
                LOG.debug("failed to load class {}", className, e)
            }
            return this
        }

        /**
         * Checks for an implementation, first finding the given class by name.
         *
         *
         * The name passed to the constructor is the method name used.
         *
         * @param className  name of a class
         * @param argClasses argument classes for the method
         * @return this Builder for method chaining
         */
        fun impl(className: String?, vararg argClasses: Class<*>?): Builder {
            impl(className, name, *argClasses)
            return this
        }

        /**
         * Checks for a method implementation.
         *
         * @param targetClass the class to check for an implementation
         * @param methodName  name of a method (different from constructor)
         * @param argClasses  argument classes for the method
         * @return this Builder for method chaining
         */
        fun impl(targetClass: Class<*>, methodName: String, vararg argClasses: Class<*>?): Builder {
            // don't do any work if an implementation has been found
            if (method != null) {
                return this
            }

            try {
                method = UnboundMethodImpl(targetClass.getMethod(methodName, *argClasses), name)
            } catch (e: NoSuchMethodException) {
                // not the right implementation
                LOG.debug("failed to load method {} from class {}", methodName, targetClass, e)
            }
            return this
        }

        /**
         * Checks for a method implementation.
         *
         *
         * The name passed to the constructor is the method name used.
         *
         * @param targetClass the class to check for an implementation
         * @param argClasses  argument classes for the method
         * @return this Builder for method chaining
         */
        fun impl(targetClass: Class<*>, vararg argClasses: Class<*>?): Builder {
            impl(targetClass, name, *argClasses)
            return this
        }

        fun ctorImpl(targetClass: Class<*>, vararg argClasses: Class<*>): Builder {
            // don't do any work if an implementation has been found
            if (method != null) {
                return this
            }

            try {
                method = DynConstructors.Builder()
                    .impl(targetClass, *argClasses)
                    .buildChecked<Any?>()
            } catch (e: NoSuchMethodException) {
                // not the right implementation
                LOG.debug("failed to load constructor arity {} from class {}", argClasses.size, targetClass, e)
            }
            return this
        }

        fun ctorImpl(className: String, vararg argClasses: Class<*>): Builder {
            // don't do any work if an implementation has been found
            method ?: try {
                method = DynConstructors.Builder()
                    .impl(className, *argClasses)
                    .buildChecked<Any?>()
            } catch (e: NoSuchMethodException) {
                // not the right implementation
                LOG.debug("failed to load constructor arity {} from class {}", argClasses.size, className, e)
            }
            return this
        }

        /**
         * Checks for an implementation, first finding the given class by name.
         *
         * @param className  name of a class
         * @param methodName name of a method (different from constructor)
         * @param argClasses argument classes for the method
         * @return this Builder for method chaining
         */
        fun hiddenImpl(className: String?, methodName: String, vararg argClasses: Class<*>): Builder {
            // don't do any work if an implementation has been found
            method ?: try {
                val targetClass = Class.forName(className, true, loader)
                hiddenImpl(targetClass, methodName, *argClasses)
            } catch (e: ClassNotFoundException) {
                // class not found on supplied classloader.
                LOG.debug("failed to load class {}", className, e)
            }
            return this
        }

        /**
         * Checks for an implementation, first finding the given class by name.
         *
         *
         * The name passed to the constructor is the method name used.
         *
         * @param className  name of a class
         * @param argClasses argument classes for the method
         * @return this Builder for method chaining
         */
        fun hiddenImpl(className: String, vararg argClasses: Class<*>): Builder {
            hiddenImpl(className, name, *argClasses)
            return this
        }

        /**
         * Checks for a method implementation.
         *
         * @param targetClass the class to check for an implementation
         * @param methodName  name of a method (different from constructor)
         * @param argClasses  argument classes for the method
         * @return this Builder for method chaining
         */
        fun hiddenImpl(targetClass: Class<*>, methodName: String, vararg argClasses: Class<*>): Builder {
            // don't do any work if an implementation has been found
            method ?: try {
                val hidden = targetClass.getDeclaredMethod(methodName, *argClasses)
                AccessController.doPrivileged<Void?>(MakeAccessible(hidden))
                method = UnboundMethodImpl(hidden, name)
            } catch (e: SecurityException) {
                // unusable or not the right implementation
                LOG.debug("failed to load method {} from class {}", methodName, targetClass, e)
            } catch (e: NoSuchMethodException) {
                LOG.debug("failed to load method {} from class {}", methodName, targetClass, e)
            }
            return this
        }

        /**
         * Checks for a method implementation.
         *
         *
         * The name passed to the constructor is the method name used.
         *
         * @param targetClass the class to check for an implementation
         * @param argClasses  argument classes for the method
         * @return this Builder for method chaining
         */
        fun hiddenImpl(targetClass: Class<*>, vararg argClasses: Class<*>): Builder {
            hiddenImpl(targetClass, name, *argClasses)
            return this
        }

        /**
         * Returns the first valid implementation as a UnboundMethod or throws a
         * NoSuchMethodException if there is none.
         *
         * @return a [UnboundMethod] with a valid implementation
         * @throws NoSuchMethodException if no implementation was found
         */
        @Throws(NoSuchMethodException::class)
        fun buildChecked(): UnboundMethod {
            return method ?: throw NoSuchMethodException("Cannot find method: $name")
        }

        /**
         * Returns the first valid implementation as a UnboundMethod or throws a
         * RuntimeError if there is none.
         *
         * @return a [UnboundMethod] with a valid implementation
         * @throws RuntimeException if no implementation was found
         */
        fun build(): UnboundMethod {
            return method ?: throw RuntimeException("Cannot find method: $name")
        }

        /**
         * Returns the first valid implementation as a BoundMethod or throws a
         * NoSuchMethodException if there is none.
         *
         * @param receiver an Object to receive the method invocation
         * @return a [BoundMethod] with a valid implementation and receiver
         * @throws IllegalStateException    if the method is static
         * @throws IllegalArgumentException if the receiver's class is incompatible
         * @throws NoSuchMethodException    if no implementation was found
         */
        @Throws(NoSuchMethodException::class)
        fun buildChecked(receiver: Any): BoundMethod? {
            return buildChecked().bind(receiver)
        }

        /**
         * Returns the first valid implementation as a BoundMethod or throws a
         * RuntimeError if there is none.
         *
         * @param receiver an Object to receive the method invocation
         * @return a [BoundMethod] with a valid implementation and receiver
         * @throws IllegalStateException    if the method is static
         * @throws IllegalArgumentException if the receiver's class is incompatible
         * @throws RuntimeException         if no implementation was found
         */
        fun build(receiver: Any): BoundMethod? {
            return build().bind(receiver)
        }

        /**
         * Returns the first valid implementation as a StaticMethod or throws a
         * NoSuchMethodException if there is none.
         *
         * @return a [StaticMethod] with a valid implementation
         * @throws IllegalStateException if the method is not static
         * @throws NoSuchMethodException if no implementation was found
         */
        @Throws(NoSuchMethodException::class)
        fun buildStaticChecked(): StaticMethod {
            return buildChecked().asStatic()
        }

        /**
         * Returns the first valid implementation as a StaticMethod or throws a
         * RuntimeException if there is none.
         *
         * @return a [StaticMethod] with a valid implementation
         * @throws IllegalStateException if the method is not static
         * @throws RuntimeException      if no implementation was found
         */
        fun buildStatic(): StaticMethod {
            return build().asStatic()
        }
    }

    private class MakeAccessible(private val hidden: Method) : PrivilegedAction<Void?> {
        override fun run(): Void? {
            hidden.setAccessible(true)
            return null
        }
    }
}
