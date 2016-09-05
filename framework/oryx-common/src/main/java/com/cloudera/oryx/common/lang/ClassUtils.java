/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.common.lang;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * {@link Class}-related utility methods.
 */
public final class ClassUtils {

  private static final Class<?>[] NO_TYPES = new Class<?>[0];
  private static final Object[] NO_ARGS = new Object[0];

  private ClassUtils() {}

  /**
   * @param className fully-qualified class name, which should be known to be available
   * @param <T> expected type of returned {@link Class}
   * @return {@link Class} for that named class
   */
  public static <T> Class<T> loadClass(String className) {
    try {
      @SuppressWarnings("unchecked")
      Class<T> theClass = (Class<T>) forName(className);
      return theClass;
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalStateException("No valid " + className + " exists", cnfe);
    }
  }

  /**
   * @param className fully-qualified class name, which should be known to be available
   * @param superClass reference type of returned {@link Class}
   * @param <T> type of returned {@link Class}
   * @return {@link Class} for that named class
   */
  public static <T> Class<? extends T> loadClass(String className, Class<T> superClass) {
    try {
      return forName(className).asSubclass(superClass);
    } catch (ClassNotFoundException cnfe) {
      throw new IllegalStateException("No valid " + superClass + " binding exists", cnfe);
    }
  }

  /**
   * @param implClassName class name to text
   * @return {@code true} if the class exists in the JVM and can be loaded
   */
  public static boolean classExists(String implClassName) {
    try {
      forName(implClassName);
      return true;
    } catch (ClassNotFoundException ignored) {
      return false;
    }
  }

  private static Class<?> forName(String implClassName) throws ClassNotFoundException {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = ClassUtils.class.getClassLoader();
    }
    return Class.forName(implClassName, true, cl);
  }

  /**
   * Like {@link #loadInstanceOf(String, Class)} where the reference returned is of the same type as
   * the class being loaded -- not any supertype.
   *
   * @param clazz {@link Class} to make an instance of
   * @param <T> type of returned instance
   * @return instance of {@code clazz}
   */
  public static <T> T loadInstanceOf(Class<T> clazz) {
    return loadInstanceOf(clazz.getName(), clazz);
  }

  /**
   * Like {@link #loadInstanceOf(String, Class, Class[], Object[])} for no-arg constructors.
   *
   * @param implClassName implementation class name
   * @param superClass superclass or interface that the implementation extends
   * @param <T> type of returned instance
   * @return instance of {@code implClassName}
   */
  public static <T> T loadInstanceOf(String implClassName, Class<T> superClass) {
    return loadInstanceOf(implClassName, superClass, NO_TYPES, NO_ARGS);
  }

  /**
   * Loads and instantiates a named implementation class, a subclass of a given supertype,
   * whose constructor takes the given arguments.
   *
   * @param implClassName implementation class name
   * @param superClass superclass or interface that the implementation extends
   * @param constructorTypes argument types of constructor to use
   * @param constructorArgs actual constructor arguments
   * @param <T> type of returned instance
   * @return instance of {@code implClassName}
   */
  public static <T> T loadInstanceOf(String implClassName,
                                     Class<T> superClass,
                                     Class<?>[] constructorTypes,
                                     Object[] constructorArgs) {
    try {
      Class<? extends T> configClass = loadClass(implClassName, superClass);
      Constructor<? extends T> constructor = configClass.getConstructor(constructorTypes);
      return constructor.newInstance(constructorArgs);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("No valid " + superClass + " binding exists", e);
    } catch (InvocationTargetException ite) {
      throw new IllegalStateException("Could not instantiate " + superClass + " due to exception",
                                      ite.getCause());
    }
  }

}
