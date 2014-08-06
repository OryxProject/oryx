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

package com.cloudera.oryx.lambda;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.Writable;

/**
 * Utility class that automatically converts a value object to and from a given, corresponding
 * {@link Writable} class. For example, this may convert between {@link String} and
 * {@link org.apache.hadoop.io.Text}.
 */
final class ValueWritableConverter<V> {

  private static final Map<Class<?>,Class<?>> WRAPPER_TO_PRIMITIVE;
  static {
    WRAPPER_TO_PRIMITIVE = new HashMap<>();
    WRAPPER_TO_PRIMITIVE.put(Byte.class, byte.class);
    WRAPPER_TO_PRIMITIVE.put(Short.class, short.class);
    WRAPPER_TO_PRIMITIVE.put(Integer.class, int.class);
    WRAPPER_TO_PRIMITIVE.put(Long.class, long.class);
    WRAPPER_TO_PRIMITIVE.put(Float.class, float.class);
    WRAPPER_TO_PRIMITIVE.put(Double.class, double.class);
    WRAPPER_TO_PRIMITIVE.put(Boolean.class, boolean.class);
    WRAPPER_TO_PRIMITIVE.put(Character.class, char.class);
  }

  private final Method fromWritableMethod;
  private final Constructor<? extends Writable> writableConstructor;
  private final Constructor<? extends Writable> writableNoArgConstructor;

  /**
   * @param valueClass underlying value class, like {@link String} or {@link Integer}
   * @param writableClass must have a method whose return type matches {@code valueClass}
   *  and whose name starts with "get". Or, in the case of value class {@link String}, the
   *  {@link #toString()} method will be used if not present. Must also have a constructor
   *  with a single argument whose type is the value class, and a no-arg constructor.
   */
  <W extends Writable> ValueWritableConverter(Class<V> valueClass, Class<W> writableClass) {
    Method fromWritableMethod = null;
    for (Method method : writableClass.getMethods()) {
      if (method.getName().startsWith("get")) {
        Class<?> returnType = method.getReturnType();
        if (returnType.equals(valueClass) ||
            returnType.equals(WRAPPER_TO_PRIMITIVE.get(valueClass))) {
          fromWritableMethod = method;
          break;
        }
      }
    }
    if (fromWritableMethod == null && String.class.equals(valueClass)) {
      // Special-case String
      try {
        fromWritableMethod = writableClass.getMethod("toString");
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(e);
      }
    }
    Preconditions.checkNotNull(fromWritableMethod,
                               "%s has no method returning %s", writableClass, valueClass);
    this.fromWritableMethod = fromWritableMethod;

    Constructor<W> theNoArgConstructor = null;
    Constructor<W> theOneArgConstructor = null;
    @SuppressWarnings("unchecked")
    Constructor<W>[] constructors = (Constructor<W>[]) writableClass.getConstructors();
    for (Constructor<W> constructor : constructors) {
      Class<?>[] parameterTypes = constructor.getParameterTypes();
      if (parameterTypes.length == 0) {
        theNoArgConstructor = constructor;
      } else if (parameterTypes.length == 1) {
        Class<?> paramType = parameterTypes[0];
        if (paramType.equals(valueClass) ||
            paramType.equals(WRAPPER_TO_PRIMITIVE.get(valueClass))) {
          theOneArgConstructor = constructor;
        }
      }
      if (theNoArgConstructor != null && theOneArgConstructor != null) {
        break;
      }
    }

    Preconditions.checkNotNull(theNoArgConstructor, "%s has no no-arg constructor", writableClass);
    Preconditions.checkNotNull(theOneArgConstructor,
                               "%s has no constructor accepting %s", writableClass, valueClass);
    writableNoArgConstructor =theNoArgConstructor;
    writableConstructor = theOneArgConstructor;
  }

  V fromWritable(Writable writable) {
    try {
      @SuppressWarnings("unchecked")
      V value = (V) fromWritableMethod.invoke(writable);
      return value;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }

  Writable toWritable(V value) {
    try {
      if (value == null) {
        return writableNoArgConstructor.newInstance();
      } else {
        return writableConstructor.newInstance(value);
      }
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }

}
