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

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Utility {@link Function2} implementations.
 */
public final class Functions {

  private Functions() {}

  /**
   * @return a function that returns the second of two values
   * @param <T> element type
   */
  public static <T> Function2<T,T,T> last() {
    return new Function2<T,T,T>() {
      @Override
      public T call(T current, T next) {
        return next;
      }
    };
  }

  public static <T> VoidFunction<T> noOp() {
    return new VoidFunction<T>() {
      @Override
      public void call(T t) {
        // do nothing
      }
    };
  }

  public static <T> Function<T,T> identity() {
    return new Function<T,T>() {
      @Override
      public T call(T t) {
        return t;
      }
    };
  }

}
