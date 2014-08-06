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

package com.cloudera.oryx.lambda.fn;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Utility {@link Function2} implementations.
 */
public final class Functions {

  private Functions() {}

  /**
   * A function that sums its (int) arguments.
   */
  public static final Function2<Integer,Integer,Integer> SUM_INT =
      new Function2<Integer,Integer,Integer>() {
        @Override
        public Integer call(Integer a, Integer b) {
          return a + b;
        }
      };

  /**
   * A function that sums its (long) arguments.
   */
  public static final Function2<Long,Long,Long> SUM_LONG =
      new Function2<Long,Long,Long>() {
        @Override
        public Long call(Long a, Long b) {
          return a + b;
        }
      };

  /**
   * A function that sums its (double) arguments.
   */
  public static final Function2<Double,Double,Double> SUM_DOUBLE =
      new Function2<Double,Double,Double>() {
        @Override
        public Double call(Double a, Double b) {
          return a + b;
        }
      };

  /**
   * A function that maps objects to their {@link #toString()} representation.
   */
  public static final Function<Object,String> TO_STRING =
      new Function<Object,String>() {
        @Override
        public String call(Object o) {
          return o == null ? null : o.toString();
        }
      };

  /**
   * @return a function that returns the last element in a stream of values
   */
  public static <T> Function<Iterable<T>,T> last() {
    return new Function<Iterable<T>,T>() {
      @Override
      public T call(Iterable<T> values) {
        T last = null;
        for (T t : values) {
          last = t;
        }
        return last;
      }
    };
  }

}
