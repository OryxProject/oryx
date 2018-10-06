/*
 * Copyright (c) 2018, Cloudera, Inc. All Rights Reserved.
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

/**
 * Analogous to both {@link java.util.function.ToDoubleBiFunction} and
 * {@link java.util.function.ObjDoubleConsumer}, combined.
 *
 * @param <T> the type of the first argument to the function
 */
@FunctionalInterface
public interface ToDoubleObjDoubleBiFunction<T> {

  /**
   * Applies this function to the given arguments.
   *
   * @param t the first function argument
   * @param u the second function argument
   * @return the function result
   */
  double applyAsDouble(T t, double u);

}
