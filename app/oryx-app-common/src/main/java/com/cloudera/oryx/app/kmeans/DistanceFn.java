/*
 * Copyright (c) 2015, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.app.kmeans;

import java.io.Serializable;
import java.util.function.ToDoubleBiFunction;

/**
 * A function that returns a distance between things.
 *
 * @param <T> type of things to compare
 */
@FunctionalInterface
public interface DistanceFn<T> extends Serializable, ToDoubleBiFunction<T,T> {
  // Adds no methods, just Serializable and specialization of bifunction
}
