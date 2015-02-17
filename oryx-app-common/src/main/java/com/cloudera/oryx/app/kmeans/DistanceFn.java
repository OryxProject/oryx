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

/**
 * @param <T> type of things to compare
 */
public interface DistanceFn<T> extends Serializable {

  /**
   * @param t1 object t1
   * @param t2 object t2
   * @return some notion of distance between t1 and t2
   */
  double distance(T t1, T t2);

}
