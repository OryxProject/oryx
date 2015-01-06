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

package com.cloudera.oryx.app.schema;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public final class CategoricalValueEncodings implements Serializable {

  private final Map<Integer,BiMap<String,Integer>> encodingsForIndex;

  /**
   * @param distinctValues map of categorical feature indices, to the set of distinct
   *  values for that categorical feature. Order matters.
   */
  public CategoricalValueEncodings(Map<Integer,? extends Collection<String>> distinctValues) {
    encodingsForIndex = new HashMap<>(distinctValues.size());
    for (Map.Entry<Integer,? extends Collection<String>> e : distinctValues.entrySet()) {
      encodingsForIndex.put(e.getKey(), mapDistinctValues(e.getValue()));
    }
  }

  private static <T> BiMap<T,Integer> mapDistinctValues(Collection<T> distinct) {
    BiMap<T,Integer> mapping = HashBiMap.create();
    int encoding = 0;
    for (T t : distinct) {
      mapping.put(t, encoding);
      encoding++;
    }
    return mapping;
  }

  /**
   * @param index feature index
   * @return mapping of categorical feature values to numeric encodings for the indicated
   *  categorical feature
   */
  public Map<String,Integer> getValueEncodingMap(int index) {
    return doGetMap(index);
  }

  /**
   * @param index feature index
   * @return mapping of numeric encodings to categorical feature values for the indicated
   *  categorical feature
   */
  public Map<Integer,String> getEncodingValueMap(int index) {
    return doGetMap(index).inverse();
  }

  /**
   * @param index feature index
   * @return number of distinct values of the categorical feature
   */
  public int getValueCount(int index) {
    return doGetMap(index).size();
  }

  /**
   * @return map of categorical feature index to number of distinct values of the feature
   */
  public Map<Integer,Integer> getCategoryCounts() {
    Map<Integer,Integer> counts = new HashMap<>(encodingsForIndex.size());
    for (Map.Entry<Integer,BiMap<String,Integer>> entry : encodingsForIndex.entrySet()) {
      counts.put(entry.getKey(), entry.getValue().size());
    }
    return counts;
  }

  private BiMap<String,Integer> doGetMap(int index) {
    Preconditions.checkArgument(index >= 0);
    BiMap<String,Integer> map = encodingsForIndex.get(index);
    Preconditions.checkNotNull(map, "No values for index %s", index);
    return map;
  }

  @Override
  public String toString() {
    return encodingsForIndex.toString();
  }

}
