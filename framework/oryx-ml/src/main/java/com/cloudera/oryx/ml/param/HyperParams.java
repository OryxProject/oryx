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

package com.cloudera.oryx.ml.param;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.typesafe.config.Config;

/**
 * Utility methods related to expressing and selecting hyperparameter values.
 */
public final class HyperParams {

  private HyperParams() {}

  public static HyperParamValues<Double> fixed(double fixedValue) {
    return new ContinuousRange(fixedValue, fixedValue);
  }

  public static HyperParamValues<Double> range(double min, double max) {
    return new ContinuousRange(min, max);
  }

  public static HyperParamValues<Double> around(double value, double step) {
    return new ContinuousAround(value, step);
  }

  public static HyperParamValues<Integer> fixed(int fixedValue) {
    return new DiscreteRange(fixedValue, fixedValue);
  }

  public static HyperParamValues<Integer> range(int min, int max) {
    return new DiscreteRange(min, max);
  }

  public static HyperParamValues<Integer> around(int value, int step) {
    return new DiscreteAround(value, step);
  }

  public static <T> HyperParamValues<T> unorderedFromValues(Collection<T> values) {
    return new Unordered<>(values);
  }

  /**
   * @param config application configuration
   * @param key config key to access
   * @return a {@link HyperParamValues} appropriate to the value. If an {@code int} or
   *  {@code double}, returns {@link #fixed(int)} or {@link #fixed(double)} respectively.
   *  If a {@code List<int>} or {@code List<double>}, returns {@link #range(int,int)} or
   *  {@link #range(double,double)} respectively.
   */
  public static HyperParamValues<?> fromConfig(Config config, String key) {
    switch (config.getValue(key).valueType()) {
      case LIST:
        List<String> stringValues = config.getStringList(key);
        // Have to distinguish int and double manually
        try {
          return range(Integer.parseInt(stringValues.get(0)),
                       Integer.parseInt(stringValues.get(1)));
        } catch (NumberFormatException nfe) {
          // continue
        }
        try {
          return range(Double.parseDouble(stringValues.get(0)),
                       Double.parseDouble(stringValues.get(1)));
        } catch (NumberFormatException nfe) {
          // continue
        }
        return unorderedFromValues(stringValues);
      case STRING:
      case NUMBER:
        // Have to distinguish int and double manually
        String stringValue = config.getString(key);
        try {
          return fixed(Integer.parseInt(stringValue));
        } catch (NumberFormatException nfe) {
          // continue
        }
        try {
          return fixed(Double.parseDouble(stringValue));
        } catch (NumberFormatException nfe) {
          // continue
        }
        return unorderedFromValues(Collections.singletonList(stringValue));
      default:
        throw new IllegalArgumentException("No valid parameter range for key " + key);
    }
  }

  public static List<List<?>> chooseHyperParameterCombos(
      List<HyperParamValues<?>> ranges, String hyperParamSearch, int howMany) {
    switch (hyperParamSearch) {
      case "grid":
        return GridSearch.chooseHyperParameterCombos(ranges, howMany);
      case "random":
        return RandomSearch.chooseHyperParameterCombos(ranges, howMany);
      default:
        throw new IllegalArgumentException("Unknown hyperparam search type: " + hyperParamSearch);
    }
  }

}
