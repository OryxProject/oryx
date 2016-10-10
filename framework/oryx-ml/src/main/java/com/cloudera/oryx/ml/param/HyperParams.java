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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.commons.math3.random.RandomDataGenerator;

import com.cloudera.oryx.common.random.RandomManager;

/**
 * Utility methods related to expressing and selecting hyperparameter values.
 */
public final class HyperParams {

  private static final int MAX_COMBOS = 65536;

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

  /**
   * @param ranges ranges of hyperparameters to try, one per hyperparameters
   * @param howMany how many combinations of hyperparameters to return
   * @param perParam how many different hyperparameter values to try per hyperparameter
   * @return combinations of concrete hyperparameter values. For example, for 5 parameters each
   *  with 3 values to try, the total number of combinations returned could be up to pow(3,5)
   *  or 243. The number could be less if the ranges do not actually have that many distinct
   *  values. If {@code howMany} is smaller than the total number of combinations, a random
   *  subset of all combinations are returned. The order is shuffled randomly. If no parameters
   *  are specified or {@code perParam} is 0, a single empty combination is returned.
   */
  public static List<List<?>> chooseHyperParameterCombos(
      Collection<HyperParamValues<?>> ranges,
      int howMany,
      int perParam) {
    Preconditions.checkArgument(howMany > 0);
    Preconditions.checkArgument(perParam >= 0);

    int numParams = ranges.size();
    if (numParams == 0 || perParam == 0) {
      return Collections.singletonList(Collections.emptyList());
    }

    // Put some reasonable upper limit on the number of combos
    Preconditions.checkArgument(Math.pow(perParam, numParams) <= MAX_COMBOS);

    int howManyCombos = 1;
    List<List<?>> paramRanges = new ArrayList<>(numParams);
    for (HyperParamValues<?> range : ranges) {
      List<?> values = range.getTrialValues(perParam);
      paramRanges.add(values);
      howManyCombos *= values.size();
    }

    List<List<?>> allCombinations = new ArrayList<>(howManyCombos);
    for (int combo = 0; combo < howManyCombos; combo++) {
      List<Object> combination = new ArrayList<>(numParams);
      for (int param = 0; param < numParams; param++) {
        int whichValueToTry = combo;
        for (int i = 0; i < param; i++) {
          whichValueToTry /= paramRanges.get(i).size();
        }
        whichValueToTry %= paramRanges.get(param).size();
        combination.add(paramRanges.get(param).get(whichValueToTry));
      }
      allCombinations.add(combination);
    }

    if (howMany >= howManyCombos) {
      Collections.shuffle(allCombinations);
      return allCombinations;
    }
    RandomDataGenerator rdg = new RandomDataGenerator(RandomManager.getRandom());
    int[] indices = rdg.nextPermutation(howManyCombos, howMany);
    List<List<?>> result = new ArrayList<>(indices.length);
    for (int i = 0; i < indices.length; i++) {
      result.add(allCombinations.get(i));
    }
    Collections.shuffle(result);
    return result;
  }

  /**
   * @param numParams number of different hyperparameters
   * @param candidates minimum number of candidates to be built
   * @return smallest value such that pow(value, numParams) is at least the number of candidates
   *  requested to build. Returns 0 if numParams is less than 1.
   */
  public static int chooseValuesPerHyperParam(int numParams, int candidates) {
    if (numParams < 1) {
      return 0;
    }
    int valuesPerHyperParam = 0;
    int total;
    do {
      valuesPerHyperParam++;
      total = 1;
      for (int i = 0; i < numParams; i++) {
        total *= valuesPerHyperParam;
      }
    } while (total < candidates);
    return valuesPerHyperParam;
  }

}
