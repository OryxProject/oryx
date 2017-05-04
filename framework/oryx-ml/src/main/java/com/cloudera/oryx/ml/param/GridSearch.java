/*
 * Copyright (c) 2017, Cloudera and Intel, Inc. All Rights Reserved.
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

import com.cloudera.oryx.common.random.RandomManager;
import com.google.common.base.Preconditions;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class GridSearch {

  private static final int MAX_COMBOS = 65536;
  
  private GridSearch() {}

  /**
   * @param ranges ranges of hyperparameters to try, one per hyperparameters
   * @param howMany how many combinations of hyperparameters to return
   * @return combinations of concrete hyperparameter values. For example, for 5 parameters each
   *  with 3 values to try, the total number of combinations returned could be up to pow(3,5)
   *  or 243. The number could be less if the ranges do not actually have that many distinct
   *  values. If {@code howMany} is smaller than the total number of combinations, a random
   *  subset of all combinations are returned. The order is shuffled randomly. If no parameters
   *  are specified or {@code perParam} is 0, a single empty combination is returned.
   */
  static List<List<?>> chooseHyperParameterCombos(List<HyperParamValues<?>> ranges, int howMany) {
    // Put some reasonable upper limit on the number of combos
    Preconditions.checkArgument(howMany > 0 && howMany <= MAX_COMBOS);
    
    int numParams = ranges.size();
    int perParam = chooseValuesPerHyperParam(ranges, howMany);
    if (numParams == 0 || perParam == 0) {
      return Collections.singletonList(Collections.emptyList());
    }
    
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
   * @param ranges ranges of hyperparameters to try, one per hyperparameters
   * @param candidates minimum number of candidates to be built
   * @return smallest value such that picking that many values from each hyperparameter would result
   *  in at least the requested minimum number of candidate combinations
   */
  private static int chooseValuesPerHyperParam(List<HyperParamValues<?>> ranges, int candidates) {
    if (ranges.isEmpty()) {
      return 0;
    }
    int valuesPerHyperParam = 0;
    long lastTotal;
    long total = 0;
    do {
      valuesPerHyperParam++;
      lastTotal = total;
      total = 1;
      for (HyperParamValues<?> range : ranges) {
        total *= Math.min(valuesPerHyperParam, range.getNumDistinctValues());
      }
      // Keep going until values-per-hyperparam is enough to generate enough candidates,
      // but stop early if it won't increase anyway because there aren't enough distinct values
    } while (total < candidates && total > lastTotal);
    return valuesPerHyperParam;
  }
  
}
