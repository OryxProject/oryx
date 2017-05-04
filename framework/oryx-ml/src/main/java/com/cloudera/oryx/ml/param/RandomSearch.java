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

final class RandomSearch {
  
  private RandomSearch() {}

  /**
   * @param ranges ranges of hyperparameters to try, one per hyperparameters
   * @param howMany how many combinations of hyperparameters to return
   * @return combinations of concrete hyperparameter values
   */
  static List<List<?>> chooseHyperParameterCombos(List<HyperParamValues<?>> ranges, int howMany) {
    Preconditions.checkArgument(howMany > 0);

    int numParams = ranges.size();
    if (numParams == 0) {
      return Collections.singletonList(Collections.emptyList());
    }
    
    RandomDataGenerator rdg = new RandomDataGenerator(RandomManager.getRandom());
    List<List<?>> allCombinations = new ArrayList<>(howMany);
    for (int i = 0; i < howMany; i++) {
      List<Object> combination = new ArrayList<>(numParams);
      for (HyperParamValues<?> range : ranges) {
        combination.add(range.getRandomValue(rdg));
      }
      allCombinations.add(combination);
    }

    return allCombinations;
  }
  
}
