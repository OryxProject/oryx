/*
 * Copyright (c) 2017, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.oryx.common.OryxTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class GridSearchTest extends OryxTest {

  @Test
  public void testCombos() {
    List<HyperParamValues<?>> hyperParams = new ArrayList<>();
    hyperParams.add(HyperParams.fixed(1.0));
    hyperParams.add(HyperParams.range(2, 10));
    hyperParams.add(HyperParams.around(5.0, 0.5));
    List<List<?>> combos = GridSearch.chooseHyperParameterCombos(hyperParams, 4);
    assertEquals(4, combos.size());
    assertContains(combos, Arrays.<Number>asList(1.0, 2, 4.75));
    assertContains(combos, Arrays.<Number>asList(1.0, 10, 4.75));
    assertContains(combos, Arrays.<Number>asList(1.0, 2, 5.25));
    assertContains(combos, Arrays.<Number>asList(1.0, 10, 5.25));
  }

  @Test
  public void testCombos2() {
    List<HyperParamValues<?>> hyperParams = new ArrayList<>();
    hyperParams.add(HyperParams.fixed(1.0));
    hyperParams.add(HyperParams.range(2, 10));
    hyperParams.add(HyperParams.around(5.0, 0.5));
    List<List<?>> combos = GridSearch.chooseHyperParameterCombos(hyperParams, 2);
    assertEquals(2, combos.size());
    assertContains(combos, Arrays.<Number>asList(1.0, 10, 4.75));
    assertContains(combos, Arrays.<Number>asList(1.0, 2, 4.75));
  }

  @Test
  public void testNoCombos() {
    List<List<?>> combos = GridSearch.chooseHyperParameterCombos(Collections.emptyList(), 1);
    assertEquals(1, combos.size());
    assertEquals(0, combos.get(0).size());
  }

}
