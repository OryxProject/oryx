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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class HyperParamsTest extends OryxTest {

  @Test
  public void testFixedContinuous() {
    doTestContinuous(HyperParams.fixed(3.0), 1,
                     3.0);
    doTestContinuous(HyperParams.fixed(3.0), 3,
                     3.0);
  }

  @Test
  public void testContinuousRange() {
    doTestContinuous(HyperParams.range(3.0, 5.0), 1,
                     4.0);
    doTestContinuous(HyperParams.range(3.0, 5.0), 2,
                     3.0, 5.0);
    doTestContinuous(HyperParams.range(3.0, 5.0), 4,
                     3.0, 3.6666666666666667, 4.3333333333333333, 5.0);
    doTestContinuous(HyperParams.range(0.0, 1.0), 3,
                     0.0, 0.5, 1.0);
    doTestContinuous(HyperParams.range(-1.0, 1.0), 5,
                     -1.0, -0.5, 0.0, 0.5, 1.0);
    doTestContinuous(HyperParams.range(-1.0, 1.0), 4,
                     -1.0, -0.3333333333333333, 0.3333333333333333, 1.0);
  }

  @Test
  public void testAroundContinuous() {
    doTestContinuous(HyperParams.around(-3.0, 0.1), 1,
                     -3.0);
    doTestContinuous(HyperParams.around(-3.0, 0.1), 2,
                     -3.05, -2.95);
    doTestContinuous(HyperParams.around(-3.0, 0.1), 3,
                     -3.1, -3.0, -2.9);
  }

  @Test
  public void testFixedDiscrete() {
    doTest(HyperParams.fixed(3), 1,
           Collections.singletonList(3));
    doTest(HyperParams.fixed(3), 3,
           Collections.singletonList(3));
  }

  @Test
  public void testDiscreteRange() {
    doTest(HyperParams.range(3, 4), 1,
           Collections.singletonList(3));
    doTest(HyperParams.range(3, 5), 1,
           Collections.singletonList(4));
    doTest(HyperParams.range(3, 5), 2,
           Arrays.asList(3, 5));
    doTest(HyperParams.range(3, 5), 3,
           Arrays.asList(3, 4, 5));
    doTest(HyperParams.range(3, 5), 4,
           Arrays.asList(3, 4, 5));
    doTest(HyperParams.range(0, 1), 3,
           Arrays.asList(0, 1));
    doTest(HyperParams.range(-1, 1), 5,
           Arrays.asList(-1, 0, 1));
    doTest(HyperParams.range(0, 10), 3,
           Arrays.asList(0, 5, 10));
  }

  @Test
  public void testAroundDiscrete() {
    doTest(HyperParams.around(-3, 1), 1,
           Collections.singletonList(-3));
    doTest(HyperParams.around(-3, 1), 2,
           Arrays.asList(-3, -2));
    doTest(HyperParams.around(-3, 1), 3,
           Arrays.asList(-4, -3, -2));
    doTest(HyperParams.around(-3, 10), 2,
           Arrays.asList(-8, 2));
    doTest(HyperParams.around(-3, 10), 3,
           Arrays.asList(-13, -3, 7));
  }

  @Test
  public void testUnordered() {
    doTest(HyperParams.unorderedFromValues(Arrays.asList("foo", "bar")), 1,
           Collections.singletonList("foo"));
    doTest(HyperParams.unorderedFromValues(Arrays.asList("foo", "bar")), 2,
           Arrays.asList("foo", "bar"));
    doTest(HyperParams.unorderedFromValues(Arrays.asList("foo", "bar")), 3,
           Arrays.asList("foo", "bar"));
  }

  @Test
  public void testCombos() {
    Collection<HyperParamValues<?>> hyperParams = new ArrayList<>();
    hyperParams.add(HyperParams.fixed(1.0));
    hyperParams.add(HyperParams.range(2, 10));
    hyperParams.add(HyperParams.around(5.0, 0.5));
    List<List<?>> combos = HyperParams.chooseHyperParameterCombos(hyperParams, 50, 2);
    assertEquals(4, combos.size());
    assertContains(combos, Arrays.<Number>asList(1.0, 2, 4.75));
    assertContains(combos, Arrays.<Number>asList(1.0, 10, 4.75));
    assertContains(combos, Arrays.<Number>asList(1.0, 2, 5.25));
    assertContains(combos, Arrays.<Number>asList(1.0, 10, 5.25));
  }

  @Test
  public void testCombos2() {
    Collection<HyperParamValues<?>> hyperParams = new ArrayList<>();
    hyperParams.add(HyperParams.fixed(1.0));
    hyperParams.add(HyperParams.range(2, 10));
    hyperParams.add(HyperParams.around(5.0, 0.5));
    List<List<?>> combos = HyperParams.chooseHyperParameterCombos(hyperParams, 2, 2);
    assertEquals(2, combos.size());
    assertContains(combos, Arrays.<Number>asList(1.0, 10, 4.75));
    assertContains(combos, Arrays.<Number>asList(1.0, 2, 4.75));
  }

  @Test
  public void testNoCombos() {
    List<List<?>> combos = HyperParams.chooseHyperParameterCombos(
        Collections.<HyperParamValues<?>>emptyList(), 1, 0);
    assertEquals(1, combos.size());
    assertTrue(combos.get(0).isEmpty());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testConfig() {
    Map<String,Object> overlay = new HashMap<>();
    overlay.put("a", 1);
    overlay.put("b", 2.7);
    overlay.put("c", "[3,4]");
    overlay.put("d", "[5.3,6.6]");
    Config config = ConfigUtils.overlayOn(overlay, ConfigUtils.getDefault());
    doTest((HyperParamValues<Integer>) HyperParams.fromConfig(config, "a"),
           1, Collections.singletonList(1));
    doTest((HyperParamValues<Double>) HyperParams.fromConfig(config, "b"),
           1, Collections.singletonList(2.7));
    doTest((HyperParamValues<Integer>) HyperParams.fromConfig(config, "c"),
           2, Arrays.asList(3, 4));
    doTest((HyperParamValues<Double>) HyperParams.fromConfig(config, "d"),
           2, Arrays.asList(5.3, 6.6));
  }

  @Test
  public void testChooseValues() {
    assertEquals(0, HyperParams.chooseValuesPerHyperParam(0, 1));
    assertEquals(1, HyperParams.chooseValuesPerHyperParam(1, 1));
    assertEquals(3, HyperParams.chooseValuesPerHyperParam(1, 3));
    assertEquals(1, HyperParams.chooseValuesPerHyperParam(2, 1));
    assertEquals(2, HyperParams.chooseValuesPerHyperParam(2, 2));
    assertEquals(2, HyperParams.chooseValuesPerHyperParam(2, 4));
    assertEquals(1, HyperParams.chooseValuesPerHyperParam(3, 1));
    assertEquals(2, HyperParams.chooseValuesPerHyperParam(3, 7));
    assertEquals(2, HyperParams.chooseValuesPerHyperParam(3, 8));
  }

  private static <T> void doTest(HyperParamValues<T> hyperParams,
                                 int howMany,
                                 List<T> expected) {
    assertEquals(expected, hyperParams.getTrialValues(howMany));
    assertNotNull(hyperParams.toString());
  }

  private static void doTestContinuous(HyperParamValues<Double> range,
                                       int howMany,
                                       double... expected) {
    List<Double> values = range.getTrialValues(howMany);
    double[] valueArray = new double[values.size()];
    for (int i = 0; i < valueArray.length; i++) {
      valueArray[i] = values.get(i);
    }
    assertArrayEquals(expected, valueArray);
    assertNotNull(range.toString());
  }

}
