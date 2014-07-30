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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class HyperParamRangeTest extends OryxTest {

  @Test
  public void testFixedContinuous() {
    doTestContinuous(HyperParamRanges.fixed(3.0), 1,
                     3.0);
    doTestContinuous(HyperParamRanges.fixed(3.0), 3,
                     3.0);
  }

  @Test
  public void testContinuousRange() {
    doTestContinuous(HyperParamRanges.range(3.0, 5.0), 1,
                     4.0);
    doTestContinuous(HyperParamRanges.range(3.0, 5.0), 2,
                     3.0, 5.0);
    doTestContinuous(HyperParamRanges.range(3.0, 5.0), 4,
                     3.0, 3.6666666666666667, 4.3333333333333333, 5.0);
    doTestContinuous(HyperParamRanges.range(0.0, 1.0), 3,
                     0.0, 0.5, 1.0);
    doTestContinuous(HyperParamRanges.range(-1.0, 1.0), 5,
                     -1.0, -0.5, 0.0, 0.5, 1.0);
    doTestContinuous(HyperParamRanges.range(-1.0, 1.0), 4,
                     -1.0, -0.3333333333333333, 0.3333333333333333, 1.0);
  }

  @Test
  public void testAroundContinuous() {
    doTestContinuous(HyperParamRanges.around(-3.0, 0.1), 1,
                     -3.0);
    doTestContinuous(HyperParamRanges.around(-3.0, 0.1), 2,
                     -3.05, -2.95);
    doTestContinuous(HyperParamRanges.around(-3.0, 0.1), 3,
                     -3.1, -3.0, -2.9);
  }

  @Test
  public void testFixedDiscrete() {
    doTestDiscrete(HyperParamRanges.fixed(3), 1,
                   3);
    doTestDiscrete(HyperParamRanges.fixed(3), 3,
                   3);
  }

  @Test
  public void testDiscreteRange() {
    doTestDiscrete(HyperParamRanges.range(3, 4), 1,
                   3);
    doTestDiscrete(HyperParamRanges.range(3, 5), 1,
                   4);
    doTestDiscrete(HyperParamRanges.range(3, 5), 2,
                   3, 5);
    doTestDiscrete(HyperParamRanges.range(3, 5), 3,
                   3, 4, 5);
    doTestDiscrete(HyperParamRanges.range(3, 5), 4,
                   3, 4, 5);
    doTestDiscrete(HyperParamRanges.range(0, 1), 3,
                   0, 1);
    doTestDiscrete(HyperParamRanges.range(-1, 1), 5,
                   -1, 0, 1);
  }

  @Test
  public void testAroundDiscrete() {
    doTestDiscrete(HyperParamRanges.around(-3, 1), 1,
                   -3);
    doTestDiscrete(HyperParamRanges.around(-3, 1), 2,
                   -3, -2);
    doTestDiscrete(HyperParamRanges.around(-3, 1), 3,
                   -4, -3, -2);
    doTestDiscrete(HyperParamRanges.around(-3, 10), 2,
                   -8, 2);
    doTestDiscrete(HyperParamRanges.around(-3, 10), 3,
                   -13, -3, 7);
  }

  @Test
  public void testCombos() {
    List<List<Number>> combos = HyperParamRanges.chooseHyperParameterCombos(Arrays.asList(
        HyperParamRanges.fixed(1.0),
        HyperParamRanges.range(2, 10),
        HyperParamRanges.around(5.0, 0.5)
    ), 50, 2);
    assertEquals(4, combos.size());
    assertTrue(combos.contains(Arrays.asList(1.0, 2, 4.75)));
    assertTrue(combos.contains(Arrays.asList(1.0, 10, 4.75)));
    assertTrue(combos.contains(Arrays.asList(1.0, 2, 5.25)));
    assertTrue(combos.contains(Arrays.asList(1.0, 10, 5.25)));
  }

  @Test
  public void testCombos2() {
    List<List<Number>> combos = HyperParamRanges.chooseHyperParameterCombos(Arrays.asList(
        HyperParamRanges.fixed(1.0),
        HyperParamRanges.range(2, 10),
        HyperParamRanges.around(5.0, 0.5)
    ), 2, 2);
    assertEquals(2, combos.size());
    assertTrue(combos.contains(Arrays.asList(1.0, 10, 4.75)));
    assertTrue(combos.contains(Arrays.asList(1.0, 2, 4.75)));
  }

  @Test
  public void testNoCombos() {
    List<List<Number>> combos = HyperParamRanges.chooseHyperParameterCombos(
        Collections.<HyperParamRange>emptyList(), 1, 0);
    assertEquals(1, combos.size());
    assertTrue(combos.get(0).isEmpty());
  }

  @Test
  public void testConfig() {
    Map<String,String> overlay = new HashMap<>();
    overlay.put("a", "1");
    overlay.put("b", "2.7");
    overlay.put("c", "[3,4]");
    overlay.put("d", "[5.3,6.6]");
    Config config = ConfigUtils.overlayOn(overlay, ConfigUtils.getDefault());
    doTestDiscrete(HyperParamRanges.fromConfig(config, "a"), 1, 1);
    doTestContinuous(HyperParamRanges.fromConfig(config, "b"), 1, 2.7);
    doTestDiscrete(HyperParamRanges.fromConfig(config, "c"), 2, 3, 4);
    doTestContinuous(HyperParamRanges.fromConfig(config, "d"), 2, 5.3, 6.6);
  }

  private static void doTestContinuous(HyperParamRange range, int howMany, double... expected) {
    List<Number> values = range.getTrialValues(howMany);
    double[] valueArray = new double[values.size()];
    for (int i = 0; i < valueArray.length; i++) {
      valueArray[i] = values.get(i).doubleValue();
    }
    assertArrayEquals(expected, valueArray);
  }

  private static void doTestDiscrete(HyperParamRange range, int howMany, int... expected) {
    List<Number> values = range.getTrialValues(howMany);
    int[] valueArray = new int[values.size()];
    for (int i = 0; i < valueArray.length; i++) {
      valueArray[i] = values.get(i).intValue();
    }
    assertArrayEquals(expected, valueArray);
  }

}
