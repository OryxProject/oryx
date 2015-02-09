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

package com.cloudera.oryx.common.random;

import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class RandomManagerRandomTest extends OryxTest {

  @Override
  @Before
  public void initRandom() {
    // specifically don't init random
  }

  @Test
  public void testRandomState() {
    RandomGenerator generator = RandomManager.getRandom();
    double unseededValue = generator.nextDouble();
    RandomManager.useTestSeed();
    double seededValue = generator.nextDouble();
    assertNotEquals(unseededValue, seededValue);
    RandomManager.useTestSeed();
    assertEquals(seededValue, RandomManager.getRandom().nextDouble());
  }

}