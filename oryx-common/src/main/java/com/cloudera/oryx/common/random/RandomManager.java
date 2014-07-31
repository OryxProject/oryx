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

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.math3.random.MersenneTwister;
import org.apache.commons.math3.random.RandomGenerator;

/**
 * Manages random number generation. Allows resetting RNGs to a known state for testing.
 */
public final class RandomManager {

  private static final long TEST_SEED = 1234567890L;

  private static final Map<RandomGenerator,Boolean> INSTANCES = new WeakHashMap<>();
  private static boolean useTestSeed;

  private RandomManager() {
  }

  /**
   * @return a new, seeded {@link RandomGenerator}
   */
  public static RandomGenerator getRandom() {
    if (useTestSeed) {
      // No need to track instances anymore
      return new MersenneTwister(TEST_SEED);
    }
    RandomGenerator random = new MersenneTwister();
    synchronized (INSTANCES) {
      INSTANCES.put(random, Boolean.TRUE); // Value does not matter
    }
    return random;
  }

  /**
   * <em>Only call in test code.</em> Causes all known instances of {@link RandomGenerator}, and future ones,
   * to be started from a fixed seed. This is useful for making tests deterministic.
   */
  public static void useTestSeed() {
    useTestSeed = true;
    synchronized (INSTANCES) {
      for (RandomGenerator random : INSTANCES.keySet()) {
        random.setSeed(TEST_SEED);
      }
      INSTANCES.clear();
    }
  }

}
