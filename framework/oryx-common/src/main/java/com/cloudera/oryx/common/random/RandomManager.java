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

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well19937c;

/**
 * Manages random number generation. Allows resetting RNGs to a known state for testing.
 */
public final class RandomManager {

  private static final long TEST_SEED = getTestSeed();

  private static final Reference<? extends Collection<RandomGenerator>> INSTANCES =
      new SoftReference<>(new ArrayList<>());
  private static boolean useTestSeed;

  private RandomManager() {}

  private static long getTestSeed() {
    String seedString = System.getProperty("oryx.test.seed", "1234567890123456789");
    try {
      return Long.parseLong(seedString);
    } catch (NumberFormatException nfe) {
      return Long.parseLong(seedString, 16);
    }
  }

  /**
   * @return a new, seeded {@link RandomGenerator}
   */
  public static RandomGenerator getRandom() {
    if (useTestSeed) {
      // No need to track instances anymore
      return new Well19937c(TEST_SEED);
    }
    return getUnseededRandom();
  }

  static RandomGenerator getUnseededRandom() {
    RandomGenerator random = new Well19937c();
    Collection<RandomGenerator> instances = INSTANCES.get();
    if (instances != null) {
      synchronized (instances) {
        instances.add(random);
      }
    } // else oh well, only matters in tests
    return random;
  }

  /**
   * @param seed explicit seed for random number generator
   * @return a new, seeded {@link RandomGenerator}
   */
  public static RandomGenerator getRandom(long seed) {
    // Don't track these or use the test seed as the caller has manually specified
    // the seeding behavior
    return new Well19937c(seed);
  }

  /**
   * <em>Only call in test code.</em> Causes all known instances of {@link RandomGenerator},
   * and future ones, to be started from a fixed seed. This is useful for making
   * tests deterministic.
   */
  public static void useTestSeed() {
    useTestSeed = true;
    Collection<RandomGenerator> instances = INSTANCES.get();
    if (instances != null) {
      synchronized (instances) {
        instances.forEach(random -> random.setSeed(TEST_SEED));
        instances.clear();
      }
      INSTANCES.clear();
    }
  }

}
