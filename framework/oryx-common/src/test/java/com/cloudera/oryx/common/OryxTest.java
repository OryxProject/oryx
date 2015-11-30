/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.random.RandomManager;

/**
 * Superclass of all tests.
 */
public abstract class OryxTest extends Assert {

  private static final Logger log = LoggerFactory.getLogger(OryxTest.class);

  public static final float FLOAT_EPSILON = 1.0e-6f;
  public static final double DOUBLE_EPSILON = 1.0e-12;

  private Path tempDir;

  protected final Path getTempDir() throws IOException {
    if (tempDir == null) {
      tempDir = Files.createTempDirectory("OryxTest").toAbsolutePath();
      log.info("Test temp dir is {}", tempDir);
      tempDir.toFile().deleteOnExit();
    }
    return tempDir;
  }

  @Before
  public void initRandom() {
    RandomManager.useTestSeed();
  }

  @After
  public final void deleteTempDir() throws IOException {
    if (tempDir != null) {
      IOUtils.deleteRecursively(tempDir);
      tempDir = null;
    }
  }

  /**
   * Asserts that two {@code float} values are equal or very close -- that the absolute value of their
   * difference is at most {@link #FLOAT_EPSILON}.
   *
   * @param expected test's expected value
   * @param actual actual value
   */
  public static void assertEquals(float expected, float actual) {
    assertEquals(expected, actual, FLOAT_EPSILON);
  }

  public static void assertGreater(double a, double b) {
    Assert.assertTrue(a + " should be > " + b, a > b);
  }

  public static void assertGreaterOrEqual(double a, double b) {
    Assert.assertTrue(a + " should be >= " + b, a >= b);
  }

  public static void assertLess(double a, double b) {
    Assert.assertTrue(a + " should be < " + b, a < b);
  }

  public static void assertLessOrEqual(double a, double b) {
    Assert.assertTrue(a + " should be <= " + b, a <= b);
  }

  public static void assertRange(double a, double min, double max) {
    assertGreaterOrEqual(a, min);
    assertLessOrEqual(a, max);
  }

  public static void assertRange(int a, int min, int max) {
    Assert.assertTrue(a + " should be >= " + min, a >= min);
    Assert.assertTrue(a + " should be <= " + max, a <= max);
  }

  public static void assertElementIndex(int i, int size) {
    Assert.assertTrue(i + " should be >= 0", i >= 0);
    Assert.assertTrue(i + " should be < " + size, i < size);
  }

  public static <T> void assertContains(Collection<T> collection, T value) {
    assertTrue(abbreviatedToString(collection) + " should contain " + value, collection.contains(value));
  }

  public static <T> void assertNotContains(Collection<T> collection, T value) {
    assertFalse(abbreviatedToString(collection) + " should not contain " + value, collection.contains(value));
  }

  private static String abbreviatedToString(Collection<?> c) {
    return c.size() <= 16 ? c.toString() : Iterables.limit(c, 16) + "...";
  }

  /**
   * Asserts that two {@code double} values are equal or very close -- that the absolute value of their
   * difference is at most {@link #DOUBLE_EPSILON}.
   *
   * @param expected test's expected value
   * @param actual actual value
   */
  @SuppressWarnings("deprecation")
  public static void assertEquals(double expected, double actual) {
    assertEquals(expected, actual, DOUBLE_EPSILON);
  }

  protected static void assertArrayEquals(float[] expecteds, float[] actuals) {
    assertArrayEquals(expecteds, actuals, FLOAT_EPSILON);
  }

  protected static void assertArrayEquals(double[] expecteds, double[] actuals) {
    assertArrayEquals(expecteds, actuals, DOUBLE_EPSILON);
  }

  protected static <T> void assertContainsSame(Collection<T> expected, Collection<T> actual) {
    if (expected == null) {
      assertNull(actual);
    } else {
      assertNotNull(actual);
      assertEquals(expected.size(), actual.size());
      assertTrue(expected.containsAll(actual) && actual.containsAll(expected));
    }
  }

  protected static void assertNonEmpty(Path p) throws IOException {
    assertTrue("File should exist: " + p, Files.exists(p));
    assertTrue("File should not be empty: " + p, Files.size(p) > 0);
  }

  protected static void sleepSeconds(int seconds) {
    Preconditions.checkArgument(seconds >= 0);
    if (seconds > 0) {
      try {
        Thread.sleep(TimeUnit.MILLISECONDS.convert(seconds, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        log.warn("Interrupted while sleeping; continuing");
      }
    }
  }

}
