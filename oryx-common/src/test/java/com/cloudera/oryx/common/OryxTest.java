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
  public final void initRandom() {
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
  protected static void assertEquals(float expected, float actual) {
    Assert.assertEquals(expected, actual, FLOAT_EPSILON);
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
    Assert.assertEquals(expected, actual, DOUBLE_EPSILON);
  }

  protected static void assertArrayEquals(float[] expecteds, float[] actuals) {
    Assert.assertArrayEquals(expecteds, actuals, FLOAT_EPSILON);
  }

  protected static void assertArrayEquals(double[] expecteds, double[] actuals) {
    Assert.assertArrayEquals(expecteds, actuals, DOUBLE_EPSILON);
  }

}
