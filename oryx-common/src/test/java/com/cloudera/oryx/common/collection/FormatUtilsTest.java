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

package com.cloudera.oryx.common.collection;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class FormatUtilsTest extends OryxTest {

  @Test
  public void testFormat() {
    assertEquals("", FormatUtils.formatFloatVec());
    assertEquals("-1.0,2.5,3.05", FormatUtils.formatFloatVec(-1.0f, 2.5f, 3.05f));
    assertEquals("", FormatUtils.formatDoubleVec());
    assertEquals("-1.0,2.5,3.05", FormatUtils.formatDoubleVec(-1.0, 2.5, 3.05));
  }

  @Test
  public void testParse() {
    assertArrayEquals(new float[] {-1.0f, 2.5f, 3.05f}, FormatUtils.parseFloatVec("-1.0,2.5,3.05"));
    assertArrayEquals(new double[] {-1.0, 2.5, 3.05}, FormatUtils.parseDoubleVec("-1.0,2.5,3.05"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPosInfinityF() {
    FormatUtils.parseFloatVec("Infinity");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPosInfinityD() {
    FormatUtils.parseDoubleVec("Infinity");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegInfinityF() {
    FormatUtils.parseFloatVec("-Infinity");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegInfinityD() {
    FormatUtils.parseDoubleVec("-Infinity");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNanF() {
    FormatUtils.parseFloatVec("NaN");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNanD() {
    FormatUtils.parseDoubleVec("NaN");
  }

}
