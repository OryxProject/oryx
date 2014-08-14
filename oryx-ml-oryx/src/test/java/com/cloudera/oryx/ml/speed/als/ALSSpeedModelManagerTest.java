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

package com.cloudera.oryx.ml.speed.als;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class ALSSpeedModelManagerTest extends OryxTest {

  @Test
  public void testDot() {
    float[] a = { 1.0f, 0.5f, -3.5f };
    float[] b = { 0.0f, -10.3f, -3.0f };
    assertEquals(5.35, ALSSpeedModelManager.dot(a, b), FLOAT_EPSILON);
  }

}
