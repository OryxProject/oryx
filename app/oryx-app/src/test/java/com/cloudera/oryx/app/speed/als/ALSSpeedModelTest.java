/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.speed.als;

import java.util.Collections;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class ALSSpeedModelTest extends OryxTest {

  @Test
  public void testUserItemVector() {
    ALSSpeedModel model = new ALSSpeedModel(2, true);
    assertEquals(2, model.getFeatures());
    model.setUserVector("U1", new float[] { 1.5f, -2.5f });
    assertArrayEquals(new float[] { 1.5f, -2.5f }, model.getUserVector("U1"));
    model.setItemVector("I0", new float[]{0.5f, 0.0f});
    assertArrayEquals(new float[] { 0.5f, 0.0f }, model.getItemVector("I0"));
  }

  @Test
  public void testToString() {
    String modelToString = new ALSSpeedModel(2, true).toString();
    assertTrue(modelToString.contains("ALSSpeedModel"));
    assertTrue(modelToString.contains("features:2"));
    assertTrue(modelToString.contains("implicit:true"));
  }

  @Test
  public void testFractionLoaded() {
    assertEquals(1.0f, new ALSSpeedModel(2, true).getFractionLoaded());
    ALSSpeedModel model = new ALSSpeedModel(2, true);
    model.retainRecentAndUserIDs(Collections.singleton("U1"));
    model.retainRecentAndItemIDs(Collections.singleton("I0"));
    assertEquals(0.0f, model.getFractionLoaded());
    model.setUserVector("U1", new float[] { 1.5f, -2.5f });
    assertEquals(0.5f, model.getFractionLoaded());
    model.setItemVector("I0", new float[]{0.5f, 0.0f});
    assertEquals(1.0f, model.getFractionLoaded());
  }

}
