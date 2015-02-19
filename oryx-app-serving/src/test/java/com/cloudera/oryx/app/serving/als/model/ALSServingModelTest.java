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

package com.cloudera.oryx.app.serving.als.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class ALSServingModelTest extends OryxTest {

  @Test
  public void testUserItemVector() {
    ALSServingModel model = new ALSServingModel(2, true, null);
    assertEquals(2, model.getFeatures());
    assertTrue(model.isImplicit());
    assertNull(model.getRescorerProvider());
    model.setUserVector("U1", new float[] { 1.5f, -2.5f });
    assertArrayEquals(new float[] { 1.5f, -2.5f }, model.getUserVector("U1"));
    model.setItemVector("I0", new float[]{0.5f, 0.0f});
    assertArrayEquals(new float[] { 0.5f, 0.0f }, model.getItemVector("I0"));
    assertEquals(Arrays.asList("U1"), model.getAllUserIDs());
    assertEquals(Arrays.asList("I0"),   model.getAllItemIDs());
  }

  @Test
  public void testKnownItems() {
    ALSServingModel model = new ALSServingModel(2, true, null);
    populateKnownItems(model);
    assertEquals(new HashSet<>(Arrays.asList("I0", "I1")), model.getKnownItems("U0"));
    assertEquals(new HashSet<>(Arrays.asList("I0", "I1", "I2")), model.getKnownItems("U1"));
    assertEquals(new HashSet<>(Arrays.asList("I8", "I9")), model.getKnownItems("U9"));
    Map<String,Integer> userCounts = model.getUserCounts();
    assertEquals(2, userCounts.get("U0").intValue());
    assertEquals(3, userCounts.get("U1").intValue());
    assertEquals(2, userCounts.get("U9").intValue());
    Map<String,Integer> itemCounts = model.getItemCounts();
    assertEquals(2, itemCounts.get("I0").intValue());
    assertEquals(3, itemCounts.get("I1").intValue());
    assertEquals(2, itemCounts.get("I9").intValue());
  }

  @Test
  public void testPruneXY() {
    ALSServingModel model = new ALSServingModel(2, true, null);

    model.setUserVector("U0", new float[] { 1.0f, 1.0f });
    model.pruneX(Collections.<String>emptyList());
    // Protected because of recent user/items
    assertNotNull(model.getUserVector("U0"));
    model.pruneX(Collections.<String>emptyList());
    assertNull(model.getUserVector("U0"));

    model.setUserVector("U0", new float[] { 1.0f, 1.0f });
    model.pruneX(Arrays.asList("U0"));
    assertNotNull(model.getUserVector("U0"));
    model.pruneX(Arrays.asList("U0"));
    assertNotNull(model.getUserVector("U0"));

    model.setItemVector("I0", new float[]{1.0f, 1.0f});
    model.pruneY(Collections.<String>emptyList());
    // Protected because of recent user/items
    assertNotNull(model.getItemVector("I0"));
    model.pruneY(Collections.<String>emptyList());
    assertNull(model.getItemVector("I0"));

    model.setItemVector("I0", new float[] { 1.0f, 1.0f });
    model.pruneY(Arrays.asList("I0"));
    assertNotNull(model.getItemVector("I0"));
    model.pruneY(Arrays.asList("I0"));
    assertNotNull(model.getItemVector("I0"));
  }

  @Test
  public void testPruneKnown() {
    ALSServingModel model = new ALSServingModel(2, true, null);
    populateKnownItems(model);
    for (int i = 0; i < 10; i++) {
      model.setUserVector("U" + i, new float[] { 0.0f, 0.0f });
      model.setItemVector("I" + i, new float[]{0.0f, 0.0f});
    }
    model.pruneKnownItems(Arrays.asList("U4", "U5", "U6"), Arrays.asList("I4", "I5", "I6"));
    assertTrue(model.getKnownItems("U3").contains("I4"));
    assertTrue(model.getKnownItems("U4").contains("I4"));
    assertTrue(model.getKnownItems("U6").contains("I6"));
    assertTrue(model.getKnownItems("U6").contains("I7"));
    // Protected because of recent user/items
    assertTrue(model.getKnownItems("U2").contains("I2"));

    // Clears recent user/items
    model.pruneX(Collections.<String>emptyList());
    model.pruneY(Collections.<String>emptyList());

    model.pruneKnownItems(Arrays.asList("U4", "U5", "U6"), Arrays.asList("I4", "I5", "I6"));
    assertNull(model.getKnownItems("U3"));
    assertTrue(model.getKnownItems("U4").contains("I4"));
    assertTrue(model.getKnownItems("U6").contains("I6"));
    assertFalse(model.getKnownItems("U6").contains("I7"));
    assertNull(model.getKnownItems("U2"));
  }

  @Test
  public void testToString() {
    ALSServingModel model = new ALSServingModel(2, true, null);
    assertEquals("ALSServingModel[features:2, implicit:true, X:(0 users), Y:(0 items)]", model.toString());
  }

  private static void populateKnownItems(ALSServingModel model) {
    for (int i = 0; i < 10; i++) {
      String userID = "U" + i;
      for (int j = 0; j < 10; j++) {
        if (Math.abs(i - j) <= 1) {
          String itemID = "I" + j;
          model.addKnownItems(userID, Collections.singleton(itemID));
        }
      }
    }
  }



}
