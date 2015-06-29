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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.serving.als.CosineAverageFunction;
import com.cloudera.oryx.app.serving.als.DotsFunction;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.random.RandomManager;

public final class ALSServingModelTest extends OryxTest {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModelTest.class);

  @Test
  public void testUserItemVector() {
    ALSServingModel model = new ALSServingModel(2, true, 1.0, null);
    assertEquals(2, model.getFeatures());
    assertTrue(model.isImplicit());
    assertNull(model.getRescorerProvider());
    model.setUserVector("U1", new float[] { 1.5f, -2.5f });
    assertArrayEquals(new float[] { 1.5f, -2.5f }, model.getUserVector("U1"));
    model.setItemVector("I0", new float[]{0.5f, 0.0f});
    assertArrayEquals(new float[] { 0.5f, 0.0f }, model.getItemVector("I0"));
    assertContainsSame(Arrays.asList("U1"), model.getAllUserIDs());
    assertContainsSame(Arrays.asList("I0"), model.getAllItemIDs());
  }

  @Test
  public void testKnownItems() {
    ALSServingModel model = new ALSServingModel(2, true, 1.0, null);
    populateKnownItems(model);
    assertContainsSame(Arrays.asList("I0", "I1"), model.getKnownItems("U0"));
    assertContainsSame(Arrays.asList("I0", "I1", "I2"), model.getKnownItems("U1"));
    assertContainsSame(Arrays.asList("I8", "I9"), model.getKnownItems("U9"));
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
    ALSServingModel model = new ALSServingModel(2, true, 1.0, null);

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

    model.setItemVector("I0", new float[]{1.0f, 1.0f});
    model.pruneY(Arrays.asList("I0"));
    assertNotNull(model.getItemVector("I0"));
    model.pruneY(Arrays.asList("I0"));
    assertNotNull(model.getItemVector("I0"));
  }

  @Test
  public void testPruneKnown() {
    ALSServingModel model = new ALSServingModel(2, true, 1.0, null);
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
    String modelToString = new ALSServingModel(2, true, 1.0, null).toString();
    assertTrue(modelToString.contains("ALSServingModel"));
    assertTrue(modelToString.contains("features:2"));
    assertTrue(modelToString.contains("implicit:true"));
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

  @Test
  public void testLSHEffect() {
    RandomGenerator random = RandomManager.getRandom();
    PoissonDistribution itemPerUserDist = new PoissonDistribution(
        random,
        20,
        PoissonDistribution.DEFAULT_EPSILON,
        PoissonDistribution.DEFAULT_MAX_ITERATIONS);
    int features = 20;
    ALSServingModel mainModel = new ALSServingModel(features, true, 1.0, null);
    ALSServingModel lshModel = new ALSServingModel(features, true, 0.5, null);

    int userItemCount = 20000;
    for (int user = 0; user < userItemCount; user++) {
      String userID = "U" + user;
      float[] vec = VectorMath.randomVectorF(features, random);
      mainModel.setUserVector(userID, vec);
      lshModel.setUserVector(userID, vec);
      int itemsPerUser = itemPerUserDist.sample();
      Collection<String> knownIDs = new ArrayList<>(itemsPerUser);
      for (int i = 0; i < itemsPerUser; i++) {
        knownIDs.add("I" + random.nextInt(userItemCount));
      }
      mainModel.addKnownItems(userID, knownIDs);
      lshModel.addKnownItems(userID, knownIDs);
    }

    for (int item = 0; item < userItemCount; item++) {
      String itemID = "I" + item;
      float[] vec = VectorMath.randomVectorF(features, random);
      mainModel.setItemVector(itemID, vec);
      lshModel.setItemVector(itemID, vec);
    }

    int numRecs = 10;
    Mean meanMatchLength = new Mean();
    for (int user = 0; user < userItemCount; user++) {
      String userID = "U" + user;
      List<Pair<String,Double>> mainRecs =
          mainModel.topN(new DotsFunction(mainModel.getUserVector(userID)), null, numRecs, null);
      List<Pair<String,Double>> lshRecs =
          lshModel.topN(new DotsFunction(lshModel.getUserVector(userID)), null, numRecs, null);
      int i = 0;
      while (i < lshRecs.size() && i < mainRecs.size() && lshRecs.get(i).equals(mainRecs.get(i))) {
        i++;
      }
      meanMatchLength.increment(i);
    }
    log.info("Mean matching prefix: {}", meanMatchLength.getResult());
    assertTrue(meanMatchLength.getResult() >= 4.0);

    meanMatchLength.clear();
    for (int item = 0; item < userItemCount; item++) {
      String itemID = "I" + item;
      List<Pair<String,Double>> mainRecs =
          mainModel.topN(new CosineAverageFunction(mainModel.getItemVector(itemID)), null, numRecs, null);
      List<Pair<String,Double>> lshRecs =
          lshModel.topN(new CosineAverageFunction(lshModel.getItemVector(itemID)), null, numRecs, null);
      int i = 0;
      while (i < lshRecs.size() && i < mainRecs.size() && lshRecs.get(i).equals(mainRecs.get(i))) {
        i++;
      }
      meanMatchLength.increment(i);
    }
    log.info("Mean matching prefix: {}", meanMatchLength.getResult());
    assertTrue(meanMatchLength.getResult() >= 5.0);
  }

}
