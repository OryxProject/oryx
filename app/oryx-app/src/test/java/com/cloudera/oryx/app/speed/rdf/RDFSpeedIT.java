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

package com.cloudera.oryx.app.speed.rdf;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.lambda.speed.AbstractSpeedIT;

public final class RDFSpeedIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(RDFSpeedIT.class);

  private static final int NUM_INPUT = 500;

  @Test
  public void testRDFSpeedRegression() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.speed.model-manager-class", RDFSpeedModelManager.class.getName());
    overlayConfig.put("oryx.speed.streaming.generation-interval-sec", 10);
    overlayConfig.put("oryx.input-schema.feature-names", "[\"foo\",\"bar\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    overlayConfig.put("oryx.input-schema.target-feature", "bar");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<KeyMessage<String,String>> updates =
        startServerProduceConsumeTopics(config,
                                        new MockRDFRegressionInputGenerator(),
                                        new MockRDFRegressionModelGenerator(),
                                        NUM_INPUT,
                                        1);

    if (log.isDebugEnabled()) {
      updates.forEach(update -> log.debug("{}", update));
    }

    int numUpdates = updates.size();

    // Model, and then pairs of positive / negative
    assertGreaterOrEqual(numUpdates, 3);
    assertNotEquals(0, numUpdates % 2);
    // Not testing the model much here:
    assertEquals("MODEL", updates.get(0).getKey());

    for (int i = 1; i < numUpdates; i++) {
      KeyMessage<String, String> update = updates.get(i);
      assertEquals("UP", update.getKey());
      List<?> fields = TextUtils.readJSON(update.getMessage(), List.class);
      int treeID = (Integer) fields.get(0);
      String nodeID = fields.get(1).toString();
      double mean = (Double) fields.get(2);
      int count = (Integer) fields.get(3);
      assertEquals(0, treeID);
      assertContains(Arrays.asList("r-", "r+"), nodeID);
      double[] minMax = minMaxExpectedMean(count, "r+".equals(nodeID));
      assertRange(mean, minMax[0] - DOUBLE_EPSILON, minMax[1] + DOUBLE_EPSILON);
    }

    for (int i = 1; i < numUpdates; i += 2) {
      KeyMessage<String, String> update1 = updates.get(i);
      KeyMessage<String, String> update2 = updates.get(i + 1);
      List<?> fields1 = TextUtils.readJSON(update1.getMessage(), List.class);
      List<?> fields2 = TextUtils.readJSON(update2.getMessage(), List.class);
      int count1 = (Integer) fields1.get(3);
      int count2 = (Integer) fields2.get(3);
      assertLessOrEqual(Math.abs(count1 - count2), 1);
      String nodeID1 = fields1.get(1).toString();
      String nodeID2 = fields2.get(1).toString();
      if ("r-".equals(nodeID1)) {
        assertEquals("r+", nodeID2);
      } else {
        assertEquals("r-", nodeID2);
      }
    }

  }

  private static double[] minMaxExpectedMean(int n, boolean positive) {
    double minTotal = 0.0;
    double maxTotal = 0.0;
    int maxOffset = 5 - n % 5;
    for (int i = 0; i < n; i++) {
      if (positive) {
        minTotal += 1 + 2 * (i % 5);
        maxTotal += 1 + 2 * ((i + maxOffset) % 5);
      } else {
        minTotal += -2 * ((i + maxOffset) % 5);
        maxTotal += -2 * (i % 5);
      }
    }
    return new double[] { minTotal / n, maxTotal / n };
  }

  @Test
  public void testRDFSpeedClassification() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.speed.model-manager-class", RDFSpeedModelManager.class.getName());
    overlayConfig.put("oryx.speed.streaming.generation-interval-sec", 5);
    overlayConfig.put("oryx.input-schema.feature-names", "[\"color\",\"fruit\"]");
    overlayConfig.put("oryx.input-schema.numeric-features", "[]");
    overlayConfig.put("oryx.input-schema.target-feature", "fruit");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<KeyMessage<String,String>> updates =
        startServerProduceConsumeTopics(config,
                                        new MockRDFClassificationInputGenerator(),
                                        new MockRDFClassificationModelGenerator(),
                                        NUM_INPUT,
                                        1);

    if (log.isDebugEnabled()) {
      updates.forEach(update -> log.debug("{}", update));
    }

    int numUpdates = updates.size();

    // Model, and then pairs of positive / negative
    assertGreaterOrEqual(numUpdates, 3);
    assertNotEquals(0, numUpdates % 2);
    // Not testing the model much here:
    assertEquals("MODEL", updates.get(0).getKey());

    PMML pmml = PMMLUtils.fromString(updates.get(0).getMessage());
    CategoricalValueEncodings encodings =
        AppPMMLUtils.buildCategoricalValueEncodings(pmml.getDataDictionary());
    log.info("{}", encodings);
    Map<String,Integer> fruitEncoding = encodings.getValueEncodingMap(0);
    String red = Integer.toString(fruitEncoding.get("red"));
    String yellow = Integer.toString(fruitEncoding.get("yellow"));

    for (int i = 1; i < numUpdates; i++) {
      KeyMessage<String, String> update = updates.get(i);
      assertEquals("UP", update.getKey());
      List<?> fields = TextUtils.readJSON(update.getMessage(), List.class);
      int treeID = (Integer) fields.get(0);
      String nodeID = fields.get(1).toString();
      @SuppressWarnings("unchecked")
      Map<String,Integer> countMap = (Map<String,Integer>) fields.get(2);
      assertEquals(0, treeID);
      assertContains(Arrays.asList("r-", "r+"), nodeID);
      int yellowCount = countMap.getOrDefault(yellow, 0);
      int redCount = countMap.getOrDefault(red, 0);
      int count = yellowCount + redCount;
      assertGreater(count, 0);
      IntegerDistribution dist = new BinomialDistribution(RandomManager.getRandom(), count, 0.9);
      if ("r+".equals(nodeID)) {
        // Should be about 9x more yellow
        checkDiscreteProbability(yellowCount, dist);
      } else {
        // Should be about 9x more red
        checkDiscreteProbability(redCount, dist);
      }
    }

  }

}
