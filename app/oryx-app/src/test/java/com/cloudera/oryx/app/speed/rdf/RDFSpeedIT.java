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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.speed.AbstractSpeedIT;

public final class RDFSpeedIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(RDFSpeedIT.class);

  private static final int NUM_INPUT = 500;

  @Test
  public void testRDFSpeedRegression() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.speed.model-manager-class", RDFSpeedModelManager.class.getName());
    overlayConfig.put("oryx.speed.streaming.generation-interval-sec", 10);
    overlayConfig.put("oryx.speed.streaming.block-interval-sec", 1);
    overlayConfig.put("oryx.input-schema.feature-names", "[\"foo\",\"bar\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    overlayConfig.put("oryx.input-schema.target-feature", "bar");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String,String>> updates =
        startServerProduceConsumeTopics(config,
                                        new MockRDFRegressionInputGenerator(),
                                        new MockRDFRegressionModelGenerator(),
                                        NUM_INPUT,
                                        1);

    if (log.isDebugEnabled()) {
      for (Pair<String, String> update : updates) {
        log.debug("{}", update);
      }
    }

    int numUpdates = updates.size();

    // Model, and then pairs of positive / negative
    assertTrue(numUpdates >= 3 && numUpdates % 2 != 0);
    // Not testing the model much here:
    assertEquals("MODEL", updates.get(0).getFirst());

    for (int i = 1; i < numUpdates; i++) {
      Pair<String, String> update = updates.get(i);
      assertEquals("UP", update.getFirst());
      List<?> fields = MAPPER.readValue(update.getSecond(), List.class);
      int treeID = (Integer) fields.get(0);
      String nodeID = fields.get(1).toString();
      double mean = (Double) fields.get(2);
      int count = (Integer) fields.get(3);
      assertEquals(0, treeID);
      assertTrue("r-".equals(nodeID) || "r+".equals(nodeID));
      if ("r+".equals(nodeID)) {
        assertEquals(expectedPositiveMean(count), mean, 0.1);
      } else {
        assertEquals(expectedNegativeMean(count), mean, 0.1);
      }
    }

    for (int i = 1; i < numUpdates; i += 2) {
      Pair<String, String> update1 = updates.get(i);
      Pair<String, String> update2 = updates.get(i + 1);
      List<?> fields1 = MAPPER.readValue(update1.getSecond(), List.class);
      List<?> fields2 = MAPPER.readValue(update2.getSecond(), List.class);
      int count1 = (Integer) fields1.get(3);
      int count2 = (Integer) fields2.get(3);
      assertTrue(Math.abs(count1 - count2) <= 1);
      String nodeID1 = fields1.get(1).toString();
      String nodeID2 = fields2.get(1).toString();
      if ("r-".equals(nodeID1)) {
        assertEquals("r+", nodeID2);
      } else {
        assertEquals("r-", nodeID2);
      }
    }

  }

  private static double expectedPositiveMean(int n) {
    int total = 0;
    for (int i = 0; i < n; i++) {
      total += 1 + 2 * (i % 5);
    }
    return (double) total / n;
  }

  private static double expectedNegativeMean(int n) {
    int total = 0;
    for (int i = 0; i < n; i++) {
      total += -2 * (i % 5);
    }
    return (double) total / n;
  }

  @Test
  public void testRDFSpeedClassification() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.speed.model-manager-class", RDFSpeedModelManager.class.getName());
    overlayConfig.put("oryx.speed.streaming.generation-interval-sec", 5);
    overlayConfig.put("oryx.speed.streaming.block-interval-sec", 1);
    overlayConfig.put("oryx.input-schema.feature-names", "[\"color\",\"fruit\"]");
    overlayConfig.put("oryx.input-schema.numeric-features", "[]");
    overlayConfig.put("oryx.input-schema.target-feature", "fruit");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String,String>> updates =
        startServerProduceConsumeTopics(config,
                                        new MockRDFClassificationInputGenerator(),
                                        new MockRDFClassificationModelGenerator(),
                                        NUM_INPUT,
                                        1);

    if (log.isDebugEnabled()) {
      for (Pair<String, String> update : updates) {
        log.debug("{}", update);
      }
    }

    int numUpdates = updates.size();

    // Model, and then pairs of positive / negative
    assertTrue(numUpdates >= 3 && numUpdates % 2 != 0);
    // Not testing the model much here:
    assertEquals("MODEL", updates.get(0).getFirst());

    PMML pmml = PMMLUtils.fromString(updates.get(0).getSecond());
    CategoricalValueEncodings encodings =
        AppPMMLUtils.buildCategoricalValueEncodings(pmml.getDataDictionary());
    log.info("{}", encodings);
    Map<String,Integer> fruitEncoding = encodings.getValueEncodingMap(0);
    String red = Integer.toString(fruitEncoding.get("red"));
    String yellow = Integer.toString(fruitEncoding.get("yellow"));

    for (int i = 1; i < numUpdates; i++) {
      Pair<String, String> update = updates.get(i);
      assertEquals("UP", update.getFirst());
      List<?> fields = MAPPER.readValue(update.getSecond(), List.class);
      int treeID = (Integer) fields.get(0);
      String nodeID = fields.get(1).toString();
      @SuppressWarnings("unchecked")
      Map<String,Integer> countMap = (Map<String,Integer>) fields.get(2);
      assertEquals(0, treeID);
      assertTrue("r-".equals(nodeID) || "r+".equals(nodeID));
      int yellowCount = countMap.containsKey(yellow) ? countMap.get(yellow) : 0;
      int redCount = countMap.containsKey(red) ? countMap.get(red) : 0;
      int count = yellowCount + redCount;
      assertTrue(count > 0);
      BinomialDistribution dist = new BinomialDistribution(count, 0.9);
      if ("r+".equals(nodeID)) {
        // Should be about 9x more yellow
        checkProbability(yellowCount, count, dist);
      } else {
        // Should be about 9x more red
        checkProbability(redCount, count, dist);
      }
    }

  }

  private static void checkProbability(int majorityCount,
                                       int count,
                                       BinomialDistribution dist) {
    double expected = 0.9 * count;
    double probAsExtreme = majorityCount <= expected ?
        dist.cumulativeProbability(majorityCount) :
        (1.0 - dist.cumulativeProbability(majorityCount)) + dist.probability(majorityCount);
    assertTrue(majorityCount + " should be about " + expected + " (~90% of " + count + ")",
               probAsExtreme >= 0.001);
  }

}
