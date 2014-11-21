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

package com.cloudera.oryx.ml;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.AbstractBatchIT;

/**
 * Tests {@link MLUpdate}.
 */
public final class SimpleMLUpdateIT extends AbstractBatchIT {

  private static final Logger log = LoggerFactory.getLogger(SimpleMLUpdateIT.class);

  private static final int DATA_TO_WRITE = 2400;
  private static final int WRITE_INTERVAL_MSEC = 25;
  private static final int GEN_INTERVAL_SEC = 20;
  private static final int BLOCK_INTERVAL_SEC = 1;
  private static final double TEST_FRACTION = 0.2;

  @Test
  public void testMLUpdate() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir = tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", MockMLUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.generation-interval-sec", Integer.toString(GEN_INTERVAL_SEC));
    overlayConfig.put("oryx.batch.block-interval-sec", Integer.toString(BLOCK_INTERVAL_SEC));
    overlayConfig.put("oryx.ml.eval.test-fraction", Double.toString(TEST_FRACTION));
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Integer> trainCounts = new ArrayList<>();
    List<Integer> testCounts = new ArrayList<>();

    MockMLUpdate.setCountHolders(trainCounts, testCounts);

    startServerProduceConsumeTopics(config, DATA_TO_WRITE, WRITE_INTERVAL_MSEC);

    // If lists are unequal at this point, there must have been an empty test set
    // which yielded no call to evaluate(). Fill in the blank
    while (trainCounts.size() > testCounts.size()) {
      testCounts.add(0);
    }

    log.info("trainCounts = {}", trainCounts);
    log.info("testCounts = {}", testCounts);

    checkOutputData(dataDir, DATA_TO_WRITE);
    checkIntervals(trainCounts.size(), DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    assertEquals(testCounts.size(), trainCounts.size());

    RandomGenerator random = RandomManager.getRandom();
    int lastTotalTrainCount = 0;
    int lastTestCount = 0;
    for (int i = 0; i < testCounts.size(); i++) {
      int totalTrainCount = trainCounts.get(i);
      int testCount = testCounts.get(i);
      int newTrainInGen = totalTrainCount - (lastTotalTrainCount + lastTestCount);
      if (newTrainInGen == 0) {
        continue;
      }
      lastTotalTrainCount = totalTrainCount;
      lastTestCount = testCount;
      int totalNew = testCount + newTrainInGen;

      IntegerDistribution dist = new BinomialDistribution(random, totalNew, TEST_FRACTION);
      double probability;
      if (testCount < dist.getNumericalMean()) {
        probability = dist.cumulativeProbability(testCount);
      } else {
        probability = 1.0 - dist.cumulativeProbability(testCount);
      }
      log.info("Probability of observing {} as {} sample of {}: {}",
               testCount, TEST_FRACTION, totalNew, probability);
      assertTrue(probability >= 0.001);
    }

  }

}
