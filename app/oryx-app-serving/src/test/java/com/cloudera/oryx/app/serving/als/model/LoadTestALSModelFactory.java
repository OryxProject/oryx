/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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
import java.util.Collection;

import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.serving.als.TestALSRescorerProvider;
import com.cloudera.oryx.common.lang.JVMUtils;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.random.RandomManager;

public final class LoadTestALSModelFactory {

  private static final Logger log = LoggerFactory.getLogger(LoadTestALSModelFactory.class);

  public static final int USERS =
      Integer.parseInt(System.getProperty("oryx.test.als.benchmark.users", "500000"));
  public static final int ITEMS =
      Integer.parseInt(System.getProperty("oryx.test.als.benchmark.items", "2000000"));
  public static final int WORKERS =
      Integer.parseInt(System.getProperty("oryx.test.als.benchmark.workers", "4"));
  public static final int REQS_PER_WORKER =
      Integer.parseInt(System.getProperty("oryx.test.als.benchmark.reqsPerWorker", "100"));
  private static final int FEATURES =
      Integer.parseInt(System.getProperty("oryx.test.als.benchmark.features", "100"));
  private static final int AVG_ITEMS_PER_USER =
      Integer.parseInt(System.getProperty("oryx.test.als.benchmark.avgItemsPerUser", "20"));
  private static final double LSH_SAMPLE_RATE =
      Double.parseDouble(System.getProperty("oryx.test.als.benchmark.lshSampleRate", "0.3"));

  private LoadTestALSModelFactory() {}

  public static ALSServingModel buildTestModel() {

    log.info("Building load test model...");

    System.gc();
    long startMemory = JVMUtils.getUsedMemory();

    RandomGenerator random = RandomManager.getRandom();
    PoissonDistribution itemPerUserDist = new PoissonDistribution(
        random,
        AVG_ITEMS_PER_USER,
        PoissonDistribution.DEFAULT_EPSILON,
        PoissonDistribution.DEFAULT_MAX_ITERATIONS);
    ALSServingModel model = new ALSServingModel(FEATURES, true, LSH_SAMPLE_RATE, new TestALSRescorerProvider());

    log.info("Adding {} users", USERS);
    long totalEntries = 0;
    for (int user = 0; user < USERS; user++) {
      String userID = "U" + user;
      model.setUserVector(userID, VectorMath.randomVectorF(FEATURES, random));
      int itemsPerUser = itemPerUserDist.sample();
      totalEntries += itemsPerUser;
      Collection<String> knownIDs = new ArrayList<>(itemsPerUser);
      for (int i = 0; i < itemsPerUser; i++) {
        knownIDs.add("I" + random.nextInt(ITEMS));
      }
      model.addKnownItems(userID, knownIDs);
    }

    log.info("Adding {} items", ITEMS);
    for (int item = 0; item < ITEMS; item++) {
      model.setItemVector("I" + item, VectorMath.randomVectorF(FEATURES, random));
    }

    System.gc();
    long endMemory = JVMUtils.getUsedMemory();

    log.info("Built model over {} users, {} items, {} features, {} entries, using {}MB",
        USERS, ITEMS, FEATURES,
        totalEntries,
        (endMemory - startMemory) / 1_000_000);
    log.info("Model: {}", model);
    return model;
  }

}
