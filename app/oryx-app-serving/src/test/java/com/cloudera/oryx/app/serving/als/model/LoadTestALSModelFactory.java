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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

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
      Integer.parseInt(System.getProperty("oryx.test.als.benchmark.reqsPerWorker", "1000"));
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

    ALSServingModel model = new ALSServingModel(FEATURES, true, LSH_SAMPLE_RATE, new TestALSRescorerProvider());
    AtomicLong totalEntries = new AtomicLong();

    int numCores = Runtime.getRuntime().availableProcessors();
    ForkJoinPool pool = new ForkJoinPool(numCores);
    try {
      log.info("Adding {} users", USERS);
      AtomicInteger userCount = new AtomicInteger();
      pool.submit(() -> IntStream.range(0, numCores).parallel().forEach(i -> {
        RandomGenerator random = RandomManager.getRandom(((long) i << 32) ^ System.nanoTime());
        PoissonDistribution itemPerUserDist = new PoissonDistribution(
            random,
            AVG_ITEMS_PER_USER,
            PoissonDistribution.DEFAULT_EPSILON,
            PoissonDistribution.DEFAULT_MAX_ITERATIONS);
        for (int user = userCount.getAndIncrement(); user < USERS; user = userCount.getAndIncrement()) {
          String userID = "U" + user;
          model.setUserVector(userID, VectorMath.randomVectorF(FEATURES, random));
          int itemsPerUser = itemPerUserDist.sample();
          totalEntries.addAndGet(itemsPerUser);
          Collection<String> knownIDs = new ArrayList<>(itemsPerUser);
          for (int item = 0; item < itemsPerUser; item++) {
            knownIDs.add("I" + random.nextInt(ITEMS));
          }
          model.addKnownItems(userID, knownIDs);
        }
      })).get();

      log.info("Adding {} items", ITEMS);
      AtomicInteger itemCount = new AtomicInteger();
      pool.submit(() -> IntStream.range(0, numCores).parallel().forEach(i -> {
        RandomGenerator random = RandomManager.getRandom(((long) i << 32) ^ System.nanoTime());
        for (int item = itemCount.getAndIncrement(); item < ITEMS; item = itemCount.getAndIncrement()) {
          model.setItemVector("I" + item, VectorMath.randomVectorF(FEATURES, random));
        }
      })).get();

    } catch (InterruptedException ie) {
      throw new IllegalStateException(ie);
    } catch (ExecutionException ee) {
      throw new IllegalStateException(ee.getCause());
    } finally {
      pool.shutdown();
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
