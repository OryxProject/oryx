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

import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.pmml.PMMLUtilsTest;

/**
 * A dummy {@link MLUpdate} that collects data seen by the framework in given {@link List}s.
 * Assists testing.
 */
public final class MockMLUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(MockMLUpdate.class);

  private static final List<Integer> trainCounts = new ArrayList<>();
  private static final List<Integer> testCounts = new ArrayList<>();

  static List<Integer> getResetTrainCounts() {
    trainCounts.clear();
    return trainCounts;
  }

  static List<Integer> getResetTestCounts() {
    testCounts.clear();
    return testCounts;
  }

  public MockMLUpdate(Config config) {
    super(config);
  }

  @Override
  public PMML buildModel(JavaSparkContext sparkContext,
                         JavaRDD<String> trainData,
                         List<?> hyperParameters,
                         Path candidatePath) {
    // If lists are unequal at this point, there must have been an empty test set
    // which yielded no call to evaluate(). Fill in the blank
    while (trainCounts.size() > testCounts.size()) {
      testCounts.add(0);
    }
    trainCounts.add((int) trainData.count());
    return PMMLUtilsTest.buildDummyModel();
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData,
                         JavaRDD<String> trainData) {
    long testDataCount = testData.count();
    testCounts.add((int) testDataCount);
    log.info("Returning eval {}", testDataCount);
    return testDataCount;
  }

}
