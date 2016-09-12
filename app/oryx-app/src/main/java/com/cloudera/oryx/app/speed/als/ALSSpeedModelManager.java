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

package com.cloudera.oryx.app.speed.als;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.speed.AbstractSpeedModelManager;
import com.cloudera.oryx.app.als.ALSUtils;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.lang.RateLimitCheck;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.common.math.SingularMatrixSolverException;
import com.cloudera.oryx.common.math.Solver;

/**
 * Implementation of {@link com.cloudera.oryx.api.speed.SpeedModelManager} that maintains and
 * updates an ALS model in memory.
 */
public final class ALSSpeedModelManager extends AbstractSpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(ALSSpeedModelManager.class);

  private ALSSpeedModel model;
  private final boolean noKnownItems;
  private final double minModelLoadFraction;
  private final RateLimitCheck logRateLimit;

  public ALSSpeedModelManager(Config config) {
    noKnownItems = config.getBoolean("oryx.als.no-known-items");
    minModelLoadFraction = config.getDouble("oryx.speed.min-model-load-fraction");
    Preconditions.checkArgument(minModelLoadFraction >= 0.0 && minModelLoadFraction <= 1.0);
    logRateLimit = new RateLimitCheck(1, TimeUnit.MINUTES);
  }

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "UP":
        if (model == null) {
          return; // No model to interpret with yet, so skip it
        }
        // Note that here, the speed layer is actually listening for updates from
        // two sources. First is the batch layer. This is somewhat unusual, because
        // the batch layer usually only makes MODELs. The ALS model is too large
        // to send in one file, so is sent as a skeleton model plus a series of updates.
        // However it is also, neatly, listening for the same updates it produces below
        // in response to new data, and applying them to the in-memory representation.
        // ALS continues to be a somewhat special case here, in that it does benefit from
        // real-time updates to even the speed layer reference model.
        List<?> update = TextUtils.readJSON(message, List.class);
        // Update
        String id = update.get(1).toString();
        float[] vector = TextUtils.convertViaJSON(update.get(2), float[].class);
        switch (update.get(0).toString()) {
          case "X":
            model.setUserVector(id, vector);
            break;
          case "Y":
            model.setItemVector(id, vector);
            break;
          default:
            throw new IllegalArgumentException("Bad message: " + message);
        }
        if (logRateLimit.test()) {
          log.info("{}", model);
        }
        break;

      case "MODEL":
      case "MODEL-REF":
        log.info("Loading new model");
        PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);
        if (pmml == null) {
          return;
        }

        int features = Integer.parseInt(AppPMMLUtils.getExtensionValue(pmml, "features"));
        boolean implicit = Boolean.parseBoolean(AppPMMLUtils.getExtensionValue(pmml, "implicit"));
        boolean logStrength = Boolean.parseBoolean(AppPMMLUtils.getExtensionValue(pmml, "logStrength"));
        double epsilon = logStrength ?
            Double.parseDouble(AppPMMLUtils.getExtensionValue(pmml, "epsilon")) :
            Double.NaN;

        if (model == null || features != model.getFeatures()) {
          log.warn("No previous model, or # features has changed; creating new one");
          model = new ALSSpeedModel(features, implicit, logStrength, epsilon);
        }

        log.info("Updating model");
        // Remove users/items no longer in the model
        Collection<String> XIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "XIDs"));
        Collection<String> YIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "YIDs"));
        model.retainRecentAndUserIDs(XIDs);
        model.retainRecentAndItemIDs(YIDs);
        log.info("Model updated: {}", model);
        break;

      default:
        throw new IllegalArgumentException("Bad key: " + key);
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    if (model == null || model.getFractionLoaded() < minModelLoadFraction) {
      return Collections.emptyList();
    }

    // Trigger proactive computation of solvers for later use
    model.precomputeSolvers();

    // Order by timestamp and parse as tuples
    JavaRDD<String> sortedValues =
        newData.values().sortBy(MLFunctions.TO_TIMESTAMP_FN, true, newData.partitions().size());
    JavaPairRDD<Tuple2<String,String>,Double> tuples = sortedValues.mapToPair(line -> {
      try {
        String[] tokens = MLFunctions.PARSE_FN.call(line);
        String user = tokens[0];
        String item = tokens[1];
        Double strength = tokens[2].isEmpty() ? Double.NaN : Double.valueOf(tokens[2]);
        return new Tuple2<>(new Tuple2<>(user, item), strength);
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", line);
        throw e;
      }
    });

    JavaPairRDD<Tuple2<String,String>,Double> aggregated;
    if (model.isImplicit()) {
      // See comments in ALSUpdate for explanation of how deletes are handled by this.
      aggregated = tuples.groupByKey().mapValues(MLFunctions.SUM_WITH_NAN);
    } else {
      // For non-implicit, last wins.
      aggregated = tuples.foldByKey(Double.NaN, (current, next) -> next);
    }

    JavaPairRDD<Tuple2<String,String>,Double> noNaN =
        aggregated.filter(kv -> !Double.isNaN(kv._2()));

    JavaRDD<UserItemStrength> inputRDD;
    if (model.isLogStrength()) {
      double epsilon = model.getEpsilon();
      inputRDD = noNaN.map(tuple -> new UserItemStrength(tuple._1()._1(), tuple._1()._2(),
                                                         (float) Math.log1p(tuple._2() / epsilon)));
    } else {
      inputRDD = noNaN.map(tuple -> new UserItemStrength(tuple._1()._1(), tuple._1()._2(),
                                                         tuple._2().floatValue()));
    }

    Collection<UserItemStrength> input = inputRDD.collect();

    Solver XTXsolver;
    Solver YTYsolver;
    try {
      XTXsolver = model.getXTXSolver();
      YTYsolver = model.getYTYSolver();
    } catch (SingularMatrixSolverException smse) {
      log.info("Not enough data for solver yet ({}); skipping inputs", smse.getMessage());
      return Collections.emptyList();
    }
    if (XTXsolver == null || YTYsolver == null) {
      log.info("No solver available yet for model; skipping inputs");
      return Collections.emptyList();
    }

    return input.parallelStream().flatMap(uis -> {
      String user = uis.getUser();
      String item = uis.getItem();
      double value = uis.getStrength();

      // Xu is the current row u in the X user-feature matrix
      float[] Xu = model.getUserVector(user);
      // Yi is the current row i in the Y item-feature matrix
      float[] Yi = model.getItemVector(item);

      float[] newXu = ALSUtils.computeUpdatedXu(YTYsolver, value, Xu, Yi, model.isImplicit());
      // Similarly for Y vs X
      float[] newYi = ALSUtils.computeUpdatedXu(XTXsolver, value, Yi, Xu, model.isImplicit());

      Collection<String> result = new ArrayList<>(2);
      if (newXu != null) {
        result.add(toUpdateJSON("X", user, newXu, item));
      }
      if (newYi != null) {
        result.add(toUpdateJSON("Y", item, newYi, user));
      }
      return result.stream();
    }).collect(Collectors.toList());
  }

  private String toUpdateJSON(String matrix, String ID, float[] vector, String otherID) {
    List<?> args;
    if (noKnownItems) {
      args = Arrays.asList(matrix, ID, vector);
    } else {
      args = Arrays.asList(matrix, ID, vector, Collections.singletonList(otherID));
    }
    return TextUtils.joinJSON(args);
  }

}
