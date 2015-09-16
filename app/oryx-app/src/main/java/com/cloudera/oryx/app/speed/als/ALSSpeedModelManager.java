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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.speed.SpeedModelManager;
import com.cloudera.oryx.app.als.ALSUtils;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.common.math.SingularMatrixSolverException;
import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.lambda.Functions;

/**
 * Implementation of {@link SpeedModelManager} that maintains and updates an ALS model in memory.
 */
public final class ALSSpeedModelManager implements SpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(ALSSpeedModelManager.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ALSSpeedModel model;
  private final boolean implicit;
  private final boolean noKnownItems;
  private final double minModelLoadFraction;

  public ALSSpeedModelManager(Config config) {
    implicit = config.getBoolean("oryx.als.implicit");
    noKnownItems = config.getBoolean("oryx.als.no-known-items");
    minModelLoadFraction = config.getDouble("oryx.speed.min-model-load-fraction");
    Preconditions.checkArgument(minModelLoadFraction >= 0.0 && minModelLoadFraction <= 1.0);
  }

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator,
                      Configuration hadoopConf) throws IOException {
    int countdownToLogModel = 10000;
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      Objects.requireNonNull(key, "Bad message: " + km);
      switch (key) {
        case "UP":
          if (model == null) {
            continue; // No model to interpret with yet, so skip it
          }
          List<?> update = MAPPER.readValue(message, List.class);
          // Update
          String id = update.get(1).toString();
          float[] vector = MAPPER.convertValue(update.get(2), float[].class);
          switch (update.get(0).toString()) {
            case "X":
              model.setUserVector(id, vector);
              break;
            case "Y":
              model.setItemVector(id, vector);
              break;
            default:
              throw new IllegalArgumentException("Bad message: " + km);
          }
          if (--countdownToLogModel <= 0) {
            log.info("{}", model);
            countdownToLogModel = 10000;
          }
          break;

        case "MODEL":
        case "MODEL-REF":
          log.info("Loading new model");
          PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);

          int features = Integer.parseInt(AppPMMLUtils.getExtensionValue(pmml, "features"));

          if (model == null || features != model.getFeatures()) {
            log.warn("No previous model, or # features has changed; creating new one");
            model = new ALSSpeedModel(features);
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
          throw new IllegalArgumentException("Bad message: " + km);
      }
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    // TODO if this is ever needed by other apps, this logic could be refactored for
    // other SpeedModelManager implementations, like in AbstractOryxResource
    if (model == null || model.getFractionLoaded() < minModelLoadFraction) {
      return Collections.emptyList();
    }

    // Order by timestamp and parse as tuples
    JavaRDD<String> sortedValues =
        newData.values().sortBy(MLFunctions.TO_TIMESTAMP_FN, true, newData.partitions().size());
    JavaPairRDD<Tuple2<String,String>,Double> tuples = sortedValues.mapToPair(TO_TUPLE_FN);

    JavaPairRDD<Tuple2<String,String>,Double> aggregated;
    if (implicit) {
      // See comments in ALSUpdate for explanation of how deletes are handled by this.
      aggregated = tuples.groupByKey().mapValues(MLFunctions.SUM_WITH_NAN);
    } else {
      // For non-implicit, last wins.
      aggregated = tuples.foldByKey(Double.NaN, Functions.<Double>last());
    }

    Collection<UserItemStrength> input = aggregated
        .filter(MLFunctions.<Tuple2<String,String>>notNaNValue())
        .map(TO_UIS_FN).collect();

    Solver XTXsolver;
    Solver YTYsolver;
    try {
      XTXsolver = model.getXTXSolver();
      YTYsolver = model.getYTYSolver();
    } catch (SingularMatrixSolverException smse) {
      return Collections.emptyList();
    }

    Collection<String> result = new ArrayList<>();
    for (UserItemStrength uis : input) {
      String user = uis.getUser();
      String item = uis.getItem();
      double value = uis.getStrength();

      // Xu is the current row u in the X user-feature matrix
      float[] Xu = model.getUserVector(user);
      // Yi is the current row i in the Y item-feature matrix
      float[] Yi = model.getItemVector(item);

      float[] newXu = newVector(YTYsolver, value, Xu, Yi);
      // Similarly for Y vs X
      float[] newYi = newVector(XTXsolver, value, Yi, Xu);

      if (newXu != null) {
        result.add(toUpdateJSON("X", user, newXu, item));
      }
      if (newYi != null) {
        result.add(toUpdateJSON("Y", item, newYi, user));
      }
    }
    return result;
  }

  private float[] newVector(Solver solver, double value, float[] Xu, float[] Yi) {
    float[] newXu = null;
    if (Yi != null) {
      // Let Qui = Xu * (Yi)^t -- it's the current estimate of user-item interaction in Q = X * Y^t
      double Qui = Xu == null ? 0.0 : VectorMath.dot(Xu, Yi);
      // Qui' is the target, new value of Qui
      double targetQui = computeTargetQui(value, Xu == null ? 0.5 : Qui); // 0.5 reflects a "don't know" state
      if (!Double.isNaN(targetQui)) {
        // In Qu = Xu * Y^T, Xu is going to change to Xu' such that Qu' = Xu' * Y^T. Qu' will change
        // from Qu by the vector dQu = [0, 0, ..., dQui, ...] where the nonzero value
        // dQui = (Qui' - Qui) is in position i. The change dXu from Xu to Xu' should satisfy
        // dQu = dXu * Y^T. We solve for dXu and then add it to Xu. dQu * Y = dXu * (Y^t * Y).
        // dQu is 0 except for one value at position i, so dQu * Y is really dQui*Yi
        double dQui = targetQui - Qui;
        float[] dQuiYi = Yi.clone();
        for (int i = 0; i < dQuiYi.length; i++) {
          dQuiYi[i] *= dQui;
        }
        double[] dXu = solver.solveFToD(dQuiYi);
        newXu = Xu == null ? new float[model.getFeatures()] : Xu.clone();
        for (int i = 0; i < newXu.length; i++) {
          newXu[i] += dXu[i];
        }
      }
    }
    return newXu;
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

  @Override
  public void close() {
    // do nothing
  }

  private double computeTargetQui(double value, double currentValue) {
    // We want Qui to change based on value. What's the target value, Qui'?
    if (implicit) {
      return ALSUtils.implicitTargetQui(value, currentValue);
    } else {
      // Non-implicit -- value is supposed to be the new value
      return value;
    }
  }

  private static final PairFunction<String,Tuple2<String,String>,Double> TO_TUPLE_FN =
      new PairFunction<String,Tuple2<String,String>,Double>() {
        @Override
        public Tuple2<Tuple2<String,String>,Double> call(String line) throws Exception {
          try {
            String[] tokens = MLFunctions.PARSE_FN.call(line);
            String user = tokens[0];
            String item = tokens[1];
            Double strength = Double.valueOf(tokens[2]);
            return new Tuple2<>(new Tuple2<>(user, item), strength);
          } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            log.warn("Bad input: {}", line);
            throw e;
          }
        }
      };

  private static final Function<Tuple2<Tuple2<String,String>,Double>,UserItemStrength> TO_UIS_FN =
      new Function<Tuple2<Tuple2<String, String>, Double>, UserItemStrength>() {
        @Override
        public UserItemStrength call(Tuple2<Tuple2<String,String>,Double> tuple) {
          Tuple2<String,String> userItem = tuple._1();
          Double strength = tuple._2();
          return new UserItemStrength(userItem._1(), userItem._2(), strength.floatValue());
        }
      };

}
