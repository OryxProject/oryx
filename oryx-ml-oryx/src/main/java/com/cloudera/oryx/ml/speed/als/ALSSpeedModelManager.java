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

package com.cloudera.oryx.ml.speed.als;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.common.collection.FormatUtils;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.fn.Functions;
import com.cloudera.oryx.lambda.speed.SpeedModelManager;
import com.cloudera.oryx.common.math.SingularMatrixSolverException;
import com.cloudera.oryx.common.math.Solver;

public final class ALSSpeedModelManager implements SpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(ALSSpeedModelManager.class);

  private ALSSpeedModel model;
  private final boolean implicit;

  public ALSSpeedModelManager(Config config) {
    implicit = config.getBoolean("als.hyperparams.implicit");
  }

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator) throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      switch (key) {
        case "UP":
          Preconditions.checkNotNull(model);
          // Update
          String[] tokens = message.split("\t");
          String id = tokens[1];
          float[] vector = FormatUtils.parseFloatVec(tokens[2]);
          switch (tokens[0]) {
            case "X":
              model.setUserVector(id, vector);
              break;
            case "Y":
              model.setItemVector(id, vector);
              break;
            default:
              throw new IllegalStateException("Bad update " + message);
          }
          break;

        case "MODEL":
          // New model
          PMML pmml = PMMLUtils.fromString(message);
          int features = Integer.parseInt(PMMLUtils.getExtensionValue(pmml, "features"));
          if (model == null) {

            log.info("No previous model");
            model = new ALSSpeedModel(features);

          } else if (features != model.getFeatures()) {

            log.warn("# features has changed! removing old model");
            model = new ALSSpeedModel(features);

          } else {

            // First, remove users/items no longer in the model
            String[] XIDs = PMMLUtils.parseArray(PMMLUtils.getExtensionContent(pmml, "XIDs"));
            String[] YIDs = PMMLUtils.parseArray(PMMLUtils.getExtensionContent(pmml, "YIDs"));
            model.retainAllUsers(Arrays.asList(XIDs));
            model.retainAllItems(Arrays.asList(YIDs));

          }
          break;

        default:
          throw new IllegalStateException("Bad model " + message);
      }
    }
  }

  @Override
  public Collection<String> buildUpdates(JavaPairRDD<String,String> newData) {
    if (model == null) {
      return Collections.emptyList();
    }

    JavaPairRDD<Tuple2<String,String>,Double> tuples = newData.mapToPair(new UpdatesToTuple());

    JavaPairRDD<Tuple2<String,String>,Double> aggregated;
    if (implicit) {
      // For implicit, values are scores to be summed
      aggregated = tuples.reduceByKey(Functions.SUM_DOUBLE);
    } else {
      // For non-implicit, last wins.
      aggregated = tuples.groupByKey().mapValues(Functions.<Double>last());
    }

    Collection<UserItemStrength> input = aggregated.map(new TupleToUserItemStrength()).collect();

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

      float[] newXu = null;
      if (Yi != null) {
        // Let Qui = Xu * (Yi)^t -- it's the current estimate of user-item interaction
        // in Q = X * Y^t
        double currentValue = Xu == null ? 0.0 : VectorMath.dot(Xu, Yi);
        double targetQui = computeTargetQui(value, currentValue);
        // The entire vector Qu' is just 0, with Qui' in position i
        // More generally we are looking for Qu' = Xu' * Y^t
        if (!Double.isNaN(targetQui)) {
          // Solving Qu' = Xu' * Y^t for Xu', now that we have Qui', as:
          // Qu' * Y * (Y^t * Yi)^-1 = Xu'
          // Qu' is 0 except for one value at position i, so it's really (Qui')*Yi
          float[] QuiYi = Yi.clone();
          for (int i = 0; i < QuiYi.length; i++) {
            QuiYi[i] *= targetQui;
          }
          newXu = YTYsolver.solveFToF(QuiYi);
        }
      }

      // Similarly for Y vs X
      float[] newYi = null;
      if (Xu != null) {
        double currentValue = Yi == null ? 0.0 : VectorMath.dot(Xu, Yi);
        double targetQui = computeTargetQui(value, currentValue);
        if (!Double.isNaN(targetQui)) {
          float[] QuiXu = Xu.clone();
          for (int i = 0; i < QuiXu.length; i++) {
            QuiXu[i] *= targetQui;
          }
          newYi = XTXsolver.solveFToF(QuiXu);
        }
      }

      if (newXu != null) {
        result.add("X\t" + user + '\t' + FormatUtils.formatFloatVec(newXu));
      }
      if (newYi != null) {
        result.add("Y\t" + item + '\t' + FormatUtils.formatFloatVec(newYi));
      }
    }
    return result;
  }

  @Override
  public void close() {
    // do nothing
  }

  private double computeTargetQui(double value, double currentValue) {
    // We want Qui to change based on value. What's the target value, Qui'?
    // Then we find a new vector Xu' such that Qui' = Xu' * (Yi)^t
    double targetQui;
    if (implicit) {
      // Target is really 1, or 0, depending on whether value is positive or negative.
      // This wouldn't account for the strength though. Instead the target is a function
      // of the current value and strength. If the current value is c, and value is positive
      // then the target is somewhere between c and 1 depending on the strength. If current
      // value is already >= 1, there's no effect. Similarly for negative values.
      if (value > 0.0f && currentValue < 1.0) {
        double diff = 1.0 - Math.max(0.0, currentValue);
        targetQui = currentValue + (1.0 - 1.0 / (1.0 + value)) * diff;
      } else if (value < 0.0f && currentValue > 0.0) {
        double diff = -Math.min(1.0, currentValue);
        targetQui = currentValue + (1.0 - 1.0 / (1.0 - value)) * diff;
      } else {
        // No change
        targetQui = Double.NaN;
      }
    } else {
      // Non-implicit -- value is supposed to be the new value
      targetQui = value;
    }
    return targetQui;
  }

  private static class UpdatesToTuple
      implements PairFunction<Tuple2<String,String>,Tuple2<String,String>,Double> {

    private static final Pattern COMMA = Pattern.compile(",");

    @Override
    public Tuple2<Tuple2<String,String>,Double> call(Tuple2<String,String> km) {
      String[] tokens = COMMA.split(km._2());
      int numTokens = tokens.length;
      Preconditions.checkArgument(numTokens >= 2 && numTokens <= 3, "Bad update: %s", km._2());
      String user = tokens[0];
      String item = tokens[1];
      double value = numTokens == 3 ? Double.parseDouble(tokens[2]) : 1.0;
      return new Tuple2<>(new Tuple2<>(user, item), value);
    }

  }

  private static class TupleToUserItemStrength
      implements Function<Tuple2<Tuple2<String,String>,Double>,UserItemStrength> {
    @Override
    public UserItemStrength call(Tuple2<Tuple2<String,String>,Double> userProductScore) {
      Tuple2<String,String> userProduct = userProductScore._1();
      return new UserItemStrength(userProduct._1(),
                                  userProduct._2(),
                                  userProductScore._2().floatValue());
    }
  }

}
