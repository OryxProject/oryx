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

package com.cloudera.oryx.ml.mllib.als;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

final class RMSE {

  private RMSE() {
  }

  // TODO use RegressionMetrics in Spark 1.2+

  static double rmse(MatrixFactorizationModel mfModel, JavaRDD<Rating> testData) {
    JavaPairRDD<Tuple2<Integer,Integer>,Double> testUserProductValues =
        testData.mapToPair(new RatingToTupleDouble());
    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,Object>> testUserProducts =
        (RDD<Tuple2<Object,Object>>) (RDD<?>) testUserProductValues.keys().rdd();
    JavaRDD<Rating> predictions = testData.wrapRDD(mfModel.predict(testUserProducts));
    double mse = predictions.mapToPair(
        new RatingToTupleDouble()
    ).join(testUserProductValues).values().mapToDouble(
        new DoubleFunction<Tuple2<Double,Double>>() {
          @Override
          public double call(Tuple2<Double,Double> valuePrediction) {
            double diff = valuePrediction._1() - valuePrediction._2();
            return diff * diff;
          }
        }).mean();
    return Math.sqrt(mse);
  }

}
