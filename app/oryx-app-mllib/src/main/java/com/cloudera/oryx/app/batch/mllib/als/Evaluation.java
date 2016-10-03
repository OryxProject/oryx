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

package com.cloudera.oryx.app.batch.mllib.als;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Sets;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import com.cloudera.oryx.common.random.RandomManager;

/**
 * Computes AUC (area under the ROC curve) as a recommender evaluation metric.
 * Really, it computes what might be described as "Mean AUC", as it computes AUC per
 * user and averages them.
 */
final class Evaluation {

  private Evaluation() {}

  /**
   * Computes root mean squared error of {@link Rating#rating()} versus predicted value.
   */
  static double rmse(MatrixFactorizationModel mfModel, JavaRDD<Rating> testData) {
    JavaPairRDD<Tuple2<Integer,Integer>,Double> testUserProductValues =
        testData.mapToPair(rating -> new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating()));
    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,Object>> testUserProducts =
        (RDD<Tuple2<Object,Object>>) (RDD<?>) testUserProductValues.keys().rdd();
    JavaRDD<Rating> predictions = testData.wrapRDD(mfModel.predict(testUserProducts));
    double mse = predictions.mapToPair(
        rating -> new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating())
    ).join(testUserProductValues).values().mapToDouble(valuePrediction -> {
      double diff = valuePrediction._1() - valuePrediction._2();
      return diff * diff;
    }).mean();
    return Math.sqrt(mse);
  }

  /**
   * Computes AUC (area under the ROC curve) as a recommender evaluation metric.
   * Really, it computes what might be described as "Mean AUC", as it computes AUC per
   * user and averages them.
   */
  static double areaUnderCurve(JavaSparkContext sparkContext,
                               MatrixFactorizationModel mfModel,
                               JavaRDD<Rating> positiveData) {

    // This does not use Spark's BinaryClassificationMetrics.areaUnderROC because it
    // is intended to operate on one large set of (score,label) pairs. The computation
    // here is really many small AUC problems, for which a much faster direct computation
    // is available.

    // Extract all positive (user,product) pairs
    JavaPairRDD<Integer,Integer> positiveUserProducts =
        positiveData.mapToPair(rating -> new Tuple2<>(rating.user(), rating.product()));

    JavaPairRDD<Integer,Iterable<Rating>> positivePredictions =
        predictAll(mfModel, positiveData, positiveUserProducts);

    // All distinct item IDs, to be broadcast
    Broadcast<List<Integer>> allItemIDsBC = sparkContext.broadcast(positiveUserProducts.values().distinct().collect());

    JavaPairRDD<Integer,Integer> negativeUserProducts =
      positiveUserProducts.groupByKey().flatMapToPair(
        new PairFlatMapFunction<Tuple2<Integer,Iterable<Integer>>,Integer,Integer>() {
          private final RandomGenerator random = RandomManager.getRandom();
          @Override
          public Iterator<Tuple2<Integer,Integer>> call(
              Tuple2<Integer,Iterable<Integer>> userIDsAndItemIDs) {
            Integer userID = userIDsAndItemIDs._1();
            Collection<Integer> positiveItemIDs = Sets.newHashSet(userIDsAndItemIDs._2());
            int numPositive = positiveItemIDs.size();
            Collection<Tuple2<Integer,Integer>> negative = new ArrayList<>(numPositive);
            List<Integer> allItemIDs = allItemIDsBC.value();
            int numItems = allItemIDs.size();
            // Sample about as many negative examples as positive
            for (int i = 0; i < numItems && negative.size() < numPositive; i++) {
              Integer itemID = allItemIDs.get(random.nextInt(numItems));
              if (!positiveItemIDs.contains(itemID)) {
                negative.add(new Tuple2<>(userID, itemID));
              }
            }
            return negative.iterator();
          }
        });

    JavaPairRDD<Integer,Iterable<Rating>> negativePredictions =
        predictAll(mfModel, positiveData, negativeUserProducts);

    return positivePredictions.join(negativePredictions).values().mapToDouble(t -> {
        // AUC is also the probability that random positive examples
        // rank higher than random examples at large. Here we compare all random negative
        // examples to all positive examples and report the totals as an alternative
        // computation for AUC
        long correct = 0;
        long total = 0;
        for (Rating positive : t._1()) {
          for (Rating negative : t._2()) {
            if (positive.rating() > negative.rating()) {
              correct++;
            }
            total++;
          }
        }
        if (total == 0) {
          return 0.0;
        }
        return (double) correct / total;
      }).mean();
  }

  private static JavaPairRDD<Integer,Iterable<Rating>> predictAll(
      MatrixFactorizationModel mfModel,
      JavaRDD<Rating> data,
      JavaPairRDD<Integer,Integer> userProducts) {
    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,Object>> userProductsRDD =
        (RDD<Tuple2<Object,Object>>) (RDD<?>) userProducts.rdd();
    return data.wrapRDD(mfModel.predict(userProductsRDD)).groupBy(Rating::user);
  }

}
