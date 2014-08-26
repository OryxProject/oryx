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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.lambda.fn.Functions;

final class AUC {

  private static final Function<Rating,Integer> RATING_TO_USER = new Function<Rating,Integer>() {
    @Override
    public Integer call(Rating r) {
      return r.user();
    }
  };

  private static final int NUM_NEGATIVE = 100;

  private AUC() {
  }

  static double areaUnderCurve(JavaSparkContext sparkContext,
                               MatrixFactorizationModel mfModel,
                               JavaRDD<Rating> positiveData) {

    // Extract all positive (user,product) pairs
    JavaPairRDD<Integer,Integer> positiveUserProducts =
        positiveData.mapToPair(new PairFunction<Rating,Integer,Integer>() {
          @Override
          public Tuple2<Integer,Integer> call(Rating rating) {
            return new Tuple2<>(rating.user(), rating.product());
          }
        });

    JavaPairRDD<Integer,Iterable<Rating>> positivePredictions =
        predictAll(mfModel, positiveData, positiveUserProducts);

    // All distinct item IDs, to be broadcast
    final Broadcast<List<Integer>> allItemIDsBC =
        sparkContext.broadcast(positiveUserProducts.values().distinct().collect());

    JavaPairRDD<Integer,Integer> negativeUserProducts =
      positiveUserProducts.groupByKey().flatMapToPair(
        new PairFlatMapFunction<Tuple2<Integer,Iterable<Integer>>,Integer,Integer>() {
          private final RandomGenerator random = RandomManager.getRandom();
          @Override
          public Iterable<Tuple2<Integer,Integer>> call(
              Tuple2<Integer,Iterable<Integer>> userIDsAndItemIDs) {
            Integer userID = userIDsAndItemIDs._1();
            Collection<Integer> positiveItemIDs = Sets.newHashSet(userIDsAndItemIDs._2());
            Collection<Tuple2<Integer,Integer>> negative = new ArrayList<>(NUM_NEGATIVE);
            List<Integer> allItemIDs = allItemIDsBC.value();
            int numItems = allItemIDs.size();
            // The number of iterations is a bit arbitrary
            for (int i = 0; i < numItems && negative.size() < NUM_NEGATIVE; i++) {
              Integer itemID = allItemIDs.get(random.nextInt(numItems));
              if (!positiveItemIDs.contains(itemID)) {
                negative.add(new Tuple2<>(userID, itemID));
              }
            }
            return negative;
          }
        });

    JavaPairRDD<Integer,Iterable<Rating>> negativePredictions =
        predictAll(mfModel, positiveData, negativeUserProducts);

    // Join positive and negative predictions
    JavaPairRDD<Long,Long> correctAndTotal = positivePredictions.join(negativePredictions).mapToPair(
        new PairFunction<Tuple2<Integer, Tuple2<Iterable<Rating>, Iterable<Rating>>>, Long, Long>() {
          @Override
          public Tuple2<Long,Long> call(
              Tuple2<Integer, Tuple2<Iterable<Rating>, Iterable<Rating>>> t) {
            // AUC is estimated by probability that random positive examples
            // rank higher than random examples at large. Here we compare all random negative
            // examples to all positive examples and report the totals
            List<Rating> positiveRatings = Lists.newArrayList(t._2()._1());
            List<Rating> negativeRatings = Lists.newArrayList(t._2()._2());
            long correct = 0;
            for (Rating negative : negativeRatings) {
              for (Rating positive : positiveRatings) {
                if (positive.rating() > negative.rating()) {
                  correct++;
                }
              }
            }
            return new Tuple2<>(correct, (long) positiveRatings.size() * negativeRatings.size());
          }
        });

    double correct = correctAndTotal.keys().fold(0L, Functions.SUM_LONG);
    double total = correctAndTotal.values().fold(0L, Functions.SUM_LONG);
    return correct / total;
  }

  private static JavaPairRDD<Integer,Iterable<Rating>> predictAll(
      MatrixFactorizationModel mfModel,
      JavaRDD<Rating> data,
      JavaPairRDD<Integer,Integer> userProducts) {
    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,Object>> userProductsRDD =
        (RDD<Tuple2<Object,Object>>) (RDD<?>) userProducts.rdd();
    return data.wrapRDD(mfModel.predict(userProductsRDD)).groupBy(RATING_TO_USER);
  }

}
