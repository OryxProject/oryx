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
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
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

  private static final Function<Rating,Integer> RATING_TO_PRODUCT = new Function<Rating,Integer>() {
    @Override
    public Integer call(Rating r) {
      return r.product();
    }
  };

  private AUC() {
  }

  static double areaUnderCurve(MatrixFactorizationModel mfModel, JavaRDD<Rating> testData) {

    // Extract all test (user,product) pairs
    JavaPairRDD<Integer,Integer> testUserProducts =
        testData.mapToPair(new PairFunction<Rating,Integer,Integer>() {
          @Override
          public Tuple2<Integer,Integer> call(Rating rating) {
            return new Tuple2<>(rating.user(), rating.product());
          }
        });

    // Predict for each of them
    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,Object>> testUserProductsRDD =
        (RDD<Tuple2<Object,Object>>) (RDD<?>) testUserProducts.rdd();
    JavaPairRDD<Integer,Iterable<Rating>> testPredictions =
        testData.wrapRDD(mfModel.predict(testUserProductsRDD)).groupBy(RATING_TO_USER);

    // All distinct item IDs, to be broadcast
    final List<Integer> allItemIDs = testData.map(RATING_TO_PRODUCT).distinct().collect();

    // Create some predictions for randomly-chosen items
    JavaRDD<Integer> userIDs = testUserProducts.keys().distinct();
    JavaPairRDD<Integer,Integer> trainUserProducts = userIDs.flatMapToPair(
        new PairFlatMapFunction<Integer,Integer,Integer>() {
          private static final int NUM_TRAIN = 100;
          private final RandomDataGenerator random =
              new RandomDataGenerator(RandomManager.getRandom());
          @Override
          public Iterable<Tuple2<Integer,Integer>> call(Integer userID) {
            int numItems = allItemIDs.size();
            Collection<Tuple2<Integer,Integer>> tuples;
            if (numItems <= NUM_TRAIN) {
              tuples = new ArrayList<>(numItems);
              for (Integer itemID : allItemIDs) {
                tuples.add(new Tuple2<>(userID, itemID));
              }
            } else {
              tuples = new ArrayList<>(NUM_TRAIN);
              for (int index : random.nextPermutation(numItems, NUM_TRAIN)) {
                tuples.add(new Tuple2<>(userID, allItemIDs.get(index)));
              }
            }
            return tuples;
          }
        });

    // Predict for each of them
    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,Object>> trainUserProductsRDD =
        (RDD<Tuple2<Object,Object>>) (RDD<?>) trainUserProducts.rdd();
    JavaPairRDD<Integer,Iterable<Rating>> trainPredictions =
        testData.wrapRDD(mfModel.predict(trainUserProductsRDD)).groupBy(RATING_TO_USER);

    // Join test and train predictions
    JavaPairRDD<Long,Long> correctIncorrect = testPredictions.join(trainPredictions).mapToPair(
        new PairFunction<Tuple2<Integer, Tuple2<Iterable<Rating>, Iterable<Rating>>>, Long, Long>() {
          @Override
          public Tuple2<Long,Long> call(
              Tuple2<Integer, Tuple2<Iterable<Rating>, Iterable<Rating>>> t) {
            // AUC is estimated by probability that random 'correct' (test) examples
            // rank higher than random examples at large. Here we test all random train
            // examples vs all test examples and report the totals
            List<Rating> testRatings = Lists.newArrayList(t._2()._1());
            List<Rating> trainRatings = Lists.newArrayList(t._2()._2());
            long correct = 0;
            long incorrect = 0;
            for (Rating train : trainRatings) {
              for (Rating test : testRatings) {
                if (test.rating() > train.rating()) {
                  correct++;
                } else {
                  incorrect++;
                }
              }
            }
            return new Tuple2<>(correct, incorrect);
          }
        });

    long correct = correctIncorrect.keys().fold(0L, Functions.SUM_LONG);
    long incorrect = correctIncorrect.values().fold(0L, Functions.SUM_LONG);
    return (double) correct / (correct + incorrect);
  }

}
