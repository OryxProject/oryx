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

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

/**
* Created by srowen on 7/20/14.
*/
class RatingToTupleDouble
    implements PairFunction<Rating,Tuple2<Integer,Integer>,Double> {
  @Override
  public Tuple2<Tuple2<Integer,Integer>,Double> call(Rating rating) {
    return new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating());
  }
}
