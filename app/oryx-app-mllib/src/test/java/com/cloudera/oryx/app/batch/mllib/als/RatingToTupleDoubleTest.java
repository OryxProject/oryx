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

import org.apache.spark.mllib.recommendation.Rating;
import org.junit.Test;
import scala.Tuple2;

import com.cloudera.oryx.common.OryxTest;

public final class RatingToTupleDoubleTest extends OryxTest {

  @Test
  public void testFunction() {
    Tuple2<Tuple2<Integer,Integer>,Double> tuple =
        new RatingToTupleDouble().call(new Rating(1, 2, 3.0));
    assertEquals(1, tuple._1()._1().intValue());
    assertEquals(2, tuple._1()._2().intValue());
    assertEquals(3.0, tuple._2().doubleValue());
  }

}
