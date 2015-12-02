/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

import com.cloudera.oryx.common.OryxTest;

public final class ALSUpdateTest extends OryxTest {

  @Test
  public void testDecay() {
    Rating rating = new Rating(1, 2, 3.0);
    long timestamp = 10_000_000L;

    Rating noDecay = ALSUpdate.decayRating(rating, timestamp, timestamp, 0.5);
    assertEquals(1, noDecay.user());
    assertEquals(2, noDecay.product());
    assertEquals(3.0, noDecay.rating());

    Rating dayDecay = ALSUpdate.decayRating(rating, timestamp, timestamp + 86_400_000L, 0.5);
    assertEquals(1, dayDecay.user());
    assertEquals(2, dayDecay.product());
    assertEquals(1.5, dayDecay.rating());
  }

}
