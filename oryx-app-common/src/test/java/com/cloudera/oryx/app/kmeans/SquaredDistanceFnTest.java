/*
 * Copyright (c) 2015, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.app.kmeans;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class SquaredDistanceFnTest extends OryxTest {

  @Test
  public void testDistance() {
    assertEquals(14.0, new SquaredDistanceFn().distance(new double[] { 1.0, 2.0, 3.0 },
                                                      new double[] { 0.0, 4.0, 6.0 }));
    assertEquals(2.0, new SquaredDistanceFn().distance(new double[] { 0.0, 1.0 },
                                                     new double[] { -1.0, 0.0 }));
    assertEquals(0.0, new SquaredDistanceFn().distance(new double[] { 1.0 }, new double[] { 1.0 }));
  }

}
