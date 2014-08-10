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

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.ml.speed.als.ALSSpeedModel;

public final class ALSSpeedModelTest extends OryxTest {

  @Test
  public void testTransposeTimesSelf() {
    ALSSpeedModel model = new ALSSpeedModel(3);
    IntObjectMap<float[]> a = new IntObjectOpenHashMap<>();
    a.put(-1, new float[] {1.3f, -2.0f, 3.0f});
    a.put(1, new float[] {2.0f, 0.0f, 5.0f});
    a.put(3, new float[] {0.0f, -1.5f, 5.5f});
    RealMatrix ata = model.transposeTimesSelf(a);
    RealMatrix expected = new Array2DRowRealMatrix(new double[][] {
        {5.69, -2.6, 13.9},
        {-2.6, 6.25, -14.25},
        {13.9, -14.25, 64.25}
    });
    for (int row = 0; row < 3; row++) {
      for (int col = 0; col < 3; col++) {
        assertEquals(expected.getEntry(row, col), ata.getEntry(row, col), FLOAT_EPSILON);
      }
    }
  }

}
