/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.als.model;

import java.util.ArrayList;
import java.util.Collection;

import com.cloudera.oryx.common.math.VectorMath;

public final class TestALSModelFactory {

  private TestALSModelFactory() {}

  /*
  X = [ 1 0 0 0 ; 0 1 0 0 ; 0 0 1 0 ; 0 0 0 1 ; 1 1 0 0 ; 0 1 1 0 ; 0 0 1 1 ]

     1   0   0   0
     0   1   0   0
     0   0   1   0
     0   0   0   1
     1   1   0   0
     0   1   1   0
     0   0   1   1

  Y = [ 1 0 0 0 ; 0 1 0 0 ; 0 0 1 0 ; 0 0 0 1 ; 1 1 0 0 ; 0 1 1 0 ; 0 0 1 1 ; 1 1 1 0 ; 0 1 1 1 ]

     1   0   0   0
     0   1   0   0
     0   0   1   0
     0   0   0   1
     1   1   0   0
     0   1   1   0
     0   0   1   1
     1   1   1   0
     0   1   1   1

  A = X * Y'

     1   0   0   0   1   0   0   1   0
     0   1   0   0   1   1   0   1   1
     0   0   1   0   0   1   1   1   1
     0   0   0   1   0   0   1   0   1
     1   1   0   0   2   1   0   2   1
     0   1   1   0   1   2   1   2   2
     0   0   1   1   0   1   2   1   2

  [U,S,V] = svds(A,2)
  Xp = U*sqrt(S)
  Yp = V*sqrt(S)

  These yield the X and Y matrices for the model.
   */

  public static ALSServingModel buildTestModel() {
    ALSServingModel model = new ALSServingModel(2, true, null);
    setVectors(model, true, new double[][] {
        {-0.358375051039897,      0.60391285187422},
        {-0.775712888238815,      0.43271270099921},
        {-0.775712888238816,     -0.43271270099921},
        {-0.358375051039898,    -0.603912851874221},
        {-1.13408793927871,      1.03662555287343},
        {-1.55142577647763,  -3.53983555040678e-16},
        {-1.13408793927871,     -1.03662555287343},
    });
    setVectors(model, false, new double[][] {
        {-0.231764776187115,     0.504302022409069},
        {-0.537494339498882,     0.451675042099952},
        {-0.537494339498881,    -0.451675042099952},
        {-0.231764776187115,     -0.50430202240907},
        {-0.769259115685997,     0.955977064509021},
        {-1.07498867899776,  1.06195066512203e-16},
        {-0.769259115685996,    -0.955977064509021},
        {-1.30675345518488,     0.504302022409069},
        {-1.30675345518488,    -0.504302022409069},
    });
    setKnownItems(model, new int[][] {
        {1, 0, 0, 0, 1, 0, 0, 1, 0},
        {0, 1, 0, 0, 1, 1, 0, 1, 1},
        {0, 0, 1, 0, 0, 1, 1, 1, 1},
        {0, 0, 0, 1, 0, 0, 1, 0, 1},
        {1, 1, 0, 0, 2, 1, 0, 2, 1},
        {0, 1, 1, 0, 1, 2, 1, 2, 2},
        {0, 0, 1, 1, 0, 1, 2, 1, 2},
    });
    return model;
  }

  private static void setVectors(ALSServingModel model, boolean user, double[]... vectors) {
    for (int i = 0; i < vectors.length; i++) {
      float[] modelVec = VectorMath.toFloats(vectors[i]);
      if (user) {
        model.setUserVector("U" + i, modelVec);
      } else {
        model.setItemVector("I" + i, modelVec);
      }
    }
  }
  
  private static void setKnownItems(ALSServingModel model, int[]... counts) {
    for (int user = 0; user < counts.length; user++) {
      Collection<String> knownItems = new ArrayList<>();
      int[] count = counts[user];
      for (int item = 0; item < count.length; item++) {
        if (count[item] > 0) {
          knownItems.add("I" + item);
        }
      }
      if (!knownItems.isEmpty()) {
        model.addKnownItems("U" + user, knownItems);
      }
    }
  }

}
