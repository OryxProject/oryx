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

import com.cloudera.oryx.app.serving.als.TestALSRescorerProvider;

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
    ALSServingModel model = new ALSServingModel(2, true, 1.0, new TestALSRescorerProvider());
    setVectors(model, true, new float[][] {
        {-0.35837504f,  0.60391283f},
        {-0.7757129f,   0.4327127f},
        {-0.7757129f,  -0.4327127f},
        {-0.35837504f, -0.60391283f},
        {-1.1340879f,   1.0366255f},
        {-1.5514258f,  -3.5398357e-16f},
        {-1.1340879f,  -1.0366255f},
    });
    setVectors(model, false, new float[][] {
        {-0.23176478f,  0.504302f},
        {-0.53749436f,  0.45167503f},
        {-0.53749436f, -0.45167503f},
        {-0.23176478f, -0.504302f},
        {-0.7692591f,   0.9559771f},
        {-1.0749887f,   1.0619507e-16f},
        {-0.7692591f,  -0.9559771f},
        {-1.3067534f,   0.504302f},
        {-1.3067534f,  -0.504302f},
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

  private static void setVectors(ALSServingModel model, boolean user, float[]... vectors) {
    for (int i = 0; i < vectors.length; i++) {
      if (user) {
        model.setUserVector("U" + i, vectors[i]);
      } else {
        model.setItemVector("I" + i, vectors[i]);
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
