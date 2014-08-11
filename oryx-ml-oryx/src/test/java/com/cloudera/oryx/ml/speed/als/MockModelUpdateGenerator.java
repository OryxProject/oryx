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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.random.RandomGenerator;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.common.collection.FormatUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;

final class MockModelUpdateGenerator implements RandomDatumGenerator<String,String> {

  static final Map<Integer,float[]> X;
  static final Map<Integer,float[]> Y;
  static {
    /*
     * Octave:
     *
     * A = [ 1 0 0 1 0 ; 0 1 0 1 1 ; 1 1 1 1 0 ; 0 0 1 0 0 ]
     * [U,S,V] = svd(A)
     *
     * U = U(:,1:2)
     * S = S(1:2,1:2)
     * V = V(:,1:2)
     *
     * X = U*sqrt(S)
     *   -0.67900   0.17323
     *   -0.82324  -0.92009
     *   -1.18653   0.44632
     *   -0.20790   0.53035
     * Y = V*sqrt(S)
     *   -0.72032   0.45655
     *   -0.77602  -0.34912
     *   -0.53842   0.71971
     *   -1.03820  -0.22146
     *   -0.31787  -0.67801
     */
    X = new HashMap<>();
    X.put(6, new float[] {-0.67900f,  0.17323f});
    X.put(7, new float[] {-0.82324f, -0.92009f});
    X.put(8, new float[] {-1.18653f,  0.44632f});
    X.put(9, new float[] {-0.20790f,  0.53035f});
    Y = new HashMap<>();
    Y.put(1, new float[] {-0.72032f,  0.45655f});
    Y.put(2, new float[] {-0.77602f, -0.34912f});
    Y.put(3, new float[] {-0.53842f,  0.71971f});
    Y.put(4, new float[] {-1.03820f, -0.22146f});
    Y.put(5, new float[] {-0.31787f, -0.67801f});
  }

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) throws IOException {
    if (id % 10 == 0) {
      PMML pmml = PMMLUtils.buildSkeletonPMML();
      PMMLUtils.addExtension(pmml, "features", "2");
      PMMLUtils.addExtensionContent(pmml, "XIDs", X.keySet());
      PMMLUtils.addExtensionContent(pmml, "YIDs", Y.keySet());
      return new Pair<>("MODEL", PMMLUtils.toString(pmml));
    } else {
      int xOrYID = id % 10;
      boolean isX = xOrYID >= 6;
      if (isX) {
        return new Pair<>( "UP", "X\t" + xOrYID + "\t" + FormatUtils.formatFloatVec(X.get(xOrYID)));
      } else {
        return new Pair<>( "UP", "Y\t" + xOrYID + "\t" + FormatUtils.formatFloatVec(Y.get(xOrYID)));
      }

    }
  }

}
