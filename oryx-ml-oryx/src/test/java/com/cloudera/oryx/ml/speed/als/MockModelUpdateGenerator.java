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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.random.RandomGenerator;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.common.collection.FormatUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.math.VectorMath;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;

public final class MockModelUpdateGenerator implements RandomDatumGenerator<String,String> {

  /*
   A = [ 1 0 0 1 0 ; 0 1 0 1 1 ; 1 1 1 1 0 ; 0 0 1 0 0 ]
   [U,S,V] = svd(A)

   U = U(:,1:2)
   S = S(1:2,1:2)
   V = V(:,1:2)

   X = U*sqrt(S)
   Y = V*sqrt(S)
   */
  public static final Map<String,float[]> X = buildMatrix(6, new double[][] {
      {-0.679001918401210,  0.173232408449017},
      {-0.823244234718400, -0.920085196137775},
      {-1.186534432549093,  0.446318558864201},
      {-0.207895139404806,  0.530350819368002},
  });
  public static final Map<String,float[]> Y = buildMatrix(1, new double[][] {
      {-0.720323513289685,  0.456546350776373},
      {-0.776018558846806, -0.349118056105777},
      {-0.538419102792183,  0.719706471415318},
      {-1.038195732260794, -0.221463305994448},
      {-0.317872218971108, -0.678009656770822},
  });

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
      String xOrYIDString = Integer.toString(xOrYID);
      String message;
      boolean isX = xOrYID >= 6;
      if (isX) {
        message = "X\t" + xOrYIDString + "\t" + FormatUtils.formatFloatVec(X.get(xOrYIDString));
      } else {
        message = "Y\t" + xOrYIDString + "\t" + FormatUtils.formatFloatVec(Y.get(xOrYIDString));
      }
      return new Pair<>("UP", message);
    }
  }

  static Map<String,float[]> buildMatrix(int startIndex, double[]... rows) {
    Map<String,float[]> matrix = new HashMap<>(rows.length);
    int index = startIndex;
    for (double[] row : rows) {
      matrix.put(Integer.toString(index), VectorMath.toFloats(row));
      index++;
    }
    return Collections.unmodifiableMap(matrix);
  }

}
