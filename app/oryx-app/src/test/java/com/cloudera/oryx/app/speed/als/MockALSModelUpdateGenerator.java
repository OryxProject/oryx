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

package com.cloudera.oryx.app.speed.als;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.random.RandomGenerator;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.app.als.ALSUtilsTest;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.kafka.util.DatumGenerator;

public final class MockALSModelUpdateGenerator implements DatumGenerator<String,String> {

  /*
   A = [ 1 0 0 1 0 ; 0 1 0 1 1 ; 1 1 1 1 0 ; 0 0 1 0 0 ]
   */

  public static final Map<String,Collection<String>> A = new HashMap<>();
  static {
    A.put(ALSUtilsTest.idToStringID(6), Arrays.asList("1", "4"));
    A.put(ALSUtilsTest.idToStringID(7), Arrays.asList("2", "4", "5"));
    A.put(ALSUtilsTest.idToStringID(8), Arrays.asList("1", "2", "3", "4"));
    A.put(ALSUtilsTest.idToStringID(9), Arrays.asList("3"));
  }
  public static final Map<String,Collection<String>> At = new HashMap<>();
  static {
    At.put(ALSUtilsTest.idToStringID(1), Arrays.asList("6", "8"));
    At.put(ALSUtilsTest.idToStringID(2), Arrays.asList("7", "8"));
    At.put(ALSUtilsTest.idToStringID(3), Arrays.asList("8", "9"));
    At.put(ALSUtilsTest.idToStringID(4), Arrays.asList("6", "7", "8"));
    At.put(ALSUtilsTest.idToStringID(5), Arrays.asList("7"));
  }

  /*
   [U,S,V] = svd(A)

   U = U(:,1:2)
   S = S(1:2,1:2)
   V = V(:,1:2)

   X = U*sqrt(S)
   Y = V*sqrt(S)
   */

  public static final Map<String,float[]> X = buildMatrix(6, new float[][] {
      {-0.6790019f,   0.1732324f},
      {-0.8232442f,  -0.9200852f},
      {-1.1865344f,   0.44631857f},
      {-0.20789514f,  0.5303508f},
  });
  public static final Map<String,float[]> Y = buildMatrix(1, new float[][] {
      {-0.7203235f,   0.45654634f},
      {-0.77601856f, -0.34911805f},
      {-0.5384191f,   0.7197065f},
      {-1.0381957f,  -0.22146331f},
      {-0.31787223f, -0.6780096f},
  });

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    if (id % 10 == 0) {
      PMML pmml = PMMLUtils.buildSkeletonPMML();
      AppPMMLUtils.addExtension(pmml, "features", 2);
      AppPMMLUtils.addExtension(pmml, "implicit", true);
      AppPMMLUtils.addExtensionContent(pmml, "XIDs", X.keySet());
      AppPMMLUtils.addExtensionContent(pmml, "YIDs", Y.keySet());
      return new Pair<>("MODEL", PMMLUtils.toString(pmml));
    } else {
      int xOrYID = id % 10;
      String xOrYIDString = ALSUtilsTest.idToStringID(id);
      String message;
      boolean isX = xOrYID >= 6;
      if (isX) {
        message = TextUtils.joinJSON(Arrays.asList(
            "X", xOrYIDString, X.get(xOrYIDString), A.get(xOrYIDString)));
      } else {
        message = TextUtils.joinJSON(Arrays.asList(
            "Y", xOrYIDString, Y.get(xOrYIDString), At.get(xOrYIDString)));
      }
      return new Pair<>("UP", message);
    }
  }

  static Map<String,float[]> buildMatrix(int startIndex, float[]... rows) {
    Map<String,float[]> matrix = new HashMap<>(rows.length);
    int index = startIndex;
    for (float[] row : rows) {
      matrix.put(ALSUtilsTest.idToStringID(index), row);
      index++;
    }
    return Collections.unmodifiableMap(matrix);
  }

}
