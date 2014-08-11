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

import org.apache.commons.math3.random.RandomGenerator;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.common.collection.FormatUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.kafka.util.RandomDatumGenerator;

final class MockModelUpdateGenerator implements RandomDatumGenerator<String,String> {

  private final int features;

  MockModelUpdateGenerator(int features) {
    this.features = features;
  }

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) throws IOException {
    if (id % 10 == 0) {
      PMML pmml = PMMLUtils.buildSkeletonPMML();
      PMMLUtils.addExtension(pmml, "features", Integer.toString(features));
      PMMLUtils.addExtensionContent(pmml, "XIDs", Arrays.asList(6,7,8,9));
      PMMLUtils.addExtensionContent(pmml, "YIDs", Arrays.asList(1,2,3,4,5));
      return new Pair<>("MODEL", PMMLUtils.toString(pmml));
    } else {
      int xOrYID = id % 10;
      boolean isX = xOrYID >= 6;
      float[] vector = new float[features];
      for (int i = 0; i < vector.length; i++) {
        vector[i] = (float) xOrYID + i * i;
      }
      return new Pair<>(
          "UP", (isX ? "X" : "Y") + "\t" + xOrYID + "\t" + FormatUtils.formatFloatVec(vector));

    }
  }

}
