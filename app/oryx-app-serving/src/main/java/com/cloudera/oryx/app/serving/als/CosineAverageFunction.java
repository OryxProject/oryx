/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.app.serving.als;

import net.openhft.koloboke.function.ToDoubleFunction;

import com.cloudera.oryx.common.math.VectorMath;

final class CosineAverageFunction implements ToDoubleFunction<float[]> {

  private final float[][] itemFeatureVectors;
  private final double[] itemFeatureVectorNorms;

  CosineAverageFunction(float[][] itemFeatureVectors) {
    this.itemFeatureVectors = itemFeatureVectors;
    this.itemFeatureVectorNorms = new double[itemFeatureVectors.length];
    for (int i = 0; i < itemFeatureVectors.length; i++) {
      itemFeatureVectorNorms[i] = VectorMath.norm(itemFeatureVectors[i]);
    }
  }

  @Override
  public double applyAsDouble(float[] itemVector) {
    double itemVectorNorm = VectorMath.norm(itemVector);
    double total = 0.0;
    for (int i = 0; i < itemFeatureVectors.length; i++) {
      total += VectorMath.dot(itemFeatureVectors[i], itemVector) / (itemFeatureVectorNorms[i] * itemVectorNorm);
    }
    return total / itemFeatureVectors.length;
  }
}
