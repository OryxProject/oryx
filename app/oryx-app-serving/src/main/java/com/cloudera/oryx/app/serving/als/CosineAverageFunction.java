/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

import com.cloudera.oryx.common.math.VectorMath;

/**
 * Computes the cosine of the angle between a target vector and other vectors.
 */
public final class CosineAverageFunction implements CosineDistanceSensitiveFunction {

  private final double[] itemFeaturesVector;

  public CosineAverageFunction(float[] itemFeatureVector) {
    this.itemFeaturesVector = new double[itemFeatureVector.length];
    double vecNorm = VectorMath.norm(itemFeatureVector);
    for (int i = 0; i < itemFeatureVector.length; i++) {
      itemFeaturesVector[i] += itemFeatureVector[i] / vecNorm;
    }
  }

  public CosineAverageFunction(float[][] itemFeatureVectors) {
    this.itemFeaturesVector = new double[itemFeatureVectors[0].length];
    for (float[] vec : itemFeatureVectors) {
      double vecNorm = VectorMath.norm(vec);
      for (int i = 0; i < vec.length; i++) {
        itemFeaturesVector[i] += vec[i] / vecNorm;
      }
    }
    for (int i = 0; i < itemFeaturesVector.length; i++) {
      itemFeaturesVector[i] /= itemFeatureVectors.length;
    }
  }

  @Override
  public double applyAsDouble(float[] itemVector) {
    return VectorMath.dot(itemFeaturesVector, itemVector) / VectorMath.norm(itemVector);
  }

  @Override
  public double[] getTargetVector() {
    return itemFeaturesVector;
  }

}
