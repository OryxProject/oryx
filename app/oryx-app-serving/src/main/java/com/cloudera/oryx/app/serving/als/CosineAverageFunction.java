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

import java.util.Objects;

import com.cloudera.oryx.common.math.VectorMath;

/**
 * Computes the cosine of the angle between a target vector and other vectors.
 */
public final class CosineAverageFunction implements CosineDistanceSensitiveFunction {

  private final float[] itemFeaturesVector;

  public CosineAverageFunction(float[] itemFeatureVector) {
    Objects.requireNonNull(itemFeatureVector);
    float[] itemFeaturesVector = new float[itemFeatureVector.length];
    double vecNorm = VectorMath.norm(itemFeatureVector);
    for (int i = 0; i < itemFeatureVector.length; i++) {
      itemFeaturesVector[i] = (float) (itemFeatureVector[i] / vecNorm);
    }
    this.itemFeaturesVector = itemFeaturesVector;
  }

  public CosineAverageFunction(float[][] itemFeatureVectors) {
    double[] tempVector = new double[itemFeatureVectors[0].length];
    for (float[] vec : itemFeatureVectors) {
      for (int i = 0; i < vec.length; i++) {
        tempVector[i] += vec[i];
      }
    }
    for (int i = 0; i < tempVector.length; i++) {
      tempVector[i] /= tempVector.length;
    }
    float[] itemFeaturesVector = new float[tempVector.length];
    double vecNorm = VectorMath.norm(tempVector);
    for (int i = 0; i < itemFeaturesVector.length; i++) {
      itemFeaturesVector[i] = (float) (tempVector[i] / vecNorm);
    }
    this.itemFeaturesVector = itemFeaturesVector;
  }

  @Override
  public double applyAsDouble(float[] itemVector) {
    return VectorMath.cosineSimilarity(itemVector, itemFeaturesVector, 1.0); // normalized already
  }

  @Override
  public float[] getTargetVector() {
    return itemFeaturesVector;
  }

}
