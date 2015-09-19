/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.als;

import com.cloudera.oryx.common.math.Solver;
import com.cloudera.oryx.common.math.VectorMath;

/**
 * ALS-related utility methods for the app tier.
 */
public final class ALSUtils {

  private ALSUtils() {}

  /**
   * Computes how the estimated strength of interaction in the model should change -- to what target
   * value -- in response to a new interaction.
   *
   * @param implicit whether the model is implicit feedback
   * @param value new interaction's strength
   * @param currentValue existing estimated of strength of interaction
   * @return new target estimated of strength of interaction
   */
  public static double computeTargetQui(boolean implicit, double value, double currentValue) {
    // We want Qui to change based on value. What's the target value, Qui'?
    if (implicit) {
      // Target is really 1, or 0, depending on whether value is positive or negative.
      // This wouldn't account for the strength though. Instead the target is a function
      // of the current value and strength. If the current value is c, and value is positive
      // then the target is somewhere between c and 1 depending on the strength. If current
      // value is already >= 1, there's no effect. Similarly for negative values.
      if (value > 0.0f && currentValue < 1.0) {
        double diff = 1.0 - Math.max(0.0, currentValue);
        return currentValue + (value / (1.0 + value)) * diff;
      }
      if (value < 0.0f && currentValue > 0.0) {
        double diff = -Math.min(1.0, currentValue);
        return currentValue + (value / (value - 1.0)) * diff;
      }
      // No change
      return Double.NaN;
    } else {
      // Non-implicit -- value is supposed to be the new value
      return value;
    }
  }

  /**
   * Computes how a user vector Xu changes in response to interaction with an item vector Yi.
   * This can also be used to compute how an item vector changes in response to a user interaction,
   * even though the code naming follows the former convention.
   *
   * @param solver solver helping solve for Xu in Qu*Y = Xu * (Yt * Y)
   * @param value strength of interaction
   * @param Xu current user vector (null if no existing user vector)
   * @param Yi current item vector
   * @param numFeatures number of model features
   * @param implicit whether the model is implicit feedback
   * @return new user vector Xu
   */
  public static float[] computeUpdatedXu(Solver solver,
                                         double value,
                                         float[] Xu,
                                         float[] Yi,
                                         int numFeatures,
                                         boolean implicit) {
    float[] newXu = null;
    if (Yi != null) {
      // Let Qui = Xu * (Yi)^t -- it's the current estimate of user-item interaction in Q = X * Y^t
      double Qui = Xu == null ? 0.0 : VectorMath.dot(Xu, Yi);
      // Qui' is the target, new value of Qui
      // 0.5 reflects a "don't know" state
      double targetQui = computeTargetQui(implicit, value, Xu == null ? 0.5 : Qui);
      if (!Double.isNaN(targetQui)) {
        // In Qu = Xu * Y^T, Xu is going to change to Xu' such that Qu' = Xu' * Y^T. Qu' will change
        // from Qu by the vector dQu = [0, 0, ..., dQui, ...] where the nonzero value
        // dQui = (Qui' - Qui) is in position i. The change dXu from Xu to Xu' should satisfy
        // dQu = dXu * Y^T. We solve for dXu and then add it to Xu. dQu * Y = dXu * (Y^t * Y).
        // dQu is 0 except for one value at position i, so dQu * Y is really dQui*Yi
        double dQui = targetQui - Qui;
        float[] dQuiYi = Yi.clone();
        for (int i = 0; i < dQuiYi.length; i++) {
          dQuiYi[i] *= dQui;
        }
        double[] dXu = solver.solveFToD(dQuiYi);
        newXu = Xu == null ? new float[numFeatures] : Xu.clone();
        for (int i = 0; i < newXu.length; i++) {
          newXu[i] += dXu[i];
        }
      }
    }
    return newXu;
  }

}
