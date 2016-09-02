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
   * @return new target estimated of strength of interaction, or NaN to signal "no change needed"
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
   * @param implicit whether the model is implicit feedback
   * @return new user vector Xu, or {@code null} if no update should be made (i.e. there was no
   *  item vector; the update would push the new Qui farther out of range)
   */
  public static float[] computeUpdatedXu(Solver solver,
                                         double value,
                                         float[] Xu,
                                         float[] Yi,
                                         boolean implicit) {
    if (Yi == null) {
      return null;
    }

    boolean noXu = Xu == null;
    double Qui = noXu ? 0.0 : VectorMath.dot(Xu, Yi);
    // Qui' is the target, new value of Qui
    // 0.5 reflects a "don't know" state
    double targetQui = computeTargetQui(implicit, value, noXu ? 0.5 : Qui);
    if (Double.isNaN(targetQui)) {
      return null;
    }

    double dQui = targetQui - Qui;
    double[] dQuiYi = new double[Yi.length];
    for (int i = 0; i < dQuiYi.length; i++) {
      dQuiYi[i] = Yi[i] * dQui;
    }
    double[] dXu = solver.solveDToD(dQuiYi);

    float[] newXu = noXu ? new float[dXu.length] : Xu.clone();
    for (int i = 0; i < newXu.length; i++) {
      newXu[i] += dXu[i];
    }
    return newXu;
  }

}
