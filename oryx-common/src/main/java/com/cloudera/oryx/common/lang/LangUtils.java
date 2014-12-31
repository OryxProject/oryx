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

package com.cloudera.oryx.common.lang;

public final class LangUtils {

  private LangUtils() {
  }

  /**
   * Like {@link com.google.common.primitives.Doubles#hashCode(double)} but avoids creating
   * a whole new object!
   *
   * @param d double to hash
   * @return the same value produced by {@link Double#hashCode()}
   */
  public static int hashDouble(double d) {
    long bits = Double.doubleToLongBits(d);
    return (int) (bits ^ (bits >>> 32));
  }

}
