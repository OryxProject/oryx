/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.kmeans;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.base.Preconditions;

public final class ClusterInfo implements Serializable {

  private final int id;
  private double[] center;
  private long count;

  public ClusterInfo(int id, double[] center, long initialCount) {
    Preconditions.checkArgument(center.length > 0);
    Preconditions.checkArgument(initialCount >= 1);
    this.id = id;
    this.center = center;
    this.count = initialCount;
  }

  public int getID() {
    return id;
  }

  public double[] getCenter() {
    return center;
  }

  public long getCount() {
    return count;
  }

  public synchronized void update(double[] newPoint) {
    update(newPoint, 1);
  }

  public synchronized void update(double[] newPoint, long newCount) {
    int length = center.length;
    Preconditions.checkArgument(length == newPoint.length);
    double[] newCenter = new double[length];
    for (int i = 0; i < length; i++) {
      newCenter[i] = (newPoint[i] * newCount + center[i] * count) / (newCount + count);
    }
    this.center = newCenter;
    count += newCount;
  }

  @Override
  public String toString() {
    return id + " " + Arrays.toString(center) + " " + count;
  }

}
