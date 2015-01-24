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

import com.google.common.base.Preconditions;

public final class ClusterInfo {

  private final int id;
  private final double[] center;
  private long count;

  public ClusterInfo(int id, double[] center) {
    Preconditions.checkArgument(center.length > 0);
    this.id = id;
    this.center = center;
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

  public void setCount(long count) {
    this.count = count;
  }

}
