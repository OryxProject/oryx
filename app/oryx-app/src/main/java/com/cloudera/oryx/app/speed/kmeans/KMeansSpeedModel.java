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

package com.cloudera.oryx.app.speed.kmeans;

import java.io.Serializable;
import java.util.List;

import com.cloudera.oryx.api.speed.SpeedModel;
import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import com.cloudera.oryx.app.kmeans.KMeansUtils;
import com.cloudera.oryx.app.kmeans.EuclideanDistanceFn;

/**
 * Contains all data structures needed to create near-real-time updates for a
 * k-means clustering model.
 */
public final class KMeansSpeedModel implements SpeedModel, Serializable {

  private final List<ClusterInfo> clusters;
  private final DistanceFn<double[]> distanceFn;

  public KMeansSpeedModel(List<ClusterInfo> clusters) {
    KMeansUtils.checkUniqueIDs(clusters);
    this.clusters = clusters;
    distanceFn = new EuclideanDistanceFn(); // For now, this is the only thing supported
  }

  public ClusterInfo getCluster(int id) {
    return clusters.get(id);
  }

  public void setCluster(int clusterID, ClusterInfo clusterInfo) {
    clusters.set(clusterID, clusterInfo);
  }

  public ClusterInfo closestCluster(double[] vector) {
    return KMeansUtils.closestCluster(clusters, distanceFn, vector).getFirst();
  }

  @Override
  public float getFractionLoaded() {
    return 1.0f;
  }

  @Override
  public String toString() {
    return "KMeansSpeedModel[numClusters=" + clusters.size() + "]";
  }
}
