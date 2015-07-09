/*
 * Copyright (c) 2015, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.kmeans.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.DistanceFn;
import com.cloudera.oryx.app.kmeans.KMeansUtils;
import com.cloudera.oryx.app.kmeans.SquaredDistanceFn;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;

/**
 * Contains all data structures needed to serve queries for a
 * k-means clustering application.
 */
public final class KMeansServingModel {

  private final List<ClusterInfo> clusters;
  private final InputSchema inputSchema;
  private final DistanceFn<double[]> distanceFn;

  KMeansServingModel(List<ClusterInfo> clusters, InputSchema inputSchema) {
    Objects.requireNonNull(clusters);
    Objects.requireNonNull(inputSchema);
    KMeansUtils.checkUniqueIDs(clusters);
    this.clusters = Collections.synchronizedList(clusters);
    this.inputSchema = inputSchema;
    distanceFn = new SquaredDistanceFn(); // For now, this is the only thing supported
  }

  public int getNumClusters() {
    return clusters.size();
  }

  public ClusterInfo getCluster(int id) {
    return clusters.get(id);
  }

  public InputSchema getInputSchema() {
    return inputSchema;
  }

  public Pair<ClusterInfo,Double> closestCluster(double[] vector) {
    return KMeansUtils.closestCluster(clusters, distanceFn, vector);
  }

  public void update(int clusterID, double[] center, long count) {
    clusters.set(clusterID, new ClusterInfo(clusterID, center, count));
  }

  @Override
  public String toString() {
    return "KMeansServingModel[clusters:" + clusters.size() + "]";
  }

}
