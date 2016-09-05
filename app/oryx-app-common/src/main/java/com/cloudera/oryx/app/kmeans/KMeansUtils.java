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

import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;

import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;

/**
 * K-means-related utility methods for the app tier.
 */
public final class KMeansUtils {

  private KMeansUtils() {}

  /**
   * @param clusters all clusters
   * @param distanceFn distance function between clusters
   * @param vector point to measure distance to
   * @return cluster whose center is closest to the given point
   */
  public static Pair<ClusterInfo,Double> closestCluster(List<ClusterInfo> clusters,
                                                        DistanceFn<double[]> distanceFn,
                                                        double[] vector) {
    Preconditions.checkArgument(!clusters.isEmpty());
    double closestDist = Double.POSITIVE_INFINITY;
    ClusterInfo minCluster = null;
    for (ClusterInfo cluster : clusters) {
      double distance = distanceFn.applyAsDouble(cluster.getCenter(), vector);
      if (distance < closestDist) {
        closestDist = distance;
        minCluster = cluster;
      }
    }
    Preconditions.checkNotNull(minCluster);
    Preconditions.checkState(!Double.isInfinite(closestDist) && !Double.isNaN(closestDist));
    return new Pair<>(minCluster, closestDist);
  }

  /**
   * @param data tokenized input
   * @param inputSchema input schema to apply to input
   * @return input parsed as numeric values
   */
  public static double[] featuresFromTokens(String[] data, InputSchema inputSchema) {
    double[] features = new double[inputSchema.getNumPredictors()];
    for (int featureIndex = 0; featureIndex < data.length; featureIndex++) {
      if (inputSchema.isActive(featureIndex)) {
        features[inputSchema.featureToPredictorIndex(featureIndex)] =
            Double.parseDouble(data[featureIndex]);
      }
    }
    return features;
  }

  /**
   * @param clusters clusters to check the IDs of
   * @throws IllegalArgumentException if any {@link ClusterInfo#getID()} is duplicated
   */
  public static void checkUniqueIDs(Collection<ClusterInfo> clusters) {
    Preconditions.checkArgument(clusters.stream().map(ClusterInfo::getID).distinct().count() == clusters.size());
  }

}
