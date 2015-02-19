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

package com.cloudera.oryx.app.mllib.kmeans;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtilsTest;
import com.cloudera.oryx.lambda.AbstractSparkIT;

public final class KMeansEvalIT extends AbstractSparkIT {

  private static final Logger log = LoggerFactory.getLogger(KMeansEvalIT.class);

  private static final int NUM_CLUSTERS = 3;
  private final List<double[]> POINTS =
      Arrays.asList(new double[][]{
          {1.0, 0.0}, {2.0, -2.0}, {2.0, 0.0},
          {-2.0, 0.0}, {-0.5, -1.0}, {-0.5, 1.0}
      });

  protected final JavaRDD<Vector> getRddOfVectors(List<double[]> pointsList) {
    return getJavaSparkContext().parallelize(pointsList).map(new ToVectorFn());
  }

  private static final class ToVectorFn implements Function<double[], Vector> {
    @Override
    public Vector call(double[] v) {
      return Vectors.dense(v);
    }
  }

  private static List<ClusterInfo> getClusters() {
    return KMeansPMMLUtils.read(KMeansPMMLUtilsTest.buildDummyClusteringModel());
  }

  @Test
  public void testFetchSampleEvalData() {
    JavaRDD<Vector> evalData = SilhouetteCoefficient.fetchSampleData(getRddOfVectors(POINTS));
    assertEquals(6, evalData.count());
  }

  @Test
  public void testDunnIndexForClustering() {
    List<ClusterInfo> clusters = getClusters();
    DunnIndex dunnIndex = new DunnIndex(clusters);
    double eval = dunnIndex.evaluate(getRddOfVectors(POINTS));
    assertEquals(3, clusters.size());
    log.info("Dunn Index for {} clusters: {}", clusters.size(), eval);
    assertEquals(1.7142857142857142, eval);
  }

  @Test
  public void testDaviesBouldinIndexForClustering() {
    List<ClusterInfo> clusters = getClusters();
    DaviesBouldinIndex daviesBouldinIndex = new DaviesBouldinIndex(clusters);
    double eval = daviesBouldinIndex.evaluate(getRddOfVectors(POINTS));
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Davies Bouldin Index for {} clusters: {}", clusters.size(), eval);
    assertEquals(0.638888888888889, eval);
  }

  @Test
  public void testSilhouetteCoefficientForClustering() {
    List<ClusterInfo> clusters = getClusters();
    SilhouetteCoefficient silhouetteCoefficient = new SilhouetteCoefficient(clusters);
    double eval = silhouetteCoefficient.evaluate(getRddOfVectors(POINTS));
    assertEquals(NUM_CLUSTERS, clusters.size());
    log.info("Silhouette Coefficient for {} clusters: {}", clusters.size(), eval);
    assertEquals(0.48484126984126985, eval);
  }

}
