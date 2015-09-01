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

package com.cloudera.oryx.app.batch.mllib.kmeans;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
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

  private static JavaRDD<Vector> getRddOfVectors() {
    List<double[]> points = Arrays.asList(new double[][] {
        {1.0, 0.0}, {2.0, -2.0}, {2.0, 0.0}, {-2.0, 0.0}, {-0.5, -1.0}, {-0.5, 1.0}
    });
    return getJavaSparkContext().parallelize(points).map(Vectors::dense);
  }

  private static List<ClusterInfo> getClusters() {
    return KMeansPMMLUtils.read(KMeansPMMLUtilsTest.buildDummyClusteringModel());
  }

  @Test
  public void testFetchSampleEvalData() {
    JavaRDD<Vector> evalData = SilhouetteCoefficient.fetchSampleData(getRddOfVectors());
    assertEquals(6, evalData.count());
  }

  @Test
  public void testDunnIndexForClustering() {
    List<ClusterInfo> clusters = getClusters();
    DunnIndex dunnIndex = new DunnIndex(clusters);
    double eval = dunnIndex.evaluate(getRddOfVectors());
    log.info("Dunn Index for {} clusters: {}", clusters.size(), eval);
    assertEquals(1.3110480733464633, eval);
  }

  @Test
  public void testDaviesBouldinIndexForClustering() {
    List<ClusterInfo> clusters = getClusters();
    DaviesBouldinIndex daviesBouldinIndex = new DaviesBouldinIndex(clusters);
    double eval = daviesBouldinIndex.evaluate(getRddOfVectors());
    log.info("Davies Bouldin Index for {} clusters: {}", clusters.size(), eval);
    assertEquals(0.9702216688254247, eval);
  }

  @Test
  public void testSilhouetteCoefficientForClustering() {
    List<ClusterInfo> clusters = getClusters();
    SilhouetteCoefficient silhouetteCoefficient = new SilhouetteCoefficient(clusters);
    double eval = silhouetteCoefficient.evaluate(getRddOfVectors());
    log.info("Silhouette Coefficient for {} clusters: {}", clusters.size(), eval);
    assertEquals(0.30648167401009796, eval);
  }

  @Test
  public void testSSEForClustering() {
    List<ClusterInfo> clusters = getClusters();
    SumSquaredError sse = new SumSquaredError(clusters);
    double eval = sse.evaluate(getRddOfVectors());
    log.info("SSE for {} clusters: {}", clusters.size(), eval);
    assertEquals(5.5, eval);
  }

  @Test
  public void testComputeSilhouetteCoefficient() {
    assertEquals(5.0, SilhouetteCoefficient.silhouetteCoefficient(-0.8, 0.2));
    assertEquals(-1.25, SilhouetteCoefficient.silhouetteCoefficient(0.8, -0.2));
    assertEquals(0.0, SilhouetteCoefficient.silhouetteCoefficient(1.5, 1.5));
    assertEquals(1.0, SilhouetteCoefficient.silhouetteCoefficient(1.5, Double.POSITIVE_INFINITY));
    assertEquals(-1.0, SilhouetteCoefficient.silhouetteCoefficient(Double.POSITIVE_INFINITY, 1.5));
  }

}
