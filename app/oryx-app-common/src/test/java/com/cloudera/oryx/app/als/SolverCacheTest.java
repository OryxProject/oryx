/*
 * Copyright (c) 2016, Cloudera, Inc. All Rights Reserved.
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

import org.junit.Test;

import com.cloudera.oryx.common.math.Solver;

public final class SolverCacheTest extends AbstractFeatureVectorTest {

  @Test
  public void testCache() throws Exception {
    FeatureVectors vectors = new FeatureVectorsPartition();
    addVectors(vectors);
    doTestCache(new SolverCache(getExecutor(), vectors));

    FeatureVectors partitioned = new PartitionedFeatureVectors(2, getExecutor());
    addVectors(partitioned);
    doTestCache(new SolverCache(getExecutor(), partitioned));
  }

  @Test
  public void testSolveFailure() {
    // Test this doesn't hang
    new SolverCache(getExecutor(), new FeatureVectorsPartition()).get(true);

    // Will fail to solve
    FeatureVectors vectors = new FeatureVectorsPartition();
    vectors.setVector("A", new float[] { 1.0f, 3.0f, 6.0f });
    vectors.setVector("B", new float[] { 0.0f, 1.0f, 2.0f });

    // Test this doesn't hang
    new SolverCache(getExecutor(), vectors).get(true);
  }

  private static void addVectors(FeatureVectors vectors) {
    vectors.setVector("A", new float[] { 1.0f, 3.0f, 6.0f });
    vectors.setVector("B", new float[] { 0.0f, 1.0f, 2.0f });
    vectors.setVector("C", new float[] { -2.0f, 4.0f, -13.0f });
  }

  private static void doTestCache(SolverCache cache) {
    assertNull(cache.get(false));

    Solver solver1 = cache.get(true);
    assertNotNull(solver1);

    assertSame(solver1, cache.get(false));

    cache.compute();
    Solver solver2 = cache.get(false);
    assertNotNull(solver2);

    cache.setDirty();
    assertSame(solver2, cache.get(false));
    Solver solver3 = cache.get(false);
    assertNotNull(solver3);

    assertSame(solver3, cache.get(false));
  }

}
