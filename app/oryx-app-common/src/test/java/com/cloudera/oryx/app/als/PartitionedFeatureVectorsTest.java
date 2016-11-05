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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.cloudera.oryx.common.lang.ExecUtils;

public final class PartitionedFeatureVectorsTest extends AbstractFeatureVectorTest {

  private static final int NUM_PARTITIONS = 2;

  @Test
  public void testGetSet() {
    PartitionedFeatureVectors fv = new PartitionedFeatureVectors(NUM_PARTITIONS, getExecutor());
    assertEquals(0, fv.size());
    fv.setVector("foo", new float[] { 1.0f });
    assertEquals(1, fv.size());
    assertArrayEquals(new float[] { 1.0f }, fv.getVector("foo"));
  }

  @Test
  public void testVTV() {
    PartitionedFeatureVectors fv = new PartitionedFeatureVectors(NUM_PARTITIONS,  getExecutor());
    fv.setVector("foo", new float[] { 1.0f, 2.0f, 4.0f });
    fv.setVector("bar", new float[] { 1.5f, -1.0f, 0.0f });
    double[] expected = { 3.25, 0.5, 4.0, 5.0, 8.0, 16.0 };
    assertArrayEquals(expected, fv.getVTV(false));
    assertArrayEquals(expected, fv.getVTV(true));
  }

  @Test
  public void testRetainRecent() {
    PartitionedFeatureVectors fv = new PartitionedFeatureVectors(NUM_PARTITIONS,  getExecutor());
    fv.setVector("foo", new float[] { 1.0f });
    fv.retainRecentAndIDs(Collections.singleton("foo"));
    assertEquals(1, fv.size());
    fv.retainRecentAndIDs(Collections.singleton("bar"));
    assertEquals(0, fv.size());
  }

  @Test
  public void testAllIDs() {
    PartitionedFeatureVectors fv = new PartitionedFeatureVectors(NUM_PARTITIONS,  getExecutor());
    fv.setVector("foo", new float[] { 1.0f });
    Collection<String> allIDs = new HashSet<>();
    fv.addAllIDsTo(allIDs);
    assertEquals(Collections.singleton("foo"), allIDs);
    fv.removeAllIDsFrom(allIDs);
    assertEquals(0, allIDs.size());
  }

  @Test
  public void testRecent() {
    PartitionedFeatureVectors fv = new PartitionedFeatureVectors(NUM_PARTITIONS,  getExecutor());
    fv.setVector("foo", new float[] { 1.0f });
    Collection<String> recentIDs = new HashSet<>();
    fv.addAllRecentTo(recentIDs);
    assertEquals(Collections.singleton("foo"), recentIDs);
    fv.retainRecentAndIDs(Collections.singleton("foo"));
    recentIDs.clear();
    fv.addAllRecentTo(recentIDs);
    assertEquals(0, recentIDs.size());
  }

  @Test
  public void testConcurrent() throws Exception {
    PartitionedFeatureVectors fv = new PartitionedFeatureVectors(NUM_PARTITIONS,  getExecutor());
    AtomicInteger counter = new AtomicInteger();
    int numWorkers = 16;
    int numIterations = 10000;
    ExecUtils.doInParallel(numWorkers, i -> {
      for (int j = 0; j < numIterations; j++) {
        int c = counter.getAndIncrement();
        fv.setVector(Integer.toString(c), new float[] { c });
      }
    });
    assertEquals((long) numIterations * numWorkers, fv.size());
    assertEquals((long) numIterations * numWorkers, counter.get());
  }

  @Test
  public void testToString() {
    PartitionedFeatureVectors partitioned = new PartitionedFeatureVectors(2, getExecutor());
    partitioned.setVector("A", new float[] { 1.0f, 3.0f, 6.0f });
    assertEquals("[1:1]", partitioned.toString());
  }

}
