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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.lang.ExecUtils;

public final class FeatureVectorsPartitionTest extends OryxTest {

  @Test
  public void testGetSet() {
    FeatureVectorsPartition fv = new FeatureVectorsPartition();
    assertEquals(0, fv.size());
    fv.setVector("foo", new float[] { 1.0f });
    assertEquals(1, fv.size());
    assertArrayEquals(new float[] { 1.0f }, fv.getVector("foo"));
    fv.removeVector("foo");
    assertEquals(0, fv.size());
    assertNull(fv.getVector("foo"));
  }

  @Test
  public void testVTV() {
    FeatureVectorsPartition fv = new FeatureVectorsPartition();
    fv.setVector("foo", new float[] { 1.0f, 2.0f, 4.0f });
    fv.setVector("bar", new float[] { 1.5f, -1.0f, 0.0f });
    double[] expected = { 3.25, 0.5, 4.0, 5.0, 8.0, 16.0 };
    assertArrayEquals(expected, fv.getVTV(false));
    assertArrayEquals(expected, fv.getVTV(true));
  }

  @Test
  public void testForEach() {
    FeatureVectorsPartition fv = new FeatureVectorsPartition();
    fv.setVector("foo", new float[] { 1.0f, 2.0f, 4.0f });
    fv.setVector("bar", new float[] { 1.5f, -1.0f, 0.0f });
    Collection<String> out = new ArrayList<>();
    fv.forEach((id, vector) -> out.add(id + vector[0]));
    assertEquals(fv.size(), out.size());
    assertContains(out, "foo1.0");
    assertContains(out, "bar1.5");
  }

  @Test
  public void testRetainRecent() {
    FeatureVectorsPartition fv = new FeatureVectorsPartition();
    fv.setVector("foo", new float[] { 1.0f });
    fv.retainRecentAndIDs(Collections.singleton("foo"));
    assertEquals(1, fv.size());
    fv.retainRecentAndIDs(Collections.singleton("bar"));
    assertEquals(0, fv.size());
  }

  @Test
  public void testAllIDs() {
    FeatureVectorsPartition fv = new FeatureVectorsPartition();
    fv.setVector("foo", new float[] { 1.0f });
    Collection<String> allIDs = new HashSet<>();
    fv.addAllIDsTo(allIDs);
    assertEquals(Collections.singleton("foo"), allIDs);
    fv.removeAllIDsFrom(allIDs);
    assertEquals(0, allIDs.size());
  }

  @Test
  public void testRecent() {
    FeatureVectorsPartition fv = new FeatureVectorsPartition();
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
    FeatureVectorsPartition fv = new FeatureVectorsPartition();
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
    ExecUtils.doInParallel(numWorkers, i -> {
      for (int j = 0; j < numIterations; j++) {
        fv.removeVector(Integer.toString(counter.decrementAndGet()));
      }
    });
    assertEquals(0, fv.size());
  }

  @Test
  public void testToString() {
    FeatureVectors vectors = new FeatureVectorsPartition();
    vectors.setVector("A", new float[]{1.0f, 3.0f, 6.0f});
    assertEquals("FeatureVectors[size:1]", vectors.toString());
  }

}
