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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import net.openhft.koloboke.function.BiConsumer;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.lang.LoggingVoidCallable;

public final class FeatureVectorsTest extends OryxTest {

  @Test
  public void testGetSet() {
    FeatureVectors fv = new FeatureVectors();
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
    FeatureVectors fv = new FeatureVectors();
    fv.setVector("foo", new float[] { 1.0f, 2.0f, 4.0f });
    fv.setVector("bar", new float[] { 1.5f, -1.0f, 0.0f });
    RealMatrix expected = new Array2DRowRealMatrix(new double[][] {
        { 3.25, 0.5, 4.0 }, { 0.5, 5.0, 8.0 }, { 4.0, 8.0, 16.0 }
    });
    assertEquals(expected, fv.getVTV());
  }

  @Test
  public void testForEach() {
    FeatureVectors fv = new FeatureVectors();
    fv.setVector("foo", new float[] { 1.0f, 2.0f, 4.0f });
    fv.setVector("bar", new float[] { 1.5f, -1.0f, 0.0f });
    final Collection<String> out = new ArrayList<>();
    fv.forEach(new BiConsumer<String, float[]>() {
      @Override
      public void accept(String id, float[] vector) {
        out.add(id + vector[0]);
      }
    });
    assertEquals(fv.size(), out.size());
    assertTrue(out.contains("foo1.0"));
    assertTrue(out.contains("bar1.5"));
  }

  @Test
  public void testRetainRecent() {
    FeatureVectors fv = new FeatureVectors();
    fv.setVector("foo", new float[] { 1.0f });
    fv.retainRecentAndIDs(Collections.singleton("foo"));
    assertEquals(1, fv.size());
    fv.retainRecentAndIDs(Collections.singleton("bar"));
    assertEquals(0, fv.size());
  }

  @Test
  public void testAllIDs() {
    FeatureVectors fv = new FeatureVectors();
    fv.setVector("foo", new float[] { 1.0f });
    Collection<String> allIDs = new HashSet<>();
    fv.addAllIDsTo(allIDs);
    assertEquals(Collections.singleton("foo"), allIDs);
    fv.removeAllIDsFrom(allIDs);
    assertTrue(allIDs.isEmpty());
  }

  @Test
  public void testRecent() {
    FeatureVectors fv = new FeatureVectors();
    fv.setVector("foo", new float[] { 1.0f });
    Collection<String> recentIDs = new HashSet<>();
    fv.addAllRecentTo(recentIDs);
    assertEquals(Collections.singleton("foo"), recentIDs);
    fv.retainRecentAndIDs(Collections.singleton("foo"));
    recentIDs.clear();
    fv.addAllRecentTo(recentIDs);
    assertTrue(recentIDs.isEmpty());
  }

  @Test
  public void testConcurrent() throws Exception {
    final FeatureVectors fv = new FeatureVectors();
    final AtomicInteger counter = new AtomicInteger();
    int numThreads = 16;

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      final int numIterations = 1000;
      Collection<Callable<Void>> adds = new ArrayList<>(numThreads);
      for (int i = 0; i < numThreads; i++) {
        adds.add(new LoggingVoidCallable() {
          @Override
          public void doCall() {
            for (int j = 0; j < numIterations; j++) {
              int i = counter.getAndIncrement();
              fv.setVector(Integer.toString(i), new float[]{i});
            }
          }
        });
      }
      executor.invokeAll(adds);

      Collection<Callable<Void>> removes = new ArrayList<>(numThreads);
      for (int i = 0; i < numThreads; i++) {
        removes.add(new LoggingVoidCallable() {
          @Override
          public void doCall() {
            for (int j = 0; j < numIterations; j++) {
              fv.removeVector(Integer.toString(counter.decrementAndGet()));
            }
          }
        });
      }
      executor.invokeAll(removes);
    } finally {
      executor.shutdown();
    }

    assertEquals(0, fv.size());
  }

}
