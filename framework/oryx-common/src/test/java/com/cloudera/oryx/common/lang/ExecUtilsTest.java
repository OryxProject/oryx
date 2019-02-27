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

package com.cloudera.oryx.common.lang;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link ExecUtils}.
 */
public final class ExecUtilsTest extends OryxTest {

  @Test
  public void testDoInParallel() {
    assertEquals(1, maxActive(1, 1, false));
    assertEquals(1, maxActive(1, 1, true));
    assertEquals(1, maxActive(4, 1, false));
    assertEquals(1, maxActive(4, 1, true));
    int cores = Runtime.getRuntime().availableProcessors();
    assertEquals(Math.min(cores, 4), maxActive(4, 4, false));
    assertEquals(4, maxActive(4, 4, true));
  }

  private static int maxActive(int numTasks, int parallelism, boolean privatePool) {
    Collection<Integer> active = new HashSet<>();
    AtomicInteger maxActive = new AtomicInteger();
    ExecUtils.doInParallel(numTasks, parallelism, privatePool, i -> {
      synchronized (active) {
        active.add(i);
        maxActive.set(Math.max(maxActive.get(), active.size()));
      }
      sleepSeconds(1);
      synchronized (active) {
        active.remove(i);
      }
    });
    return maxActive.get();
  }

  @Test
  public void testCollectInParallel() {
    assertEquals(1, maxActiveCollect(1, 1, false));
    assertEquals(1, maxActiveCollect(1, 1, true));
    assertEquals(1, maxActiveCollect(4, 1, false));
    assertEquals(1, maxActiveCollect(4, 1, true));
    int cores = Runtime.getRuntime().availableProcessors();
    assertEquals(Math.min(cores, 4), maxActiveCollect(4, 4, false));
    assertEquals(4, maxActiveCollect(4, 4, true));
  }

  private static int maxActiveCollect(int numTasks, int parallelism, boolean privatePool) {
    Collection<Integer> active = new HashSet<>();
    AtomicInteger maxActive = new AtomicInteger();
    Set<Integer> completed = ExecUtils.collectInParallel(numTasks, parallelism, privatePool, i -> {
      synchronized (active) {
        active.add(i);
        maxActive.set(Math.max(maxActive.get(), active.size()));
      }
      sleepSeconds(1);
      synchronized (active) {
        active.remove(i);
      }
      return i;
    }, Collectors.toSet());
    assertEquals(numTasks, completed.size());
    for (int i = 0; i < numTasks; i++) {
      assertTrue(completed.contains(i));
    }
    return maxActive.get();
  }

}
