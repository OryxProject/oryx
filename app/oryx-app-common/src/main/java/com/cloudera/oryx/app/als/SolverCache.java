/*
 * Copyright (c) 2016, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.math3.linear.RealMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.math.LinearSystemSolver;
import com.cloudera.oryx.common.math.Solver;

/**
 * A cache around a {@link Solver} that manages computing it when necessary from
 * {@link FeatureVectors}, tracking when it must be recomputed, and doing so
 * correctly in the presence of multiple threads.
 */
public final class SolverCache {

  private static final Logger log = LoggerFactory.getLogger(SolverCache.class);

  private final AtomicReference<Solver> solver;
  private final AtomicBoolean solverDirty;
  private final AtomicBoolean solverUpdating;
  private final CountDownLatch solverInitialized;
  private final ExecutorService executor;
  private final FeatureVectors[] vectorPartitions;

  /**
   * @param executor {@link ExecutorService} which should be used to asynchronously compute
   *  a {@link Solver}
   * @param vectorPartitions underling {@link FeatureVectors} data from which it should be
   *  computed
   */
  public SolverCache(ExecutorService executor, FeatureVectors... vectorPartitions) {
    solver = new AtomicReference<>();
    solverDirty = new AtomicBoolean(true);
    solverUpdating = new AtomicBoolean(false);
    solverInitialized = new CountDownLatch(1);
    this.executor = executor;
    this.vectorPartitions = vectorPartitions;
  }

  /**
   * Indicates that the underlying {@link FeatureVectors} have changed and so the
   * resulting {@link Solver} should be recomputed.
   */
  public void setDirty() {
    solverDirty.set(true);
  }

  /**
   * @return a recent {@link Solver}, blocking if necessary to wait for an initial one to
   *  be computed. It does not block otherwise and returns the most recently computed one.
   */
  public Solver get() {
    if (solverDirty.getAndSet(false)) {
      // launch asynchronous update
      executor.submit(() -> {
        // Make sure only one attempts to build at one time
        if (solverUpdating.compareAndSet(false, true)) {
          RealMatrix YTY = null;
          for (FeatureVectors yPartition : vectorPartitions) {
            RealMatrix YTYpartial = yPartition.getVTV();
            if (YTYpartial != null) {
              YTY = YTY == null ? YTYpartial : YTY.add(YTYpartial);
            }
          }
          // Possible to compute this twice, but not a big deal
          Solver newYTYSolver = LinearSystemSolver.getSolver(YTY);
          solver.set(newYTYSolver);
          solverUpdating.set(false);
          // Allow any threads waiting for initial model to proceed
          solverInitialized.countDown();
        }
      });
    }
    // Wait, in the case that there is no existing model already.
    // Otherwise this immediately proceeds.
    try {
      solverInitialized.await();
    } catch (InterruptedException e) {
      log.warn("Interrupted while waiting for model", e);
      // continue, but will probably fail because this returns null
    }
    return solver.get();
  }

}
