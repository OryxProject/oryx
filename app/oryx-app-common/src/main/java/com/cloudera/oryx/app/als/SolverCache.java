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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.lang.LoggingCallable;
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
  private final Executor executor;
  private final FeatureVectors vectorPartitions;

  /**
   * @param executor {@link Executor} which should be used to asynchronously compute
   *  a {@link Solver}. Important: this should be able to execute more than 1 task in parallel.
   * @param vectorPartitions underling {@link FeatureVectors} data from which it should be
   *  computed
   */
  public SolverCache(Executor executor, FeatureVectors vectorPartitions) {
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
   * Proactively try to compute the solver asynchronously, if not already computed.
   * Does not block.
   */
  public void compute() {
    // Make sure only one attempts to build at one time
    if (solverUpdating.compareAndSet(false, true)) {
      executor.execute(LoggingCallable.log(() -> {
        try {
          log.info("Computing cached solver");
          // Not as much hurry if one already exists
          boolean lowPriority = solver.get() != null;
          Solver newYTYSolver = LinearSystemSolver.getSolver(vectorPartitions.getVTV(lowPriority));
          if (newYTYSolver != null) {
            log.info("Computed new solver {}", solver);
            solver.set(newYTYSolver);
          }
        } finally {
          // Allow any threads waiting for initial model to proceed.
          // It's possible the solver is still null here if there is no input.
          solverInitialized.countDown();
          solverUpdating.set(false);
        }
      }).asRunnable());
    }
  }

  /**
   * @param blocking whether to block waiting for a first model
   * @return a recent {@link Solver}, optionally blocking if necessary to wait for an initial one to
   *  be computed. It does not block otherwise and returns the most recently computed one.
   *  Note that this method may return {@code null} even when blocking , for instance,
   *  if no solver is computable because there is no data.
   */
  public Solver get(boolean blocking) {
    if (solverDirty.getAndSet(false)) {
      // launch asynchronous update
      compute();
    }
    // Wait, in the case that there is no existing model already.
    // Otherwise this immediately proceeds.
    if (blocking && solverInitialized.getCount() > 0) { // Always 0 or 1
      // OK if countDown() happens here; await() will return immediately
      try {
        solverInitialized.await();
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for model", e);
      }
    }
    return solver.get();
  }

}
