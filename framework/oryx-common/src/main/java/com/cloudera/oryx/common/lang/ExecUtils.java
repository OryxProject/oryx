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

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;

/**
 * Utility methods for executing tasks in parallel, possibly returning a result
 * and possibly using a private pool, using Java 8 abstractions.
 */
public final class ExecUtils {

  private ExecUtils() {}

  /**
   * @param numTasks how many copies of {@code task} to run; all may be run simultaneously,
   *  and a shared thread pool is used
   * @param task task to execute; takes an {@code Integer} argument that is the index of
   *  the task that has been executed in [0, numTasks)
   */
  public static void doInParallel(int numTasks, Consumer<Integer> task) {
    doInParallel(numTasks, numTasks, false, task);
  }

  /**
   *
   * @param numTasks how many copies of {@code task} to run
   * @param parallelism maximum how many tasks to run simultaneously
   * @param privatePool whether to create and use a new private thread pool; otherwise
   *  a shared pool is used. No parallelism or pool is used if {@code parallelism} is 1
   * @param task task to execute; takes an {@code Integer} argument that is the index of
   *  the task that has been executed in [0, numTasks)
   */
  public static void doInParallel(int numTasks,
                                  int parallelism,
                                  boolean privatePool,
                                  Consumer<Integer> task) {
    Preconditions.checkArgument(numTasks >= 1);
    Preconditions.checkArgument(parallelism >= 1);
    Objects.requireNonNull(task);
    IntStream range = IntStream.range(0, numTasks);
    IntStream taskIndices = parallelism > 1 ? range.parallel() : range;
    if (parallelism > 1 && privatePool) {
      ForkJoinPool pool = new ForkJoinPool(parallelism);
      try {
        pool.submit(() -> taskIndices.forEach(task::accept)).get();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause());
      } finally {
        pool.shutdown();
      }
    } else {
      taskIndices.forEach(task::accept);
    }
  }

  /**
   *
   * @param numTasks how many copies of {@code task} to run
   * @param parallelism maximum how many tasks to run simultaneously
   * @param privatePool whether to create and use a new private thread pool; otherwise
   *  a shared pool is used. No parallelism or pool is used if {@code parallelism} is 1
   * @param task task to execute; takes an {@code Integer} argument that is the index of
   *  the task that has been executed in [0, numTasks) and returns some value
   * @param collector instance used to collect results into the return value
   * @param <T> type produced by each task
   * @param <R> type produced by {@code collector} from task results
   * @return result of collecting individual task results
   */
  public static <T,R> R collectInParallel(int numTasks,
                                          int parallelism,
                                          boolean privatePool,
                                          Function<Integer,T> task,
                                          Collector<T,?,R> collector) {
    Preconditions.checkArgument(numTasks >= 1);
    Preconditions.checkArgument(parallelism >= 1);
    Objects.requireNonNull(task);
    Objects.requireNonNull(collector);
    IntStream range = IntStream.range(0, numTasks);
    IntStream taskIndices = parallelism > 1 ? range.parallel() : range;
    if (parallelism > 1 && privatePool) {
      ForkJoinPool pool = new ForkJoinPool(parallelism);
      try {
        return pool.submit(() -> taskIndices.mapToObj(task::apply).collect(collector)).get();
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(e.getCause());
      } finally {
        pool.shutdown();
      }
    } else {
      return taskIndices.mapToObj(task::apply).collect(collector);
    }
  }

}
