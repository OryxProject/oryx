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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.ToIntBiFunction;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.koloboke.collect.map.ObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;

import com.cloudera.oryx.common.lang.AutoLock;
import com.cloudera.oryx.common.lang.AutoReadWriteLock;
import com.cloudera.oryx.common.lang.LoggingCallable;

/**
 * Encapsulates a feature vectors, keyed by an ID, and associated data structures.
 * This supports the representation of user-feature and item-feature vectors in {@code ALSServingModel}
 * or {@code ALSSpeedModel}. This represents many partition of vectors; see {@link FeatureVectorsPartition}
 * for a representation of one partition.
 */
public final class PartitionedFeatureVectors implements FeatureVectors {

  private final FeatureVectorsPartition[] partitions;
  private final ToIntBiFunction<String,float[]> partitioner;
  /** Maps item IDs to their existing partition, if any */
  private final ObjIntMap<String> partitionMap;
  /** Controls access to yPartitionMap. */
  private final AutoReadWriteLock partitionMapLock;
  private final ExecutorService executor;

  public PartitionedFeatureVectors(int numPartitions,
                                   ExecutorService executor) {
    this(numPartitions, executor,
         (String id, float[] vec) -> (id.hashCode() & 0x7FFFFFFF) % numPartitions);
  }

  public PartitionedFeatureVectors(int numPartitions,
                                   ExecutorService executor,
                                   ToIntBiFunction<String,float[]> partitioner) {
    Preconditions.checkArgument(numPartitions > 0);
    Objects.requireNonNull(executor);
    Objects.requireNonNull(partitioner);
    partitions = new FeatureVectorsPartition[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      partitions[i] = new FeatureVectorsPartition();
    }
    partitionMap = HashObjIntMaps.newMutableMap();
    partitionMapLock = new AutoReadWriteLock();
    this.partitioner = partitioner;
    this.executor = executor;
  }

  @Override
  public int size() {
    int total = 0;
    for (FeatureVectors partition : partitions) {
      total += partition.size();
    }
    return total;
  }

  public <T> Stream<T> mapPartitionsParallel(Function<FeatureVectorsPartition,Stream<T>> fn,
                                             boolean background) {
    return mapPartitionsParallel(fn, null, background);
  }

  public <T> Stream<T> mapPartitionsParallel(Function<FeatureVectorsPartition,Stream<T>> fn,
                                             int[] candidateIndices,
                                             boolean background) {

    List<Callable<Stream<T>>> tasks;
    if (candidateIndices == null) {
      tasks = new ArrayList<>(partitions.length);
      for (FeatureVectorsPartition partition : partitions) {
        if (partition.size() > 0) {
          tasks.add(LoggingCallable.log(() -> fn.apply(partition)));
        }
      }
    } else {
      tasks = new ArrayList<>(candidateIndices.length);
      for (int i : candidateIndices) {
        if (partitions[i].size() > 0) {
          tasks.add(LoggingCallable.log(() -> fn.apply(partitions[i])));
        }
      }
    }

    int numTasks = tasks.size();
    if (numTasks == 0) {
      return Stream.empty();
    }

    Stream<T> stream;
    if (numTasks == 1) {
      try {
        stream = tasks.get(0).call();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    } else if (background) {
      // Run locally
      stream = tasks.stream().map(task -> {
        try {
          return task.call();
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }).reduce(Stream::concat).orElse(null);
    } else {
      try {
        stream = executor.invokeAll(tasks).stream().map(future -> {
          try {
            return future.get();
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
          }
        }).reduce(Stream::concat).orElse(null);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    return stream;
  }

  @Override
  public float[] getVector(String item) {
    // Partitioner isn't used here to compute partition, because for at least one use case
    // the partitioner might change its answer over time.
    int partition;
    try (AutoLock al = partitionMapLock.autoReadLock()) {
      partition = partitionMap.getOrDefault(item, Integer.MIN_VALUE);
    }
    if (partition < 0) {
      return null;
    }
    return partitions[partition].getVector(item);
  }

  @Override
  public void setVector(String item, float[] vector) {
    int newPartition = partitioner.applyAsInt(item, vector);
    // Exclusive update to mapping -- careful since other locks are acquired inside here
    try (AutoLock al = partitionMapLock.autoWriteLock()) {
      int existingPartition = partitionMap.getOrDefault(item, Integer.MIN_VALUE);
      if (existingPartition >= 0 && existingPartition != newPartition) {
        // Move from one to the other partition, so first remove old entry
        partitions[existingPartition].removeVector(item);
        // Note that it's conceivable that a recommendation call sees *no* copy of this
        // item here in this brief window
      }
      // Then regardless put in new partition
      partitions[newPartition].setVector(item, vector);
      partitionMap.put(item, newPartition);
    }
  }

  @Override
  public void addAllIDsTo(Collection<String> allIDs) {
    for (FeatureVectors partition : partitions) {
      partition.addAllIDsTo(allIDs);
    }
  }

  @Override
  public void removeAllIDsFrom(Collection<String> allIDs) {
    for (FeatureVectors partition : partitions) {
      partition.removeAllIDsFrom(allIDs);
    }
  }

  @Override
  public void addAllRecentTo(Collection<String> allRecent) {
    for (FeatureVectors partition : partitions) {
      partition.addAllRecentTo(allRecent);
    }
  }

  @Override
  public void retainRecentAndIDs(Collection<String> newModelIDs) {
    for (FeatureVectors partition : partitions) {
      partition.retainRecentAndIDs(newModelIDs);
    }
  }

  @Override
  public double[] getVTV(boolean background) {
    return mapPartitionsParallel(partition -> Stream.of(partition.getVTV(background)), background)
        .reduce((a, b) -> {
          for (int i = 0; i < a.length; i++) {
            a[i] += b[i];
          }
          return a;
        })
        .orElse(null);
  }

  @Override
  public String toString() {
    int maxSize = 16;
    List<String> partitionSizes = new ArrayList<>(maxSize);
    for (int i = 0; i < partitions.length; i++) {
      int size = partitions[i].size();
      if (size > 0) {
        partitionSizes.add(i + ":" + size);
        if (partitionSizes.size() == maxSize) {
          partitionSizes.add("...");
          break;
        }
      }
    }
    return partitionSizes.toString();
  }

}
