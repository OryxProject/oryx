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

package com.cloudera.oryx.app.serving.als.model;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;

import com.koloboke.function.ObjDoubleToDoubleFunction;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.collection.Pairs;

final class TopNConsumer implements BiConsumer<String,float[]> {

  private static final Predicate<String> ALWAYS_ALLOWED = key -> true;
  private static final ObjDoubleToDoubleFunction<String> NO_RESCORE = (key, value) -> value;

  private final Queue<Pair<String, Double>> topN;
  private final int howMany;
  private final ToDoubleFunction<float[]> scoreFn;
  private final ObjDoubleToDoubleFunction<String> rescoreFn;
  private final Predicate<String> allowedPredicate;
  /** Local copy of lower bound of min score in the priority queue, to avoid polling. */
  private double topScoreLowerBound;

  TopNConsumer(int howMany,
               ToDoubleFunction<float[]> scoreFn,
               ObjDoubleToDoubleFunction<String> rescoreFn,
               Predicate<String> allowedPredicate) {
    this.topN = new PriorityQueue<>(howMany, Pairs.orderBySecond(Pairs.SortOrder.ASCENDING));
    this.howMany = howMany;
    this.scoreFn = scoreFn;
    this.rescoreFn = rescoreFn == null ? NO_RESCORE : rescoreFn;
    this.allowedPredicate = allowedPredicate == null ? ALWAYS_ALLOWED : allowedPredicate;
    topScoreLowerBound = Double.NEGATIVE_INFINITY;
  }

  @Override
  public void accept(String key, float[] value) {
    if (allowedPredicate.test(key)) {
      double score = rescoreFn.applyAsDouble(key, scoreFn.applyAsDouble(value));
      // Only proceed if score can possibly exceed (cached) minimum score in the queue.
      if (score > topScoreLowerBound) {
        // If full,
        if (topN.size() >= howMany) {
          // Must double-check against next value because new one may still not be bigger
          if (score > (topScoreLowerBound = topN.peek().getSecond())) {
            // Swap in new, larger value for old smallest one
            topN.poll();
            topN.add(new Pair<>(key, score));
          }
        } else {
          topN.add(new Pair<>(key, score));
        }
      }
    }
  }

  Stream<Pair<String,Double>> getTopN() {
    return topN.stream();
  }

}
