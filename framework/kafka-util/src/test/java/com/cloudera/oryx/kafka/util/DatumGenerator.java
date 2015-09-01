/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.kafka.util;

import org.apache.commons.math3.random.RandomGenerator;

import com.cloudera.oryx.common.collection.Pair;

/**
 * Interface which generates one datum.
 *
 * @param <K> key type
 * @param <M> message type
 */
@FunctionalInterface
public interface DatumGenerator<K,M> {

  /**
   * @param id a unique identifier for datum being generated
   * @param random random number generator to use if randomness is needed
   * @return a datum, which may or may not use {@code id} or {@code random}
   */
  Pair<K,M> generate(int id, RandomGenerator random);

}
