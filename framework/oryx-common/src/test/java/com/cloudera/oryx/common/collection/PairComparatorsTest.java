/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.common.collection;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class PairComparatorsTest extends OryxTest {

  @Test
  public void testByFirst() {
    List<Pair<Integer,String>> pairs = Arrays.asList(
        new Pair<>(3, "foo"),
        new Pair<>(4, "bing"),
        new Pair<>(1, "baz"),
        new Pair<>(2, "whizz")
    );
    Collections.sort(pairs, PairComparators.<Integer>byFirst());
    assertEquals(1, pairs.get(0).getFirst().intValue());
    assertEquals(2, pairs.get(1).getFirst().intValue());
    assertEquals("baz", pairs.get(0).getSecond());
    assertEquals("whizz", pairs.get(1).getSecond());
  }

  @Test
  public void testBySecond() {
    List<Pair<Integer,String>> pairs = Arrays.asList(
        new Pair<>(3, "foo"),
        new Pair<>(4, "bing"),
        new Pair<>(1, "baz"),
        new Pair<>(2, "whizz")
    );
    Collections.sort(pairs, PairComparators.<String>bySecond());
    assertEquals(1, pairs.get(0).getFirst().intValue());
    assertEquals(4, pairs.get(1).getFirst().intValue());
    assertEquals("baz", pairs.get(0).getSecond());
    assertEquals("bing", pairs.get(1).getSecond());
  }

}
