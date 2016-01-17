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

package com.cloudera.oryx.common.collection;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link Pairs}.
 */
public final class PairsTest extends OryxTest {

  @Test
  public void testOrderBy() {
    Pair<Double,String> a = new Pair<>(1.0, "foo");
    Pair<Double,String> b = new Pair<>(2.0, "bar");

    assertEquals(0, Pairs.<Double,String>orderByFirst(Pairs.SortOrder.ASCENDING).compare(a, a));
    assertEquals(0, Pairs.<Double,String>orderByFirst(Pairs.SortOrder.DESCENDING).compare(a, a));
    assertEquals(0, Pairs.<Double,String>orderBySecond(Pairs.SortOrder.ASCENDING).compare(a, a));
    assertEquals(0, Pairs.<Double,String>orderBySecond(Pairs.SortOrder.DESCENDING).compare(a, a));

    assertTrue(Pairs.<Double,String>orderByFirst(Pairs.SortOrder.ASCENDING).compare(a, b) < 0);
    assertTrue(Pairs.<Double,String>orderByFirst(Pairs.SortOrder.DESCENDING).compare(a, b) > 0);
    assertTrue(Pairs.<Double,String>orderBySecond(Pairs.SortOrder.ASCENDING).compare(a, b) > 0);
    assertTrue(Pairs.<Double,String>orderBySecond(Pairs.SortOrder.DESCENDING).compare(a, b) < 0);
  }

}
