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

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class PairTest extends OryxTest {

  @Test
  public void testEquals() {
    assertEquals(new Pair<>(3.0, "foo"), new Pair<>(3.0, "foo"));
    assertEquals(new Pair<>(null, null), new Pair<>(null, null));
    assertNotEquals(new Pair<>(3.0, "foo"), new Pair<>(4.0, "foo"));
    assertNotEquals(new Pair<>(3.0, "foo"), new Pair<>("foo", 3.0));
  }

  @Test
  public void testHashCode() {
    assertEquals(new Pair<>(3.0, "foo").hashCode(), new Pair<>(3.0, "foo").hashCode());
    assertEquals(new Pair<>(null, null).hashCode(), new Pair<>(null, null).hashCode());
  }

}
