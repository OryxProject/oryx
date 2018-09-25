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

package com.cloudera.oryx.app.rdf.tree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class TreePathTest extends OryxTest {

  private static final TreePath LRL = TreePath.EMPTY.extendLeft().extendRight().extendLeft();
  private static final TreePath LRR = TreePath.EMPTY.extendLeft().extendRight().extendRight();
  private static final TreePath LR = TreePath.EMPTY.extendLeft().extendRight();
  private static final TreePath R = TreePath.EMPTY.extendRight();

  @Test
  public void testEquals() {
    assertEquals(LRL, TreePath.EMPTY.extendLeft().extendRight().extendLeft());
    assertNotEquals(TreePath.EMPTY, LRL);
    Collection<TreePath> paths = new HashSet<>();
    paths.add(LRL);
    paths.add(LRR);
    paths.add(LRR);
    assertEquals(2, paths.size());
  }

  @Test
  public void testToString() {
    assertEquals("", TreePath.EMPTY.toString());
    assertEquals("010", LRL.toString());
    assertEquals("011", LRR.toString());
  }

  @Test
  public void testOrder() {
    List<TreePath> paths = new ArrayList<>(Arrays.asList(LRL, R, LR, LRR));
    Collections.sort(paths);
    assertEquals(Arrays.asList(LRL, LR, LRR, R), paths);
  }

}
