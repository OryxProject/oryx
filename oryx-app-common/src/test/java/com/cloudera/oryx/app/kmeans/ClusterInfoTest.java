/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.kmeans;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class ClusterInfoTest extends OryxTest {

  @Test
  public void testUpdate() {
    ClusterInfo info = new ClusterInfo(0, new double[] { -1.0, 2.0 }, 2);
    assertEquals(0, info.getID());
    assertArrayEquals(new double[]{-1.0, 2.0}, info.getCenter());
    assertEquals(2, info.getCount());
    assertTrue("0 [-1.0, 2.0] 2".equals(info.toString()));

    info.update(new double[] { -1.0, -1.0 });
    assertEquals(0, info.getID());
    assertArrayEquals(new double[] { -1.0, 1.0 }, info.getCenter());
    assertEquals(3, info.getCount());
    assertTrue("0 [-1.0, 1.0] 3".equals(info.toString()));

    info.update(new double[] { 0.0, 0.0 }, 3);
    assertEquals(0, info.getID());
    assertArrayEquals(new double[] { -0.5, 0.5 }, info.getCenter());
    assertEquals(6, info.getCount());
    assertTrue("0 [-0.5, 0.5] 6".equals(info.toString()));
  }

}
