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

package com.cloudera.oryx.app.als;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class MultiRescorerTest extends OryxTest {

  @Test
  public void testOf() {
    MultiRescorer multi = (MultiRescorer) MultiRescorer.of(new SimpleModRescorer(1), new SimpleModRescorer(2));
    assertEquals(2, multi.getRescorers().length);
    MultiRescorer multi2 = (MultiRescorer) MultiRescorer.of(multi, new SimpleModRescorer(3));
    assertEquals(3, multi2.getRescorers().length);
  }

}
