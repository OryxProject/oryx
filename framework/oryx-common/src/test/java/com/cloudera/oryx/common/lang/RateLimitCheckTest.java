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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class RateLimitCheckTest extends OryxTest {

  @Test
  public void testLimit() throws Exception {
    RateLimitCheck check = new RateLimitCheck(100, TimeUnit.MILLISECONDS);
    assertTrue(check.test());
    assertFalse(check.test());
    Thread.sleep(200);
    assertTrue(check.test());
    assertFalse(check.test());
  }

}
