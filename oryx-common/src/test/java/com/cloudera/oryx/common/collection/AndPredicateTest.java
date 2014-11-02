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

package com.cloudera.oryx.common.collection;

import java.util.Arrays;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class AndPredicateTest extends OryxTest {

  @Test
  public void testAnd() {
    NotContainsPredicate<String> a = new NotContainsPredicate<>(Arrays.asList("foo"));
    NotContainsPredicate<String> b = new NotContainsPredicate<>(Arrays.asList("bar", "baz"));
    @SuppressWarnings("unchecked")
    AndPredicate<String> and = new AndPredicate<>(a, b);
    assertFalse(and.apply("foo"));
    assertFalse(and.apply("bar"));
    assertFalse(and.apply("baz"));
    assertTrue(and.apply("bing"));
  }

}
