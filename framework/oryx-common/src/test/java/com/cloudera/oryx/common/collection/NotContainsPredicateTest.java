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
import java.util.Collection;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class NotContainsPredicateTest extends OryxTest {

  @Test
  public void testPredicate() {
    Collection<Integer> contains = Arrays.asList(1, 3, 5);
    NotContainsPredicate<Integer> predicate = new NotContainsPredicate<>(contains);
    assertTrue(predicate.test(2));
    assertFalse(predicate.test(5));
  }

}
