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

import net.openhft.koloboke.collect.map.ObjObjMap;
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;
import net.openhft.koloboke.function.Predicate;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class KeyOnlyBiPredicateTest extends OryxTest {

  @Test
  public void testKeyOnly() {
    ObjObjMap<String,String> map = HashObjObjMaps.newMutableMap(
        new String[]{"foo", "bar", "baz"},
        new String[]{"1", "3", "4"}
    );
    map.removeIf(new KeyOnlyBiPredicate<String, String>(new Predicate<String>() {
      @Override
      public boolean test(String s) {
        return s.startsWith("b");
      }
    }));
    assertEquals(1, map.size());
    assertEquals("1", map.get("foo"));
  }

}
