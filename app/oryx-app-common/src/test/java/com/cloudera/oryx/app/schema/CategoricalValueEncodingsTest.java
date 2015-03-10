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

package com.cloudera.oryx.app.schema;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class CategoricalValueEncodingsTest extends OryxTest {

  private static CategoricalValueEncodings makeTestValues() {
    Map<Integer,Collection<String>> distinctValues = new HashMap<>();
    distinctValues.put(0, Arrays.asList("foo", "bar", "baz"));
    distinctValues.put(2, Arrays.asList("3", "2", "1", "0"));
    distinctValues.put(3, Arrays.asList("one"));
    return new CategoricalValueEncodings(distinctValues);
  }

  @Test
  public void testCounts() {
    CategoricalValueEncodings encodings = makeTestValues();
    assertEquals(3, encodings.getValueCount(0));
    assertEquals(4, encodings.getValueCount(2));
    assertEquals(1, encodings.getValueCount(3));
    Map<Integer,Integer> counts = encodings.getCategoryCounts();
    assertEquals(3, counts.get(0).intValue());
    assertEquals(4, counts.get(2).intValue());
    assertEquals(1, counts.get(3).intValue());
  }

  @Test
  public void testEncoding() {
    CategoricalValueEncodings encodings = makeTestValues();
    Map<String,Integer> valueEncoding = encodings.getValueEncodingMap(0);
    assertEquals(0, valueEncoding.get("foo").intValue());
    assertEquals(1, valueEncoding.get("bar").intValue());
    assertEquals(2, valueEncoding.get("baz").intValue());
  }

  @Test
  public void testValue() {
    CategoricalValueEncodings encodings = makeTestValues();
    Map<Integer,String> valueEncoding = encodings.getEncodingValueMap(2);
    assertEquals("3", valueEncoding.get(0));
    assertEquals("2", valueEncoding.get(1));
    assertEquals("1", valueEncoding.get(2));
    assertEquals("0", valueEncoding.get(3));
  }

}
