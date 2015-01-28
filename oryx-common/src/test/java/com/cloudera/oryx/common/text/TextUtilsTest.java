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

package com.cloudera.oryx.common.text;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

/**
 * Tests {@link TextUtils}.
 */
public final class TextUtilsTest extends OryxTest {

  @Test
  public void testParseJSON() throws Exception {
    assertArrayEquals(new String[] {"a", "1", "foo"},
                      TextUtils.parseJSONArray("[\"a\",\"1\",\"foo\"]"));
    assertArrayEquals(new String[] {"a", "1", "foo", ""},
                      TextUtils.parseJSONArray("[\"a\",\"1\",\"foo\",\"\"]"));
    assertArrayEquals(new String[] {"2.3"}, TextUtils.parseJSONArray("[\"2.3\"]"));
    assertArrayEquals(new String[] {}, TextUtils.parseJSONArray("[]"));
  }

  @Test
  public void testParseCSV() throws Exception {
    assertArrayEquals(new String[] {"a", "1", "foo"}, TextUtils.parseCSV("a,1,foo"));
    assertArrayEquals(new String[] {"a", "1", "foo", ""}, TextUtils.parseCSV("a,1,foo,"));
    assertArrayEquals(new String[] {"2.3"}, TextUtils.parseCSV("2.3"));
    // Different from JSON, sort of:
    assertArrayEquals(new String[] {""}, TextUtils.parseCSV(""));
  }

  @Test
  public void testParseDelimited() throws Exception {
    assertArrayEquals(new String[] {"a", "1,", ",foo"},
                      TextUtils.parseDelimited("a\t1,\t,foo", '\t'));
    assertArrayEquals(new String[] {"a", "1", "foo", ""},
                      TextUtils.parseDelimited("a 1 foo ", ' '));
  }

  @Test
  public void testJoinCSV() {
    assertEquals("1,2,3", TextUtils.joinCSV(Arrays.asList("1", "2", "3")));
    assertEquals("\"a,b\"", TextUtils.joinCSV(Arrays.asList("a,b")));
    assertEquals("\"\"\"a\"\"\"", TextUtils.joinCSV(Arrays.asList("\"a\"")));
  }

  @Test
  public void testJoinDelimited() {
    assertEquals("1 2 3", TextUtils.joinDelimited(Arrays.asList("1", "2", "3"), ' '));
    assertEquals("\"1 \" \"2 \" 3", TextUtils.joinDelimited(Arrays.asList("1 ", "2 ", "3"), ' '));
    assertEquals("", TextUtils.joinDelimited(Collections.emptyList(), '\t'));
  }

  @Test
  public void testJoinDelimitedNoQuote() {
    assertEquals("-1.0 2.01 3.5",
                 TextUtils.joinDelimited(Arrays.asList(-1.0, 2.01, 3.5), ' ', true));
  }

  @Test
  public void testJoinJSON() {
    assertEquals("[\"1\",\"2\",\"3\"]", TextUtils.joinJSON(Arrays.asList("1", "2", "3")));
    assertEquals("[\"1 \",\"2 \",\"3\"]", TextUtils.joinJSON(Arrays.asList("1 ", "2 ", "3")));
    assertEquals("[]", TextUtils.joinJSON(Collections.emptyList()));
  }

  @Test
  public void testJSONList() {
    List<Object> list = new ArrayList<>();
    list.add("foo");
    list.add(2);
    assertEquals("[\"A\",[\"foo\",2],\"B\"]", TextUtils.joinJSON(Arrays.asList("A", list, "B")));
  }

  @Test
  public void testJSONMap() {
    Map<Object,Object> map = new HashMap<>();
    map.put(1, "bar");
    map.put("foo", 2);
    assertEquals("[\"A\",{\"1\":\"bar\",\"foo\":2},\"B\"]",
                 TextUtils.joinJSON(Arrays.asList("A", map, "B")));
  }

}
