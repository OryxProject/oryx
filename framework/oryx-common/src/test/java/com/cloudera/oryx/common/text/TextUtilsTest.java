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
import java.util.LinkedHashMap;
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
  public void testParseDelimited() throws Exception {
    assertArrayEquals(new String[] {"a", "1", "foo"}, TextUtils.parseDelimited("a,1,foo", ','));
    assertArrayEquals(new String[] {"a", "1", "foo", ""},
                      TextUtils.parseDelimited("a,1,foo,", ','));
    assertArrayEquals(new String[] {"2.3"}, TextUtils.parseDelimited("2.3", ','));
    assertArrayEquals(new String[] {"\"a\""}, TextUtils.parseDelimited("\"\"\"a\"\"\"", ','));
    assertArrayEquals(new String[] {"\"", "\"\""},
                      TextUtils.parseDelimited("\"\"\"\" \"\"\"\"\"\"", ' '));
    // Different from JSON, sort of:
    assertArrayEquals(new String[] {""}, TextUtils.parseDelimited("", ','));
    assertArrayEquals(new String[] {"a", "1,", ",foo"},
                      TextUtils.parseDelimited("a\t1,\t,foo", '\t'));
    assertArrayEquals(new String[] {"a", "1", "foo", ""},
                      TextUtils.parseDelimited("a 1 foo ", ' '));
    assertArrayEquals(new String[] {"-1.0", "a\" \"b"},
                      TextUtils.parseDelimited("-1.0 a\"\\ \"b", ' '));
    assertArrayEquals(new String[] {"-1.0", "a\"b\"c"},
                      TextUtils.parseDelimited("-1.0 \"a\\\"b\\\"c\"", ' '));

  }

  @Test
  public void testParsePMMLDelimited() {
    assertArrayEquals(new String[] {"1", "22", "3"}, TextUtils.parsePMMLDelimited("1 22 3"));
    assertArrayEquals(new String[] {"ab", "a b", "with \"quotes\" "},
                      TextUtils.parsePMMLDelimited("ab  \"a b\"   \"with \\\"quotes\\\" \" "));
    assertArrayEquals(new String[] {"\" \""},
                      TextUtils.parsePMMLDelimited("\"\\\" \\\"\""));
    assertArrayEquals(new String[] {" c\" d \"e ", " c\" d \"e "},
                      TextUtils.parsePMMLDelimited(" \" c\\\" d \\\"e \" \" c\\\" d \\\"e \" "));
  }

  @Test
  public void testJoinDelimited() {
    assertEquals("1,2,3", TextUtils.joinDelimited(Arrays.asList("1", "2", "3"), ','));
    assertEquals("\"a,b\"", TextUtils.joinDelimited(Arrays.asList("a,b"), ','));
    assertEquals("\"\"\"a\"\"\"", TextUtils.joinDelimited(Arrays.asList("\"a\""), ','));
    assertEquals("1 2 3", TextUtils.joinDelimited(Arrays.asList("1", "2", "3"), ' '));
    assertEquals("\"1 \" \"2 \" 3", TextUtils.joinDelimited(Arrays.asList("1 ", "2 ", "3"), ' '));
    assertEquals("\"\"\"a\"\"\"", TextUtils.joinDelimited(Arrays.asList("\"a\""), ' '));
    assertEquals("\"\"\"\" \"\"\"\"\"\"",
                 TextUtils.joinDelimited(Arrays.asList("\"", "\"\""), ' '));
    assertEquals("", TextUtils.joinDelimited(Collections.emptyList(), '\t'));
  }

  @Test
  public void testJoinPMMLDelimited() {
    assertEquals("ab \"a b\" \"with \\\"quotes\\\" \"",
                 TextUtils.joinPMMLDelimited(Arrays.asList("ab", "a b", "with \"quotes\" ")));
    assertEquals("1 22 3",
                 TextUtils.joinPMMLDelimited(Arrays.asList("1", "22", "3")));
    assertEquals("\" c\\\" d \\\"e \" \" c\\\" d \\\"e \"",
                 TextUtils.joinPMMLDelimited(Arrays.asList(" c\" d \"e ", " c\" d \"e ")));
  }

  @Test
  public void testJoinPMMLDelimitedNumbers() {
    assertEquals("-1.0 2.01 3.5",
                 TextUtils.joinPMMLDelimitedNumbers(Arrays.asList(-1.0, 2.01, 3.5)));
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
    Map<Object,Object> map = new LinkedHashMap<>();
    map.put(1, "bar");
    map.put("foo", 2);
    assertEquals("[\"A\",{\"1\":\"bar\",\"foo\":2},\"B\"]",
                 TextUtils.joinJSON(Arrays.asList("A", map, "B")));
  }

  @Test
  public void testReadJSON() {
    assertEquals(3, TextUtils.readJSON("3", Integer.class).intValue());
    assertEquals(Arrays.asList("foo", "bar"), TextUtils.readJSON("[\"foo\", \"bar\"]", List.class));
    assertArrayEquals(new float[] { 1.0f, 2.0f }, TextUtils.readJSON("[1,2]", float[].class));
  }

  @Test
  public void testConvertViaJSON() {
    assertEquals(3, TextUtils.convertViaJSON("3", Long.class).longValue());
    assertArrayEquals(new float[] { 1.0f, 2.0f },
                      TextUtils.convertViaJSON(new double[] { 1.0, 2.0 }, float[].class));
  }

}
