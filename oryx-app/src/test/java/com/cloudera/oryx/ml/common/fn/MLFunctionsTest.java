/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.ml.common.fn;

import java.util.Arrays;

import org.junit.Test;
import scala.Tuple2;

import com.cloudera.oryx.common.OryxTest;

public final class MLFunctionsTest extends OryxTest {

  @Test
  public void testParseJSON() throws Exception {
    assertArrayEquals(new String[] {"a", "1", "foo"},
                      MLFunctions.PARSE_FN.call("[\"a\",\"1\",\"foo\"]"));
    assertArrayEquals(new String[] {"a", "1", "foo", ""},
                      MLFunctions.PARSE_FN.call("[\"a\",\"1\",\"foo\",\"\"]"));
    assertArrayEquals(new String[] {"2.3"},
                      MLFunctions.PARSE_FN.call("[\"2.3\"]"));
    assertArrayEquals(new String[] {},
                      MLFunctions.PARSE_FN.call("[]"));
  }

  @Test
  public void testParseCSV() throws Exception {
    assertArrayEquals(new String[] {"a", "1", "foo"},
                      MLFunctions.PARSE_FN.call("a,1,foo"));
    assertArrayEquals(new String[] {"a", "1", "foo", ""},
                      MLFunctions.PARSE_FN.call("a,1,foo,"));
    assertArrayEquals(new String[] {"2.3"},
                      MLFunctions.PARSE_FN.call("2.3"));
    // Different from JSON, sort of:
    assertArrayEquals(new String[] {""},
                      MLFunctions.PARSE_FN.call(""));
  }

  @Test
  public void testToTimestamp() throws Exception {
    assertEquals(123L, MLFunctions.TO_TIMESTAMP_FN.call("a,b,c,123").longValue());
    assertEquals(123L, MLFunctions.TO_TIMESTAMP_FN.call("a,b,c,123,").longValue());
    assertEquals(123L, MLFunctions.TO_TIMESTAMP_FN.call("[\"a\",\"b\",\"c\",123]").longValue());
    assertEquals(123L,
                 MLFunctions.TO_TIMESTAMP_FN.call("[\"a\",\"b\",\"c\",123,\"d\"]").longValue());
  }

  @Test
  public void testNotNaN() throws Exception {
    assertTrue(MLFunctions.<String>notNaNValue().call(new Tuple2<>("foo", 0.0)));
    assertFalse(MLFunctions.<String>notNaNValue().call(new Tuple2<>("foo", Double.NaN)));
  }

  @Test
  public void testSumWithNaN() throws Exception {
    assertEquals(1.0, MLFunctions.SUM_WITH_NAN.call(Arrays.asList(1.0)).doubleValue());
    assertEquals(6.0, MLFunctions.SUM_WITH_NAN.call(Arrays.asList(1.0, 2.0, 3.0)).doubleValue());
    assertEquals(3.0,
                 MLFunctions.SUM_WITH_NAN.call(Arrays.asList(1.0, Double.NaN, 3.0)).doubleValue());
    assertTrue(Double.isNaN(MLFunctions.SUM_WITH_NAN.call(Arrays.asList(1.0, 2.0, Double.NaN))));
    assertTrue(Double.isNaN(MLFunctions.SUM_WITH_NAN.call(Arrays.asList(Double.NaN))));
  }

}
