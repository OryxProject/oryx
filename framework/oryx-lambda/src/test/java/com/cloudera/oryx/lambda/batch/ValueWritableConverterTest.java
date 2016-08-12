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

package com.cloudera.oryx.lambda.batch;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class ValueWritableConverterTest extends OryxTest {

  @Test
  public void testText() {
    ValueWritableConverter<String> converter =
        new ValueWritableConverter<>(String.class, Text.class);
    assertEquals("foo", converter.fromWritable(new Text("foo")));
    assertEquals(new Text("bar"), converter.toWritable("bar"));
  }

  @Test
  public void testLong() {
    ValueWritableConverter<Long> converter =
        new ValueWritableConverter<>(Long.class, LongWritable.class);
    assertEquals(-1L, converter.fromWritable(new LongWritable(-1L)).longValue());
    assertEquals(new LongWritable(1L), converter.toWritable(1L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoSuchMethod() {
    new ValueWritableConverter<>(byte[].class, ArrayPrimitiveWritable.class);
  }

  @Test(expected = NullPointerException.class)
  public void testFromNull() {
    new ValueWritableConverter<>(String.class, Text.class).fromWritable(null);
  }

  @Test
  public void testToNullText() {
    assertEquals(new Text(), new ValueWritableConverter<>(String.class, Text.class).toWritable(null));
  }

}
