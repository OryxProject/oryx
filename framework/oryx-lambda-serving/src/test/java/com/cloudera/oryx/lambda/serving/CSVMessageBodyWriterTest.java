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

package com.cloudera.oryx.lambda.serving;

import javax.ws.rs.core.MediaType;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import com.cloudera.oryx.api.serving.HasCSV;
import com.cloudera.oryx.common.OryxTest;

public final class CSVMessageBodyWriterTest extends OryxTest {

  private static final MediaType TEXT_CSV_TYPE = new MediaType("text", "csv");

  @Test
  public void testIsWriteable() {
    CSVMessageBodyWriter writer = new CSVMessageBodyWriter();
    assertTrue(writer.isWriteable(null, null, null, MediaType.TEXT_PLAIN_TYPE));
    assertTrue(writer.isWriteable(null, null, null, TEXT_CSV_TYPE));
    assertFalse(writer.isWriteable(null, null, null, MediaType.TEXT_HTML_TYPE));
  }

  @Test
  public void testGetSize() {
    CSVMessageBodyWriter writer = new CSVMessageBodyWriter();
    assertEquals(-1, writer.getSize(null, null, null, null, null));
  }

  @Test
  public void testWrite() throws Exception {
    CSVMessageBodyWriter writer = new CSVMessageBodyWriter();

    HasCSV foo = () -> "foo";
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.writeTo(foo, HasCSV.class, null, null, TEXT_CSV_TYPE, null, out);
    out.close();
    assertEquals("foo\n", new String(out.toByteArray(), StandardCharsets.UTF_8));

    HasCSV bar = () -> "bar";
    ByteArrayOutputStream multiOut = new ByteArrayOutputStream();
    writer.writeTo(Arrays.asList(foo, bar), Iterable.class, null, null, TEXT_CSV_TYPE, null, multiOut);
    multiOut.close();
    assertEquals("foo\nbar\n", new String(multiOut.toByteArray(), StandardCharsets.UTF_8));
  }

}
