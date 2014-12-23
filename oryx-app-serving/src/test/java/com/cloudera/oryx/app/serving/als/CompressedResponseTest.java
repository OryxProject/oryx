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

package com.cloudera.oryx.app.serving.als;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import com.google.common.io.CharStreams;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.lang.ClassUtils;

public final class CompressedResponseTest extends AbstractALSServingTest {

  @Test
  public void testGzipResponse() throws Exception {
    doTestCompressedResponse("gzip", GZIPInputStream.class);
  }

  @Test
  public void testDeflateResponse() throws Exception {
    doTestCompressedResponse("deflate", InflaterInputStream.class);
  }

  private void doTestCompressedResponse(
      String encoding, Class<? extends InputStream> decompressingStreamClass) throws IOException {
    byte[] bytes = target("/item/allIDs").request().acceptEncoding(encoding).get(byte[].class);
    String content;
    try (Reader reader = new InputStreamReader(
             ClassUtils.loadInstanceOf(decompressingStreamClass.getName(),
                 decompressingStreamClass,
                 new Class<?>[] { InputStream.class },
                 new Object[] { new ByteArrayInputStream(bytes) }),
             StandardCharsets.UTF_8)) {
      content = CharStreams.toString(reader);
    }
    String expected = target("/item/allIDs").request().get(String.class);
    Assert.assertEquals(expected, content);
  }

}
