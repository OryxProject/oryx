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

package com.cloudera.oryx.ml.serving.als;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Variant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.lang.ClassUtils;

public final class IngestTest extends AbstractALSServingTest {

  private static final String INGEST_DATA = "a,B,1\nc,B\nc,D,5.5\n";
  private static final String[] INGEST_LINES = INGEST_DATA.split("\n");

  @Before
  public void clearProducerData() {
    MockQueueProducer.getData().clear();
  }

  @Test
  public void testSimpleIngest() {
    Response response = target("/ingest").request().post(Entity.text(INGEST_DATA));
    checkResponse(response);
  }

  @Test
  public void testGZippedIngest() {
    byte[] compressed = compress(INGEST_DATA, GZIPOutputStream.class);
    Entity<byte[]> entity = Entity.entity(
        compressed, compressedVariant(MediaType.TEXT_PLAIN_TYPE, "gzip"));
    Response response = target("/ingest").request().post(entity);
    checkResponse(response);
  }

  @Test
  public void testDeflateIngest() {
    byte[] compressed = compress(INGEST_DATA, DeflaterOutputStream.class);
    Entity<byte[]> entity = Entity.entity(
        compressed, compressedVariant(MediaType.TEXT_PLAIN_TYPE, "deflate"));
    Response response = target("/ingest").request().post(entity);
    checkResponse(response);
  }

  @Ignore("Form parts unsupported by Grizzly so far")
  @Test
  public void testFormIngest() {
    Response response = target("/ingest").request().post(
        Entity.entity(INGEST_DATA, MediaType.MULTIPART_FORM_DATA_TYPE));
    checkResponse(response);
  }

  @Ignore("Form parts unsupported by Grizzly so far")
  @Test
  public void testGzippedFormIngest() {
    byte[] compressed = compress(INGEST_DATA, GZIPOutputStream.class);
    Entity<byte[]> entity = Entity.entity(
        compressed, compressedVariant(MediaType.MULTIPART_FORM_DATA_TYPE, "gzip"));
    Response response = target("/ingest").request().post(entity);
    checkResponse(response);
  }

  private static Variant compressedVariant(MediaType contentType, String contentEncoding) {
    return Variant.mediaTypes(contentType).encodings(contentEncoding).build().get(0);
  }

  private static byte[] compress(
      String data, Class<? extends OutputStream> compressingStreamClass) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (OutputStream compressingStream =
             ClassUtils.loadInstanceOf(compressingStreamClass.getName(),
                                       compressingStreamClass,
                                       new Class<?>[] { OutputStream.class },
                                       new Object[] { bytes })) {
      compressingStream.write(data.getBytes(StandardCharsets.UTF_8));
      compressingStream.flush();
    } catch (IOException e) {
      // Can't happen
      throw new IllegalStateException(e);
    }
    return bytes.toByteArray();
  }

  private static void checkResponse(Response response) {
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    List<Pair<String,String>> data = MockQueueProducer.getData();
    for (int i = 0; i < data.size(); i++) {
      Pair<String,String> expected = data.get(i);
      Assert.assertNull(expected.getFirst());
      Assert.assertEquals(INGEST_LINES[i], expected.getSecond());
    }
  }

}
