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

import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipOutputStream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Variant;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.lambda.serving.MockTopicProducer;

public final class IngestTest extends AbstractALSServingTest {

  private static final String INGEST_DATA = "a,B,1\nc,B\nc,D,5.5\nc,D,\na,C,2,123456789";
  private static final String[][] EXPECTED_TOPIC = {
      {"a", "B", "1.0"},
      {"c", "B", "1"},
      {"c", "D", "5.5"},
      {"c", "D", ""},
      {"a", "C", "2.0"},
  };

  @Test
  public void testSimpleIngest() {
    checkResponse(target("/ingest").request().post(Entity.text(INGEST_DATA)));
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
    checkResponse(target("/ingest").request().post(entity));
  }

  @Test
  public void testFormIngest() throws Exception {
    checkResponse(getFormPostResponse(INGEST_DATA, "/ingest", null, null));
  }

  @Test
  public void testGzippedFormIngest() throws Exception {
    checkResponse(getFormPostResponse(INGEST_DATA, "/ingest", GZIPOutputStream.class, "gzip"));
  }

  @Test
  public void testZippedFormIngest() throws Exception {
    checkResponse(getFormPostResponse(INGEST_DATA, "/ingest", ZipOutputStream.class, "zip"));
  }

  private static Variant compressedVariant(MediaType contentType, String contentEncoding) {
    return Variant.mediaTypes(contentType).encodings(contentEncoding).build().get(0);
  }

  private static void checkResponse(Response response) {
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    List<Pair<String,String>> data = MockTopicProducer.getData();
    for (int i = 0; i < data.size(); i++) {
      Pair<String,String> actual = data.get(i);
      Assert.assertNotNull(actual.getFirst());
      String[] tokens = actual.getSecond().split(",");
      Assert.assertEquals(EXPECTED_TOPIC[i][0], tokens[0]);
      Assert.assertEquals(EXPECTED_TOPIC[i][1], tokens[1]);
      Assert.assertEquals(EXPECTED_TOPIC[i][2], tokens[2]);
    }
  }

}
