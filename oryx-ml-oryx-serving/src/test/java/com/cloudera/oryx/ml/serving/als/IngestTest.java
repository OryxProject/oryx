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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Variant;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.oryx.common.collection.Pair;

public final class IngestTest extends AbstractALSServingTest {

  private static final String INGEST_DATA = "a,B,1\nc,B\nc,D,5.5\n";
  private static final String[] INGEST_LINES = INGEST_DATA.split("\n");

  @Test
  public void testSimpleIngest() {
    Response response = target("/ingest").request().post(Entity.text(INGEST_DATA));
    checkResponse(response);
  }

  @Ignore("Form parts unsupported by Grizzly so far")
  @Test
  public void testFormIngest() {
    Response response = target("/ingest").request().post(
        Entity.entity(INGEST_DATA, MediaType.MULTIPART_FORM_DATA_TYPE));
    checkResponse(response);
  }

  @Ignore("TODO figure out how to handle compressed input")
  @Test
  public void testGZippedIngest() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream zipOut = new GZIPOutputStream(bytes)) {
      zipOut.write(INGEST_DATA.getBytes(StandardCharsets.UTF_8));
    }
    Response response = target("/ingest").request().post(Entity.entity(bytes.toByteArray(),
        Variant.mediaTypes(MediaType.TEXT_PLAIN_TYPE).encodings("application/gzip").build().get(0)));
    checkResponse(response);
  }

  private static void checkResponse(Response response) {
    Assert.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    List<Pair<String, String>> data = MockQueueProducer.getData();
    for (int i = 0; i < data.size(); i++) {
      Pair<String,String> expected = data.get(i);
      Assert.assertNull(expected.getFirst());
      Assert.assertEquals(INGEST_LINES[i], expected.getSecond());
    }
  }

}
