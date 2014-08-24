package com.cloudera.oryx.ml.serving.als;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;

public class PreferenceTest extends AbstractALSServingTest {

  @Test
  public void testPost() {
    Response response = target("pref").path("Z").path("A").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get();
    Assert.assertEquals(201, response.getStatus());
  }

  @Test
  public void testDelete() {
    Response response = target("pref").path("Z").path("A").request().accept(MediaType.APPLICATION_JSON_TYPE).delete();
    Assert.assertEquals(200, response.getStatus());
  }
}
