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

import javax.ws.rs.core.MediaType;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public final class AllUserIDsTest extends AbstractALSServingTest {

  @Test
  public void testAllItemIDs() {
    List<String> users = target("/user/allIDs").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_STRING_TYPE);
    Assert.assertEquals(7, users.size());
    for (int user = 0; user < 7; user++) {
      Assert.assertTrue(users.contains("U" + user));
    }
  }

  @Test
  public void testAllUserIDsCSV() {
    String response = target("/user/allIDs").request().get(String.class);
    Assert.assertEquals(7, response.split("\n").length);
  }

}
