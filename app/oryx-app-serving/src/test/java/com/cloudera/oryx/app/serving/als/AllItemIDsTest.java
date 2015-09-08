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
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;

public final class AllItemIDsTest extends AbstractALSServingTest {

  @Test
  public void testAllItemIDs() {
    List<String> items = target("/item/allIDs").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_STRING_TYPE);
    Assert.assertEquals(9, items.size());
    for (int item = 0; item < 9; item++) {
      OryxTest.assertContains(items, "I" + item);
    }
  }

  @Test
  public void testAllItemIDsCSV() {
    String response = target("/item/allIDs").request().get(String.class);
    Assert.assertEquals(9, response.split("\n").length);
  }

}
