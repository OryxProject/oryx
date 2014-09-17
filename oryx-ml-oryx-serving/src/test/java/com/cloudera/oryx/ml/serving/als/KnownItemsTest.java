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

import java.util.Arrays;
import java.util.List;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

public final class KnownItemsTest extends AbstractALSServingTest {

  @Test
  public void testKnownItems() {
    List<String> items = target("/knownItems/U1").request()
        .accept(MediaType.APPLICATION_JSON_TYPE).get(LIST_STRING_TYPE);
    Assert.assertEquals(5, items.size());
    for (int i : new int[] {1, 4, 5, 7, 8}) {
      Assert.assertTrue(items.contains("I" + i));
    }
  }

  @Test
  public void testKnownItemsCSV() {
    String response = target("/knownItems/U1").request().get(String.class);
    List<String> items = Arrays.asList(response.split("\n"));
    Assert.assertEquals(5, items.size());
    for (int i : new int[] {1, 4, 5, 7, 8}) {
      Assert.assertTrue(items.contains("I" + i));
    }
  }

}
