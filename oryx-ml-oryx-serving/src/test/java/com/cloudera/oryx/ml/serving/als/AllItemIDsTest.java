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

import java.util.Collection;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.Test;

public final class AllItemIDsTest extends AbstractALSServingTest {

  @Override
  protected Class<?> getResourceClass() {
    return AllItemIDs.class;
  }

  @Test
  public void test() {
    GenericType<Collection<String>> genericList = new GenericType<Collection<String>>() {
    };
    Collection<String> items = target("item/allIDs").request().accept(MediaType.APPLICATION_JSON_TYPE).get(genericList);
    Assert.assertTrue(items.size() > 0);
  }
}
