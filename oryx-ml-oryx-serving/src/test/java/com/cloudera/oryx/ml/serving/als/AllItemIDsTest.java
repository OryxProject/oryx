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

import java.util.List;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.GenericType;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

public class AllItemIDsTest extends JerseyTest {

  @Override
  protected Application configure() {
    return new ResourceConfig(AllItemIDs.class);
  }

  @Test
  public void test() {
    GenericType<List<Integer>> genericList = new GenericType<List<Integer>>() {
    };
    List<Integer> itemsList = target("items").request().get(genericList);
    Assert.assertTrue(itemsList.size() > 0);
  }
}
