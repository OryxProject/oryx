/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.serving.als;

import com.cloudera.oryx.ml.serving.als.Recommend;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Application;

public final class RecommendTest extends JerseyTest {
 
  @Override
  protected Application configure() {
    return new ResourceConfig(Recommend.class);
  }

  @Test
  public void test() {
    String recommend = target("recommend/foo").request().get(String.class);
    Assert.assertNotNull(recommend);
  }
}
