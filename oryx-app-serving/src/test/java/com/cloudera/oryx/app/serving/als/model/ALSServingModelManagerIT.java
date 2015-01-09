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

package com.cloudera.oryx.app.serving.als.model;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.serving.AbstractServingIT;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.app.speed.als.MockModelUpdateGenerator;

import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public final class ALSServingModelManagerIT extends AbstractServingIT {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModelManagerIT.class);

  @Test
  public void testALSServingModel() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.serving.application-resources",
        "\"com.cloudera.oryx.app.serving,com.cloudera.oryx.app.serving.als\"");
    overlayConfig.put("oryx.serving.model-manager-class", ALSServingModelManager.class.getName());
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();
    startServer(config);
    startUpdateTopics(new MockModelUpdateGenerator(), 10);

    // Let updates finish
    Thread.sleep(1000);

    ALSServingModelManager manager = (ALSServingModelManager)
        getServingLayer().getContext().getServletContext().getAttribute(
            AbstractOryxResource.MODEL_MANAGER_KEY);

    assertNotNull("Manager must initialize in web context", manager);

    ALSServingModel model = manager.getModel();
    log.info("{}", model);

    assertNotNull(model);
    assertEquals(2, model.getFeatures());
    assertTrue(model.isImplicit());

    Collection<String> expectedItems = MockModelUpdateGenerator.Y.keySet();
    assertTrue(expectedItems.containsAll(model.getAllItemIDs()));
    assertTrue(model.getAllItemIDs().containsAll(expectedItems));

    assertNotNull(model.getYTYSolver());

    for (Map.Entry<String,float[]> entry : MockModelUpdateGenerator.X.entrySet()) {
      assertArrayEquals(entry.getValue(), model.getUserVector(entry.getKey()));
    }
    for (Map.Entry<String,float[]> entry : MockModelUpdateGenerator.Y.entrySet()) {
      assertArrayEquals(entry.getValue(), model.getItemVector(entry.getKey()));
    }
    for (Map.Entry<String,Collection<String>> entry : MockModelUpdateGenerator.A.entrySet()) {
      Collection<String> expected = entry.getValue();
      Collection<String> actual = model.getKnownItems(entry.getKey());
      assertTrue(expected.containsAll(actual));
      assertTrue(actual.containsAll(expected));
    }
  }

}
