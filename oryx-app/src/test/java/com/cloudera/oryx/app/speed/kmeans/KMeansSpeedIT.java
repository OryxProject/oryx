/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.speed.kmeans;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.speed.AbstractSpeedIT;

public class KMeansSpeedIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(KMeansSpeedIT.class);

  private static final int NUM_INPUT = 10;

  @Test
  public void testKMeansServingModel() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.speed.model-manager-class", KMeansSpeedModelManager.class.getName());
    overlayConfig.put("oryx.speed.streaming.generation-interval-sec", 5);
    overlayConfig.put("oryx.speed.streaming.block-interval-sec", 1);
    overlayConfig.put("oryx.input-schema.feature-names", "[\"x\",\"y\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String,String>> updates =
        startServerProduceConsumeTopics(config, new MockKMeansInputGenerator(),
            new MockKMeansModelGenerator(), NUM_INPUT, 1);

    if (log.isDebugEnabled()) {
      for (Pair<String, String> update : updates) {
        log.debug("{}", update);
      }
    }

    int numUpdates = updates.size();
    log.info("Num updates: {}", numUpdates);

    assertEquals(11, updates.size());
    assertEquals("MODEL", updates.get(0).getFirst());

  }

}
