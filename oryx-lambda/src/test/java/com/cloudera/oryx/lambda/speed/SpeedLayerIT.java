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

package com.cloudera.oryx.lambda.speed;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests {@link SpeedLayer}.
 */
public final class SpeedLayerIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(SpeedLayerIT.class);

  @Test
  public void testSpeedLayer() throws Exception {
    Path tempDir = getTempDir();
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("speed.model-manager-class", MockSpeedModelManager.class.getName());
    overlayConfig.put("speed.generation-interval-sec", "3");
    overlayConfig.put("speed.block-interval-sec", "1");
    overlayConfig.put("speed.storage.checkpoint-dir",
                      "\"" + tempDir.resolve("checkpoint").toUri() + "\"");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessageQueue();

    List<Pair<String,String>> updates = startServerProduceConsumeQueues(config, 1000, 10);

    int inputToUpdate = 0;
    int receivedUpdates = 0;
    int models = 0;
    for (Pair<String,String> update : updates) {
      String key = update.getFirst();
      String message = update.getSecond();
      if (message.contains(",")) {
        // it's an input converted to update
        assertEquals("UP", update.getFirst());
        inputToUpdate++;
      } else {
        // Else should be just an int
        boolean shouldBeModel = Integer.parseInt(message) % 10 == 0;
        assertEquals(shouldBeModel ? "MODEL" : "UP", key);
        if (shouldBeModel) {
          models++;
        } else {
          receivedUpdates++;
        }
      }
    }

    log.info("Received {} models, {} inputs converted to updates, and {} other updates",
             models, inputToUpdate, receivedUpdates);

    assertEquals(1, models);
    assertEquals(9, receivedUpdates);
    assertEquals(1000, inputToUpdate);
  }

}
