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

package com.cloudera.oryx.ml.mllib.als;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class ALSModelContentIT extends AbstractALSIT {

  private static final Logger log = LoggerFactory.getLogger(ALSModelContentIT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testModelContent() throws Exception {
    Path tempDir = getTempDir();
    Path modelDir = tempDir.resolve("model");

    Map<String, String> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", ALSUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", tempDir.resolve("data"));
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.generation-interval-sec", "10");
    overlayConfig.put("oryx.batch.block-interval-sec", "1");
    overlayConfig.put("oryx.ml.eval.test-fraction", "0");
    overlayConfig.put("oryx.als.implicit", "false");
    overlayConfig.put("oryx.als.hyperparams.lambda", "0.0001");
    overlayConfig.put("oryx.als.hyperparams.features", "2");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessageQueue();

    ModelContentDataGenerator generator = new ModelContentDataGenerator();
    List<Pair<String, String>> updates = startServerProduceConsumeQueues(
        config,
        generator,
        generator.getSentData().size(),
        100);

    Collection<Integer> modelUsers = null;
    Collection<Integer> modelItems = null;
    Map<Integer,Collection<Integer>> knownUsersItems = new HashMap<>();

    for (Pair<String, String> km : updates) {
      String type = km.getFirst();
      String value = km.getSecond();
      log.info("{} = {}", type, value);

      if ("UP".equals(type)) {

        List<?> update = MAPPER.readValue(value, List.class);
        if ("X".equals(update.get(0).toString())) {
          Integer userID = Integer.valueOf(update.get(1).toString());
          @SuppressWarnings("unchecked")
          Collection<String> userKnownItems = (Collection<String>) update.get(3);
          Collection<Integer> knownItemsIDs = new ArrayList<>();
          for (String itemString : userKnownItems) {
            knownItemsIDs.add(Integer.valueOf(itemString));
          }
          knownUsersItems.put(userID, knownItemsIDs);
        }

      } else { // "MODEL"

        PMML pmml = PMMLUtils.fromString(value);
        modelUsers = parseIDsFromContent(PMMLUtils.getExtensionContent(pmml, "XIDs"));
        modelItems = parseIDsFromContent(PMMLUtils.getExtensionContent(pmml, "YIDs"));

      }

    }

    assertEquals(new HashSet<>(Arrays.asList(0, 1, 2)), modelUsers);
    assertEquals(new HashSet<>(Arrays.asList(0, 1, 2, 3)), modelItems);
    assertEquals(Arrays.asList(0, 1, 2, 3), knownUsersItems.get(0));
    assertEquals(Arrays.asList(2, 3), knownUsersItems.get(1));
    assertEquals(Arrays.asList(3), knownUsersItems.get(2));
  }

}
