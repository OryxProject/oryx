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

package com.cloudera.oryx.app.batch.mllib.als;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.text.TextUtils;

public final class ALSModelContentIT extends AbstractALSIT {

  private static final Logger log = LoggerFactory.getLogger(ALSModelContentIT.class);

  @Test
  public void testModelContent() throws Exception {
    Path tempDir = getTempDir();
    Path modelDir = tempDir.resolve("model");

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", ALSUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", tempDir.resolve("data"));
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", 10);
    overlayConfig.put("oryx.ml.eval.test-fraction", 0);
    overlayConfig.put("oryx.als.implicit", false);
    overlayConfig.put("oryx.als.hyperparams.lambda", 0.0001);
    overlayConfig.put("oryx.als.hyperparams.features", 2);
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    ModelContentDataGenerator generator = new ModelContentDataGenerator();
    List<KeyMessage<String, String>> updates = startServerProduceConsumeTopics(
        config,
        generator,
        generator.getSentData().size(),
        20);

    Collection<String> modelUsers = null;
    Collection<String> modelItems = null;
    Map<String,Collection<String>> knownUsersItems = new HashMap<>();

    for (KeyMessage<String, String> km : updates) {
      String type = km.getKey();
      String value = km.getMessage();
      log.debug("{} = {}", type, value);

      if ("UP".equals(type)) {

        List<?> update = TextUtils.readJSON(value, List.class);
        if ("X".equals(update.get(0).toString())) {
          String userID = update.get(1).toString();
          @SuppressWarnings("unchecked")
          Collection<String> userKnownItems = (Collection<String>) update.get(3);
          knownUsersItems.put(userID, new ArrayList<>(userKnownItems));
        }

      } else {

        assertContains(Arrays.asList("MODEL", "MODEL-REF"), type);
        PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(type, value, null);
        modelUsers = AppPMMLUtils.getExtensionContent(pmml, "XIDs");
        modelItems = AppPMMLUtils.getExtensionContent(pmml, "YIDs");

      }

    }

    assertContainsSame(Arrays.asList("A0", "B1", "C2"), modelUsers);
    assertContainsSame(Arrays.asList("A0", "B1", "C2", "D3"), modelItems);
    assertContainsSame(Arrays.asList("A0", "B1", "C2", "D3"), knownUsersItems.get("A0"));
    assertContainsSame(Arrays.asList("C2", "D3"), knownUsersItems.get("B1"));
    assertContainsSame(Arrays.asList("D3"), knownUsersItems.get("C2"));
  }

}
