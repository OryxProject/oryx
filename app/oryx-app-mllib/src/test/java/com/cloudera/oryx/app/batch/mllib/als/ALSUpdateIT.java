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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.als.ALSUtilsTest;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.ml.MLUpdate;

public final class ALSUpdateIT extends AbstractALSIT {

  private static final Logger log = LoggerFactory.getLogger(ALSUpdateIT.class);

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int FEATURES = 4;
  private static final double LAMBDA = 0.001;
  private static final int NUM_USERS_ITEMS = 1000;

  @Test
  public void testALS() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir =  tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");

    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.batch.update-class", ALSUpdate.class.getName());
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.data-dir", dataDir);
    ConfigUtils.set(overlayConfig, "oryx.batch.storage.model-dir", modelDir);
    overlayConfig.put("oryx.batch.streaming.generation-interval-sec", GEN_INTERVAL_SEC);
    overlayConfig.put("oryx.als.implicit", false);
    overlayConfig.put("oryx.als.hyperparams.lambda", LAMBDA);
    overlayConfig.put("oryx.als.hyperparams.features", FEATURES);
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();

    List<Pair<String,String>> updates = startServerProduceConsumeTopics(
        config,
        new RandomALSDataGenerator(NUM_USERS_ITEMS, NUM_USERS_ITEMS, 1, 5),
        DATA_TO_WRITE,
        WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");

    int generations = modelInstanceDirs.size();
    checkIntervals(generations, DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    List<Collection<String>> userIDs = new ArrayList<>();
    userIDs.add(Collections.<String>emptySet()); // Add dummy empty set as prior value
    List<Collection<String>> productIDs = new ArrayList<>();
    productIDs.add(Collections.<String>emptySet()); // Add dummy empty set as prior value

    for (Path modelInstanceDir : modelInstanceDirs) {
      Path modelFile = modelInstanceDir.resolve(MLUpdate.MODEL_FILE_NAME);
      assertTrue("Model file should exist: " + modelFile, Files.exists(modelFile));
      assertTrue("Model file should not be empty: " + modelFile, Files.size(modelFile) > 0);
      PMMLUtils.read(modelFile); // Shouldn't throw exception
      Path xDir = modelInstanceDir.resolve("X");
      assertTrue(Files.exists(xDir));
      userIDs.add(checkFeatures(xDir, userIDs.get(userIDs.size() - 1)));
      Path yDir = modelInstanceDir.resolve("Y");
      assertTrue(Files.exists(yDir));
      productIDs.add(checkFeatures(yDir, productIDs.get(productIDs.size() - 1)));
    }
    // Remove dummy empty sets
    userIDs.remove(0);
    productIDs.remove(0);

    Collection<String> expectedUsers = null;
    Collection<String> expectedProducts = null;
    Collection<String> seenUsers = null;
    Collection<String> seenProducts = null;
    Collection<String> lastModelUsers = null;
    Collection<String> lastModelProducts = null;
    int whichGeneration = -1;
    for (Pair<String,String> km : updates) {

      String type = km.getFirst();
      String value = km.getSecond();

      log.debug("{} = {}", type, value);

      boolean isModel = "MODEL".equals(type);
      boolean isUpdate = "UP".equals(type);
      assertTrue(isModel || isUpdate);

      if (isUpdate) {

        assertNotNull(seenUsers);
        assertNotNull(seenProducts);

        List<?> update = MAPPER.readValue(value, List.class);
        // First field is X or Y, depending on whether it's a user or item vector
        String whichMatrixField = update.get(0).toString();
        boolean isUser = "X".equals(whichMatrixField);
        boolean isProduct = "Y".equals(whichMatrixField);
        // Next is user/item ID
        String id = update.get(1).toString();
        assertTrue(isUser || isProduct);
        if (isUser) {
          seenUsers.add(id);
        } else {
          seenProducts.add(id);
        }
        // Verify that feature vector are valid floats
        for (float f : MAPPER.convertValue(update.get(2), float[].class)) {
          assertTrue(!Float.isNaN(f) && !Float.isInfinite(f));
        }

        if (isUser) {
          // Only known-items for users exist now, not known users for items
          @SuppressWarnings("unchecked")
          Collection<String> knownUsersItems = (Collection<String>) update.get(3);
          assertFalse(knownUsersItems.isEmpty());
          for (String known : knownUsersItems) {
            int i = ALSUtilsTest.stringIDtoID(known);
            assertTrue(i >= 0 && i < NUM_USERS_ITEMS);
          }
        }

      } else {

        PMML pmml = PMMLUtils.fromString(value);

        checkHeader(pmml.getHeader());

        assertEquals(7, pmml.getExtensions().size());
        Map<String,Object> expected = new HashMap<>();
        expected.put("features", FEATURES);
        expected.put("lambda", LAMBDA);
        expected.put("implicit", false);
        checkExtensions(pmml, expected);

        // See if users/item sets seen in updates match what was expected from output
        assertContainsSame(expectedUsers, seenUsers);
        assertContainsSame(expectedProducts, seenProducts);

        // Also check key sets reported in model
        assertContainsSame(expectedUsers, lastModelUsers);
        assertContainsSame(expectedProducts, lastModelProducts);

        // Update for next round
        whichGeneration++;
        expectedUsers = userIDs.get(whichGeneration);
        expectedProducts = productIDs.get(whichGeneration);
        seenUsers = new HashSet<>();
        seenProducts = new HashSet<>();
        lastModelUsers = AppPMMLUtils.getExtensionContent(pmml, "XIDs");
        lastModelProducts = AppPMMLUtils.getExtensionContent(pmml, "YIDs");

      }
    }

  }

  private static Collection<String> checkFeatures(Path path, Collection<String> previousIDs)
      throws IOException {
    Collection<String> seenIDs = new HashSet<>();
    for (Path file : IOUtils.listFiles(path, "part-*")) {
      for (String line : IOUtils.readLines(file)) {
        List<?> update = MAPPER.readValue(line, List.class);
        seenIDs.add(update.get(0).toString());
        assertEquals(FEATURES, MAPPER.convertValue(update.get(1), float[].class).length);
      }
    }
    assertFalse(seenIDs.isEmpty());
    assertTrue(seenIDs.containsAll(previousIDs));
    return seenIDs;
  }

}
