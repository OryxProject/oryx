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

package com.cloudera.oryx.ml.mllib.als;

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
import org.dmg.pmml.Extension;
import org.dmg.pmml.PMML;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.pmml.PMMLUtils;

public final class ALSUpdateIT extends AbstractALSIT {

  private static final Logger log = LoggerFactory.getLogger(ALSUpdateIT.class);

  private static final int DATA_TO_WRITE = 2000;
  private static final int WRITE_INTERVAL_MSEC = 10;
  private static final int GEN_INTERVAL_SEC = 10;
  private static final int BLOCK_INTERVAL_SEC = 1;
  private static final int FEATURES = 4;
  private static final double LAMBDA = 0.001;

  @Test
  public void testALS() throws Exception {
    Path tempDir = getTempDir();
    Path dataDir =  tempDir.resolve("data");
    Path modelDir = tempDir.resolve("model");

    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("batch.update-class", ALSUpdate.class.getName());
    overlayConfig.put("batch.storage.data-dir",
                      "\"" + dataDir.toUri() + "\"");
    overlayConfig.put("batch.storage.model-dir",
                      "\"" + modelDir.toUri() + "\"");
    overlayConfig.put("batch.generation-interval-sec",
                      Integer.toString(GEN_INTERVAL_SEC));
    overlayConfig.put("batch.block-interval-sec",
                      Integer.toString(BLOCK_INTERVAL_SEC));
    overlayConfig.put("als.hyperparams.implicit", "false");
    overlayConfig.put("als.hyperparams.lambda", Double.toString(LAMBDA));
    overlayConfig.put("als.hyperparams.features", Integer.toString(FEATURES));
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessageQueue();

    List<String[]> updates =
        startServerProduceConsumeQueues(config,
                                        new RandomALSDataGenerator(1000, 1000, 1, 5),
                                        DATA_TO_WRITE,
                                        WRITE_INTERVAL_MSEC);

    List<Path> modelInstanceDirs = IOUtils.listFiles(modelDir, "*");
    log.info("Model instance dirs: {}", modelInstanceDirs);

    int generations = modelInstanceDirs.size();
    checkIntervals(generations, DATA_TO_WRITE, WRITE_INTERVAL_MSEC, GEN_INTERVAL_SEC);

    List<Collection<Integer>> userIDs = new ArrayList<>();
    userIDs.add(Collections.<Integer>emptySet());
    List<Collection<Integer>> productIDs = new ArrayList<>();
    productIDs.add(Collections.<Integer>emptySet());

    for (Path modelInstanceDir : modelInstanceDirs) {
      log.info("Testing model instance dir {}", modelInstanceDir);
      Path modelFile = modelInstanceDir.resolve(MLUpdate.MODEL_FILE_NAME);
      assertTrue("Model file should exist: " + modelFile, Files.exists(modelFile));
      assertTrue("Model file should not be empty: " + modelFile, Files.size(modelFile) > 0);
      Path xDir = modelInstanceDir.resolve("X");
      assertTrue(Files.exists(xDir));
      userIDs.add(checkFeatures(xDir, userIDs.get(userIDs.size() - 1)));
      Path yDir = modelInstanceDir.resolve("Y");
      assertTrue(Files.exists(yDir));
      productIDs.add(checkFeatures(yDir, productIDs.get(productIDs.size() - 1)));
    }
    userIDs.remove(0);
    productIDs.remove(0);

    Collection<Integer> expectedUsers = null;
    Collection<Integer> expectedProducts = null;
    Collection<Integer> seenUsers = null;
    Collection<Integer> seenProducts = null;
    int whichGeneration = -1;
    for (String[] km : updates) {

      String type = km[0];
      String value = km[1];

      log.debug("{} = {}", type, value);

      boolean isModel = "MODEL".equals(type);
      boolean isUpdate = "UP".equals(type);
      assertTrue(isModel || isUpdate);

      if (isUpdate) {
        String[] tokens = value.split("\t");
        boolean isUser = "X".equals(tokens[0]);
        boolean isProduct = "Y".equals(tokens[0]);
        Integer id = Integer.valueOf(tokens[1]);
        assertTrue(isUser || isProduct);
        if (isUser) {
          seenUsers.add(id);
        } else {
          seenProducts.add(id);
        }
        for (String dimensionString : tokens[2].split(",")) {
          float f = Float.parseFloat(dimensionString);
          assertTrue(!Float.isNaN(f) && !Float.isInfinite(f));
        }
      } else {
        PMML pmml = PMMLUtils.fromString(value);
        List<Extension> extensions = pmml.getExtensions();
        assertEquals(5, extensions.size());
        Map<String,String> extensionKeyValues = new HashMap<>();
        for (Extension extension : extensions) {
          extensionKeyValues.put(extension.getName(), extension.getValue());
        }
        assertEquals(Integer.toString(FEATURES), extensionKeyValues.get("features"));
        assertEquals(Double.toString(LAMBDA), extensionKeyValues.get("lambda"));
        assertEquals("false", extensionKeyValues.get("implicit"));

        assertEquals(expectedUsers, seenUsers);
        assertEquals(expectedProducts, seenProducts);
        whichGeneration++;
        expectedUsers = userIDs.get(whichGeneration);
        expectedProducts = productIDs.get(whichGeneration);
        seenUsers = new HashSet<>();
        seenProducts = new HashSet<>();
      }
    }

  }

  private static Collection<Integer> checkFeatures(Path path, Collection<Integer> previousIDs)
      throws IOException {
    Collection<Integer> seenIDs = new HashSet<>();
    for (Path file : IOUtils.listFiles(path, "part-*")) {
      for (String line : IOUtils.readLines(file)) {
        String[] idVector = line.split("\t");
        seenIDs.add(Integer.valueOf(idVector[0]));
        assertEquals(FEATURES, idVector[1].split(",").length);
      }
    }
    assertFalse(seenIDs.isEmpty());
    assertTrue(seenIDs.containsAll(previousIDs));
    return seenIDs;
  }

}
