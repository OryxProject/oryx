/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.lambda.serving;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.catalina.Context;
import org.junit.Test;

import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class ServingLayerTest extends OryxTest {

  @Test
  public void testServingLayer() throws Exception {
    Map<String,Object> overlay = buildOverlay();
    Config config = ConfigUtils.overlayOn(overlay, ConfigUtils.getDefault());
    doTestServingLayer(config);
  }

  @Test
  public void testServingLayerSecure() throws Exception {
    Path keystoreFile = SecureAPIConfigIT.buildKeystoreFile();
    Map<String,Object> overlay = buildOverlay();
    overlay.put("oryx.serving.api.keystore-file", "\"" + keystoreFile + "\"");
    overlay.put("oryx.serving.api.keystore-password", "oryxpass");
    overlay.put("oryx.serving.api.key-alias", "oryxtest");
    Config config = ConfigUtils.overlayOn(overlay, ConfigUtils.getDefault());
    try {
      doTestServingLayer(config);
    } finally {
      Files.delete(Paths.get(config.getString("oryx.serving.api.keystore-file")));
    }
  }

  private static Map<String,Object> buildOverlay() throws IOException {
    Map<String,Object> overlay = new HashMap<>();
    overlay.put("oryx.serving.api.port", IOUtils.chooseFreePort());
    overlay.put("oryx.serving.api.secure-port", IOUtils.chooseFreePort());
    overlay.put("oryx.serving.application-resources", "com.cloudera.oryx.lambda.serving");
    overlay.put("oryx.serving.model-manager-class", MockServingModelManager.class.getName());
    overlay.put("oryx.serving.no-init-topics", true);
    return overlay;
  }

  private static void doTestServingLayer(Config config) throws IOException {
    try (ServingLayer servingLayer = new ServingLayer(config)) {
      servingLayer.start();
      Context context = servingLayer.getContext();
      assertNotNull(context.findErrorPage(500));
      assertEquals(0, context.getApplicationLifecycleListeners().length);
      assertNotNull(context.findParameter(ConfigUtils.class.getName() + ".serialized"));
      assertNotNull(context.getName());
      assertGreaterOrEqual(Double.parseDouble(context.getWebappVersion()), 3.1);
    }
  }

}
