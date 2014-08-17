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

package com.cloudera.oryx.ml.serving.als.model;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.serving.AbstractServingIT;
import com.cloudera.oryx.ml.speed.als.MockModelUpdateGenerator;
import com.typesafe.config.Config;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public final class ALSServingModelManagerIT extends AbstractServingIT {

  @Test
  public void testALS() throws Exception {
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("serving.application-resources", "com.cloudera.oryx.ml.serving.als");
    overlayConfig.put("serving.model-manager-class", MockALSServingModelManager.class.getName());
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessageQueue();
    startServerUpdateQueues(config, new MockModelUpdateGenerator(), 10);

    MockALSServingModelManager manager = MockALSServingModelManager.getInstance();
    assertNotNull("Manager must initialize in web context", manager);

    ALSServingModel model = manager.getModel();
    assertNotNull(model);
    //assertEquals(2, model.getFeatures());
  }

}
