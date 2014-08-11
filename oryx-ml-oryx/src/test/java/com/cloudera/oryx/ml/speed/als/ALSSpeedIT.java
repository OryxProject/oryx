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

package com.cloudera.oryx.ml.speed.als;

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

public final class ALSSpeedIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(ALSSpeedIT.class);

  @Test
  public void testALSSpeed() throws Exception {
    int features = 2;
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("speed.model-manager-class", ALSSpeedModelManager.class.getName());
    overlayConfig.put("speed.generation-interval-sec", "3");
    overlayConfig.put("speed.block-interval-sec", "1");
    overlayConfig.put("als.hyperparams.implicit", "true");
    overlayConfig.put("als.hyperparams.features", Integer.toString(features));
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessageQueue();

    List<Pair<String,String>> updates =
        startServerProduceConsumeQueues(config,
                                        new MockInputGenerator(),
                                        new MockModelUpdateGenerator(features),
                                        1000, 10);

    log.info("{}", updates);
  }

}
