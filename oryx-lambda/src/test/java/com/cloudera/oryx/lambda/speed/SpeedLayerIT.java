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

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Tests {@link SpeedLayer}.
 */
public final class SpeedLayerIT extends AbstractSpeedIT {

  private static final int GEN_INTERVAL_SEC = 3;
  private static final int BLOCK_INTERVAL_SEC = 1;

  @Test
  public void testSpeedLayer() {
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("speed.model-manager-class", MockSpeedModelManager.class.getName());
    overlayConfig.put("speed.generation-interval-sec",
                      Integer.toString(GEN_INTERVAL_SEC));
    overlayConfig.put("speed.block-interval-sec",
                      Integer.toString(BLOCK_INTERVAL_SEC));
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());
  }

}
