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

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.AbstractBatchIT;

public abstract class AbstractALSIT extends AbstractBatchIT {

  @Override
  protected Config getConfig() {
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("als.hyperparams.iterations", "10");
    overlayConfig.put("als.hyperparams.features", "10");
    overlayConfig.put("als.hyperparams.implicit", "true");
    overlayConfig.put("als.hyperparams.lambda", "0.001");
    overlayConfig.put("als.hyperparams.alpha", "1.0");
    return ConfigUtils.overlayOn(overlayConfig, super.getConfig());
  }

}
