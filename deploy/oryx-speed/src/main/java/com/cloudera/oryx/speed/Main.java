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

package com.cloudera.oryx.speed;

import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.HadoopUtils;
import com.cloudera.oryx.lambda.speed.SpeedLayer;

/**
 * Runs {@link SpeedLayer} from the command line. It will use configuration as loaded
 * by TypeSafe Config's {@code ConfigFactory}.
 */
public final class Main {

  private Main() {}

  public static void main(String[] args) throws Exception {
    try (SpeedLayer<?,?,?> speedLayer = new SpeedLayer<>(ConfigUtils.getDefault())) {
      HadoopUtils.closeAtShutdown(speedLayer);
      speedLayer.start();
      speedLayer.await();
    }
  }

}
