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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.speed.AbstractSpeedIT;

public final class ALSSpeedIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(ALSSpeedIT.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testALSSpeed() throws Exception {
    Map<String,String> overlayConfig = new HashMap<>();
    overlayConfig.put("speed.model-manager-class", ALSSpeedModelManager.class.getName());
    overlayConfig.put("speed.generation-interval-sec", "5");
    overlayConfig.put("speed.block-interval-sec", "1");
    overlayConfig.put("als.hyperparams.implicit", "true");
    overlayConfig.put("als.hyperparams.features", "2");
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessageQueue();

    List<Pair<String,String>> updates =
        startServerProduceConsumeQueues(config,
                                        new MockInputGenerator(),
                                        new MockModelUpdateGenerator(),
                                        9, 10);

    for (Pair<String,String> update : updates) {
      log.info("{}", update);
    }

    // 10 original updates. 9 generate just 1 update since user or item is new.
    assertEquals(19, updates.size());
    assertEquals("MODEL", updates.get(0).getFirst());
    assertEquals(2, Integer.parseInt(PMMLUtils.getExtensionValue(
        PMMLUtils.fromString(updates.get(0).getSecond()), "features")));

    for (int i = 1; i <= 9; i++) {
      assertEquals("UP", updates.get(i).getFirst());
      List<?> update = MAPPER.readValue(updates.get(i).getSecond(), List.class);
      boolean isX = "X".equals(update.get(0).toString());
      String id = update.get(1).toString();
      float[] expected = (isX ? MockModelUpdateGenerator.X : MockModelUpdateGenerator.Y).get(id);
      assertArrayEquals(expected, MAPPER.convertValue(update.get(2), float[].class));
    }

    /*
     * User 100 - 104 are solutions to eye(5)*Y*pinv(Y'*Y), but default scaling
     * will produce values that are 3/4 of this since they are brand new.
     * That is, it's really the solution to (0.75*eye(5))*Y*pinv(Y'*Y)
     * Likewise 105 - 108 are (0.75*eye(4))*X*pinv(X'*X)
     */

    Map<String,float[]> X = MockModelUpdateGenerator.buildMatrix(100, new double[][] {
        {-0.2085992442067743,  0.2523213360207475},
        {-0.2247280310573082, -0.1929485017146139},
        {-0.1559213545536042,  0.3977631145260019},
        {-0.3006521945941331, -0.1223970296839849},
        {-0.0920529503873587, -0.3747183657047325},
    });
    Map<String,float[]> Y = MockModelUpdateGenerator.buildMatrix(105, new double[][] {
        {-0.1966328800604910,  0.0957410625834965},
        {-0.2384041642283309, -0.5085072425781164},
        {-0.3436097549067730,  0.2466687004987837},
        {-0.0602045721873638,  0.2931111530627041},
    });

    for (int i = 10; i <= 18; i++) {
      assertEquals("UP", updates.get(i).getFirst());
      List<?> update = MAPPER.readValue(updates.get(i).getSecond(), List.class);
      boolean isX = "X".equals(update.get(0).toString());
      String id = update.get(1).toString();
      float[] expected = (isX ? X : Y).get(id);
      assertArrayEquals(expected, MAPPER.convertValue(update.get(2), float[].class), 1.0e-5f);
    }

  }

}
