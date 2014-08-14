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

import com.cloudera.oryx.common.collection.FormatUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.lambda.speed.AbstractSpeedIT;

public final class ALSSpeedIT extends AbstractSpeedIT {

  private static final Logger log = LoggerFactory.getLogger(ALSSpeedIT.class);

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
      String[] tokens = updates.get(i).getSecond().split("\t");
      boolean isX = "X".equals(tokens[0]);
      int id = Integer.parseInt(tokens[1]);
      float[] expected = (isX ? MockModelUpdateGenerator.X : MockModelUpdateGenerator.Y).get(id);
      assertArrayEquals(expected, FormatUtils.parseFloatVec(tokens[2]));
    }

    /*
     * User 100 - 104 are solutions to eye(5)*Y*pinv(Y'*Y), but default scaling
     * will produce values that are half of this since they are brand new.
     * That is, it's really the solution to (eye(5)/2)*Y*pinv(Y'*Y)
     *  -0.139066   0.168214
     *  -0.149819  -0.128632
     *  -0.103948   0.265175
     *  -0.200435  -0.081598
     *  -0.061369  -0.249812
     * Likewise (eye(4)/2)*X*pinv(X'*X)
     *  -0.131089   0.063827
     *  -0.158936  -0.339005
     *  -0.229073   0.164446
     *  -0.040136   0.195407
     */

    Map<Integer,float[]> X = new HashMap<>();
    X.put(100, new float[] {-0.139066f,  0.168214f});
    X.put(101, new float[] {-0.149819f, -0.128632f});
    X.put(102, new float[] {-0.103948f,  0.265175f});
    X.put(103, new float[] {-0.200435f, -0.081598f});
    X.put(104, new float[] {-0.061369f, -0.249812f});
    Map<Integer,float[]> Y = new HashMap<>();
    Y.put(105, new float[] {-0.131089f,  0.063827f});
    Y.put(106, new float[] {-0.158936f, -0.339005f});
    Y.put(107, new float[] {-0.229073f,  0.164446f});
    Y.put(108, new float[] {-0.040136f,  0.195407f});

    for (int i = 10; i <= 18; i++) {
      assertEquals("UP", updates.get(i).getFirst());
      String[] tokens = updates.get(i).getSecond().split("\t");
      boolean isX = "X".equals(tokens[0]);
      int id = Integer.parseInt(tokens[1]);
      float[] expected = (isX ? X : Y).get(id);
      assertArrayEquals(expected, FormatUtils.parseFloatVec(tokens[2]), 1.0e-5f);
    }

  }

}
