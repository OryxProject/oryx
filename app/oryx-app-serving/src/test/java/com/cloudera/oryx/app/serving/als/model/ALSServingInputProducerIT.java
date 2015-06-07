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

package com.cloudera.oryx.app.serving.als.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.app.serving.AbstractOryxResource;
import com.cloudera.oryx.common.collection.CloseableIterator;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.kafka.util.ConsumeData;
import com.cloudera.oryx.kafka.util.ConsumeTopicRunnable;
import com.cloudera.oryx.lambda.serving.AbstractServingIT;

public final class ALSServingInputProducerIT extends AbstractServingIT {

  private static final Logger log = LoggerFactory.getLogger(ALSServingInputProducerIT.class);

  @Test
  public void testALSInputProducer() throws Exception {
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.serving.application-resources",
        "\"com.cloudera.oryx.app.serving,com.cloudera.oryx.app.serving.als\"");
    overlayConfig.put("oryx.serving.model-manager-class", ALSServingModelManager.class.getName());
    Config config = ConfigUtils.overlayOn(overlayConfig, getConfig());

    startMessaging();
    startServer(config);

    @SuppressWarnings("unchecked")
    TopicProducer<String,String> inputProducer = (TopicProducer<String,String>)
        getServingLayer().getContext().getServletContext().getAttribute(
            AbstractOryxResource.INPUT_PRODUCER_KEY);

    String[] inputs = {
        "abc,123,1.5",
        "xyz,234,-0.5",
        "AB,10,0",
    };

    List<Pair<String,String>> keyMessages;
    try (CloseableIterator<Pair<String,String>> data =
             new ConsumeData(INPUT_TOPIC, getZKPort()).iterator()) {

      log.info("Starting consumer thread");
      ConsumeTopicRunnable consumeInput = new ConsumeTopicRunnable(data, inputs.length);
      new Thread(consumeInput, "ConsumeInputThread").start();

      consumeInput.awaitRun();

      for (String input : inputs) {
        inputProducer.send("", input);
      }

      consumeInput.awaitMessages();
      keyMessages = consumeInput.getKeyMessages();
    }

    for (int i = 0; i < keyMessages.size(); i++) {
      Pair<String,String> keyMessage = keyMessages.get(i);
      assertEquals("", keyMessage.getFirst());
      assertEquals(inputs[i], keyMessage.getSecond());
    }
    assertEquals(inputs.length, keyMessages.size());

  }

}
