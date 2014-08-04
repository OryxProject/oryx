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

package com.cloudera.oryx.ml.als.speed;

import java.io.IOException;
import java.util.Collection;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.lambda.speed.SpeedModel;
import com.cloudera.oryx.lambda.speed.SpeedModelManager;
import com.cloudera.oryx.ml.pmml.PMMLUtils;

public final class ALSSpeedModelManager implements SpeedModelManager<UserItemStrength> {

  private static final Logger log = LoggerFactory.getLogger(ALSSpeedModelManager.class);

  private ALSSpeedModel model;

  @Override
  public void start(ConsumerIterator<String,String> updateIterator,
                    Producer<String,String> updateProducer) throws IOException {
    while (updateIterator.hasNext()) {
      MessageAndMetadata<String,String> mam = updateIterator.next();
      if ("UP".equals(mam.key())) {
        // TODO

      } else if ("MODEL".equals(mam.key())) {
        // New model
        PMML pmml = PMMLUtils.fromString(mam.message());
        int features = Integer.parseInt(PMMLUtils.getExtensionValue(pmml, "features"));
        if (model == null) {
          log.info("No previous model");
          model = new ALSSpeedModel(features);
        } else if (features != model.getFeatures()) {
          log.warn("# features has changed! removing old model");
          model = new ALSSpeedModel(features);
        } else {
          // TODO
        }

      } else {
        throw new IllegalStateException("Bad event " + mam.key() + " / " + mam.message());
      }
    }
  }

  @Override
  public void onInput(Collection<UserItemStrength> input) {
    // TODO
  }

  @Override
  public SpeedModel getModel() {
    return model;
  }

  @Override
  public void close() {
    // TODO
  }

}
