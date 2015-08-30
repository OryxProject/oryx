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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.serving.ServingModelManager;
import com.cloudera.oryx.app.als.AbstractRescorerProvider;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * A {@link ServingModelManager} that manages and provides access to an {@link ALSServingModel}
 * for the ALS Serving Layer application.
 */
public final class ALSServingModelManager implements ServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModelManager.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Config config;
  private ALSServingModel model;
  private final double sampleRate;
  private final RescorerProvider rescorerProvider;

  public ALSServingModelManager(Config config) {
    this.config = config;
    String rescorerProviderClass =
        ConfigUtils.getOptionalString(config, "oryx.als.rescorer-provider-class");
    rescorerProvider = AbstractRescorerProvider.loadRescorerProviders(rescorerProviderClass);
    sampleRate = config.getDouble("oryx.als.sample-rate");
    Preconditions.checkArgument(sampleRate > 0.0 && sampleRate <= 1.0);
  }

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator, Configuration hadoopConf)
      throws IOException {
    int countdownToLogModel = 10000;
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      Objects.requireNonNull(key, "Bad message: " + km);
      switch (key) {
        case "UP":
          if (model == null) {
            continue; // No model to interpret with yet, so skip it
          }
          List<?> update = MAPPER.readValue(message, List.class);
          // Update
          String id = update.get(1).toString();
          float[] vector = MAPPER.convertValue(update.get(2), float[].class);
          switch (update.get(0).toString()) {
            case "X":
              model.setUserVector(id, vector);
              if (update.size() > 3) {
                @SuppressWarnings("unchecked")
                Collection<String> knownItems = (Collection<String>) update.get(3);
                model.addKnownItems(id, knownItems);
              }
              break;
            case "Y":
              model.setItemVector(id, vector);
              // Right now, no equivalent knownUsers
              break;
            default:
              throw new IllegalArgumentException("Bad message: " + km);
          }
          if (--countdownToLogModel <= 0) {
            log.info("{}", model);
            countdownToLogModel = 10000;
          }
          break;

        case "MODEL":
        case "MODEL-REF":
          log.info("Loading new model");
          PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);

          int features = Integer.parseInt(AppPMMLUtils.getExtensionValue(pmml, "features"));
          boolean implicit = Boolean.valueOf(AppPMMLUtils.getExtensionValue(pmml, "implicit"));

          if (model == null || features != model.getFeatures()) {
            log.warn("No previous model, or # features has changed; creating new one");
            model = new ALSServingModel(features, implicit, sampleRate, rescorerProvider);
          }

          log.info("Updating model");
          // Remove users/items no longer in the model
          Collection<String> XIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "XIDs"));
          Collection<String> YIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "YIDs"));
          model.retainRecentAndKnownItems(XIDs, YIDs);
          model.retainRecentAndUserIDs(XIDs);
          model.retainRecentAndItemIDs(YIDs);
          log.info("Model updated: {}", model);
          break;

        default:
          throw new IllegalArgumentException("Bad message: " + km);
      }
    }
  }

  @Override
  public Config getConfig() {
    return config;
  }

  @Override
  public ALSServingModel getModel() {
    return model;
  }

  @Override
  public void close() {
    // do nothing
  }

}
