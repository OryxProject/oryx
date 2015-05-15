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

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.serving.ServingModelManager;
import com.cloudera.oryx.app.als.AbstractRescorerProvider;
import com.cloudera.oryx.app.als.RescorerProvider;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * A {@link ServingModelManager} that manages and provides access to an {@link ALSServingModel}
 * for the ALS Serving Layer application.
 */
public final class ALSServingModelManager implements ServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModelManager.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ALSServingModel model;
  private final RescorerProvider rescorerProvider;

  public ALSServingModelManager(Config config) {
    String rescorerProviderClass =
        ConfigUtils.getOptionalString(config, "oryx.als.rescorer-provider-class");
    if (rescorerProviderClass == null) {
      rescorerProvider = null;
    } else {
      rescorerProvider = AbstractRescorerProvider.loadRescorerProviders(rescorerProviderClass);
    }
  }

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator) throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
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
              throw new IllegalStateException("Bad update " + message);
          }
          break;

        case "MODEL":
          // New model
          PMML pmml;
          try {
            pmml = PMMLUtils.fromString(message);
          } catch (JAXBException e) {
            throw new IOException(e);
          }
          int features = Integer.parseInt(AppPMMLUtils.getExtensionValue(pmml, "features"));
          boolean implicit = Boolean.valueOf(AppPMMLUtils.getExtensionValue(pmml, "implicit"));
          if (model == null) {

            log.info("No previous model; creating new model");
            model = new ALSServingModel(features, implicit, rescorerProvider);

          } else if (features != model.getFeatures()) {

            log.warn("# features has changed! removing old model and creating new one");
            model = new ALSServingModel(features, implicit, rescorerProvider);

          } else {

            log.info("Updating current model");
            // Remove users/items no longer in the model
            Collection<String> XIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "XIDs"));
            Collection<String> YIDs = new HashSet<>(AppPMMLUtils.getExtensionContent(pmml, "YIDs"));
            model.pruneKnownItems(XIDs, YIDs);
            model.pruneX(XIDs);
            model.pruneY(YIDs);

          }
          break;

        default:
          throw new IllegalStateException("Bad model " + message);
      }
    }
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
