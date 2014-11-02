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

package com.cloudera.oryx.ml.serving.als.model;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.serving.ServingModelManager;

public final class ALSServingModelManager implements ServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModelManager.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private ALSServingModel model;

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator) throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      switch (key) {
        case "UP":
          Preconditions.checkNotNull(model);
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
          PMML pmml = PMMLUtils.fromString(message);
          int features = Integer.parseInt(PMMLUtils.getExtensionValue(pmml, "features"));
          boolean implicit = Boolean.valueOf(PMMLUtils.getExtensionValue(pmml, "implicit"));
          if (model == null) {

            log.info("No previous model");
            model = new ALSServingModel(features, implicit);

          } else if (features != model.getFeatures()) {

            log.warn("# features has changed! removing old model");
            model = new ALSServingModel(features, implicit);

          } else {

            // Remove users/items no longer in the model
            Collection<String> XIDs =
                new HashSet<>(PMMLUtils.parseArray(PMMLUtils.getExtensionContent(pmml, "XIDs")));
            Collection<String> YIDs =
                new HashSet<>(PMMLUtils.parseArray(PMMLUtils.getExtensionContent(pmml, "YIDs")));
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
