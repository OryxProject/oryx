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

import com.google.common.base.Preconditions;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.collection.FormatUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.lambda.serving.ServingModelManager;

public final class ALSServingModelManager implements ServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSServingModelManager.class);

  private ALSServingModel model;

  @Override
  public void consume(Iterator<Pair<String,String>> updateIterator) throws IOException {
    while (updateIterator.hasNext()) {
      Pair<String,String> km = updateIterator.next();
      String key = km.getFirst();
      String message = km.getSecond();
      switch (key) {
        case "UP":
          Preconditions.checkNotNull(model);
          // Update
          String[] tokens = message.split("\t");
          int id = Integer.parseInt(tokens[1]);
          float[] vector = FormatUtils.parseFloatVec(tokens[2]);
          switch (tokens[0]) {
            case "X":
              model.setUserVector(id, vector);
              break;
            case "Y":
              model.setItemVector(id, vector);
              break;
            default:
              throw new IllegalStateException("Bad update " + message);
          }
          break;

        case "MODEL":
          // New model
          PMML pmml = PMMLUtils.fromString(message);
          int features = Integer.parseInt(PMMLUtils.getExtensionValue(pmml, "features"));
          if (model == null) {

            log.info("No previous model");
            model = new ALSServingModel(features);

          } else if (features != model.getFeatures()) {

            log.warn("# features has changed! removing old model");
            model = new ALSServingModel(features);

          } else {

            // First, remove users/items no longer in the model
            Collection<Integer> XIDs =
                parseIDsFromContent(PMMLUtils.getExtensionContent(pmml, "XIDs"));
            Collection<Integer> YIDs =
                parseIDsFromContent(PMMLUtils.getExtensionContent(pmml, "YIDs"));
            model.retainAllUsers(XIDs);
            model.retainAllItems(YIDs);

          }
          break;

        default:
          throw new IllegalStateException("Bad model " + message);
      }
    }
  }

  private static Collection<Integer> parseIDsFromContent(List<?> content) {
    String[] tokens = content.get(0).toString().split(" ");
    Collection<Integer> result = new HashSet<>(content.size());
    for (String s : tokens) {
      result.add(Integer.valueOf(s));
    }
    return result;
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
