/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.app.serving.rdf.model;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.serving.AbstractServingModelManager;
import com.cloudera.oryx.app.classreg.predict.CategoricalPrediction;
import com.cloudera.oryx.app.classreg.predict.NumericPrediction;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.rdf.RDFPMMLUtils;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.rdf.tree.TerminalNode;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * A {@link com.cloudera.oryx.api.serving.ServingModelManager} that manages and provides access to an
 * {@link RDFServingModel} for the random decision forest Serving Layer application.
 */
public final class RDFServingModelManager extends AbstractServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(RDFServingModelManager.class);

  private final InputSchema inputSchema;
  private RDFServingModel model;

  public RDFServingModelManager(Config config) {
    super(config);
    inputSchema = new InputSchema(config);
  }

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "UP":
        if (model == null) {
          return; // No model to interpret with yet, so skip it
        }

        DecisionForest forest = model.getForest();
        List<?> update = TextUtils.readJSON(message, List.class);
        int treeID = Integer.parseInt(update.get(0).toString());
        String nodeID = update.get(1).toString();

        if (inputSchema.isClassification()) {
          TerminalNode nodeToUpdate = (TerminalNode) forest.getTrees()[treeID].findByID(nodeID);
          CategoricalPrediction predictionToUpdate =
              (CategoricalPrediction) nodeToUpdate.getPrediction();
          @SuppressWarnings("unchecked")
          Map<String,Integer> counts = (Map<String,Integer>) update.get(2); // JSON map keys are always Strings
          counts.forEach((encoding, count) -> predictionToUpdate.update(Integer.parseInt(encoding), count));
        } else {
          TerminalNode nodeToUpdate = (TerminalNode) forest.getTrees()[treeID].findByID(nodeID);
          NumericPrediction predictionToUpdate = (NumericPrediction) nodeToUpdate.getPrediction();
          double mean = Double.parseDouble(update.get(2).toString());
          int count = Integer.parseInt(update.get(3).toString());
          predictionToUpdate.update(mean, count);
        }

        break;

      case "MODEL":
      case "MODEL-REF":
        log.info("Loading new model");
        PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);
        if (pmml == null) {
          return;
        }

        RDFPMMLUtils.validatePMMLVsSchema(pmml, inputSchema);
        Pair<DecisionForest,CategoricalValueEncodings> forestAndEncodings =
            RDFPMMLUtils.read(pmml);
        model = new RDFServingModel(forestAndEncodings.getFirst(),
                                    forestAndEncodings.getSecond(),
                                    inputSchema);
        log.info("New model: {}", model);
        break;

      default:
        throw new IllegalArgumentException("Bad key: " + key);
    }
  }

  @Override
  public RDFServingModel getModel() {
    return model;
  }

}
