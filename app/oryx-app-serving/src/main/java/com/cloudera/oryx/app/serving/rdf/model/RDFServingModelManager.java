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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.KeyMessage;
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

/**
 * A {@link com.cloudera.oryx.api.serving.ServingModelManager} that manages and provides access to an
 * {@link RDFServingModel} for the random decision forest Serving Layer application.
 */
public final class RDFServingModelManager extends AbstractServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(RDFServingModelManager.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final InputSchema inputSchema;
  private RDFServingModel model;

  public RDFServingModelManager(Config config) {
    super(config);
    inputSchema = new InputSchema(config);
  }

  /**
   * Called by the framework to initiate a continuous process of reading models, and reading
   * from the input topic and updating model state in memory, and issuing updates to the
   * update topic. This will be executed asynchronously and may block.
   *
   * @param updateIterator iterator to read models from
   * @param hadoopConf Hadoop context, which may be required for reading from HDFS
   * @throws IOException if an error occurs while reading updates
   */
  @Override
  public void consume(Iterator<KeyMessage<String, String>> updateIterator, Configuration hadoopConf)
      throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String, String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      Objects.requireNonNull(key, "Bad message: " + km);
      switch (key) {
        case "UP":
          if (model == null) {
            continue; // No model to interpret with yet, so skip it
          }

          DecisionForest forest = model.getForest();
          List<?> update = MAPPER.readValue(message, List.class);
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

          RDFPMMLUtils.validatePMMLVsSchema(pmml, inputSchema);
          Pair<DecisionForest,CategoricalValueEncodings> forestAndEncodings =
              RDFPMMLUtils.read(pmml);
          model = new RDFServingModel(forestAndEncodings.getFirst(),
                                      forestAndEncodings.getSecond(),
                                      inputSchema);
          log.info("New model: {}", model);
          break;

        default:
          throw new IllegalArgumentException("Bad message: " + km);
      }
    }
  }

  @Override
  public RDFServingModel getModel() {
    return model;
  }

}
