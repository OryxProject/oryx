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

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.dmg.pmml.PMML;

import com.cloudera.oryx.app.rdf.RDFPMMLUtils;
import com.cloudera.oryx.app.rdf.predict.CategoricalPrediction;
import com.cloudera.oryx.app.rdf.predict.NumericPrediction;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.rdf.tree.TerminalNode;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.serving.ServingModelManager;

public final class RDFServingModelManager implements ServingModelManager<String> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final InputSchema inputSchema;
  private RDFServingModel model;

  public RDFServingModelManager(Config config) {
    inputSchema = new InputSchema(config);
  }

  /**
   * Called by the framework to initiate a continuous process of reading models, and reading
   * from the input topic and updating model state in memory, and issuing updates to the
   * update topic. This will be executed asynchronously and may block.
   *
   * @param updateIterator iterator to read models from
   * @throws IOException if an error occurs while reading updates
   */
  @Override
  public void consume(Iterator<KeyMessage<String, String>> updateIterator) throws IOException {

    while (updateIterator.hasNext()) {
      KeyMessage<String, String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
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
            int encoding = Integer.parseInt(update.get(2).toString());
            int count = Integer.parseInt(update.get(3).toString());
            predictionToUpdate.update(encoding, count);
          } else {
            TerminalNode nodeToUpdate = (TerminalNode) forest.getTrees()[treeID].findByID(nodeID);
            NumericPrediction predictionToUpdate = (NumericPrediction) nodeToUpdate.getPrediction();
            double mean = Double.parseDouble(update.get(2).toString());
            int count = Integer.parseInt(update.get(3).toString());
            predictionToUpdate.update(mean, count);
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
          Pair<DecisionForest,CategoricalValueEncodings> forestAndEncodings =
              RDFPMMLUtils.read(pmml, inputSchema);
          model = new RDFServingModel(forestAndEncodings.getFirst(),
                                      forestAndEncodings.getSecond());
          break;

        default:
          throw new IllegalStateException("Unexpected key " + key);
      }
    }
  }

  @Override
  public RDFServingModel getModel() {
    return model;
  }

  @Override
  public void close() {
    // do nothing
  }

}
