/*
 * Copyright (c) 2015, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.speed.kmeans;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.speed.SpeedModelManager;

public final class KMeansSpeedModelManager implements SpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(KMeansSpeedModelManager.class);

  private KMeansSpeedModel model;
  private final InputSchema inputSchema;

  public KMeansSpeedModelManager(Config config) {
    inputSchema = new InputSchema(config);
  }

  @Override
  public void consume(Iterator<KeyMessage<String, String>> updateIterator) throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String, String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      switch (key) {
        case "UP":
          // do nothing;
          break;
        case "MODEL":
          // New model
          PMML pmml;
          try {
            pmml = PMMLUtils.fromString(message);
          } catch (JAXBException e) {
            throw new IOException(e);
          }
          KMeansPMMLUtils.validatePMMLVsSchema(pmml, inputSchema);
          model = new KMeansSpeedModel(KMeansPMMLUtils.read(pmml));
          log.info("Model with {} clusters", model.getClusters().size());
          break;
        default:
          throw new IllegalStateException("Unexpected key " + key);
      }
    }

  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String, String> newData) throws IOException {
    if (model == null) {
      return Collections.emptyList();
    }

    return null;
  }

  @Override
  public void close() {
    // do nothing
  }
}
