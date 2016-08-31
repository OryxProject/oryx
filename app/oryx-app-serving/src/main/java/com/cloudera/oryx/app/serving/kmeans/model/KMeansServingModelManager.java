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

package com.cloudera.oryx.app.serving.kmeans.model;

import java.io.IOException;
import java.util.List;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.serving.AbstractServingModelManager;
import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * A {@link com.cloudera.oryx.api.serving.ServingModelManager} that manages and provides access to an
 * {@link KMeansServingModel} for the k-means Serving Layer application.
 */
public final class KMeansServingModelManager extends AbstractServingModelManager<String> {

  private static final Logger log = LoggerFactory.getLogger(KMeansServingModelManager.class);

  private final InputSchema inputSchema;
  private KMeansServingModel model;

  public KMeansServingModelManager(Config config) {
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
        List<?> update = TextUtils.readJSON(message, List.class);
        // Update
        int id = Integer.parseInt(update.get(0).toString());
        double[] center = TextUtils.convertViaJSON(update.get(1), double[].class);
        long count = Long.parseLong(update.get(2).toString());
        model.update(id, center, count);
        break;

      case "MODEL":
      case "MODEL-REF":
        log.info("Loading new model");
        PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);
        if (pmml == null) {
          return;
        }

        KMeansPMMLUtils.validatePMMLVsSchema(pmml, inputSchema);
        List<ClusterInfo> clusters = KMeansPMMLUtils.read(pmml);
        model = new KMeansServingModel(clusters, inputSchema);
        log.info("New model: {}", model);
        break;

      default:
        throw new IllegalArgumentException("Bad key: " + key);
    }
  }

  @Override
  public KMeansServingModel getModel() {
    return model;
  }

}
