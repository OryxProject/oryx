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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.speed.AbstractSpeedModelManager;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.kmeans.KMeansUtils;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * Implementation of {@link com.cloudera.oryx.api.speed.SpeedModelManager} that maintains and
 * updates an k-means clustering model in memory.
 */
public final class KMeansSpeedModelManager extends AbstractSpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(KMeansSpeedModelManager.class);

  private KMeansSpeedModel model;
  private final InputSchema inputSchema;

  public KMeansSpeedModelManager(Config config) {
    inputSchema = new InputSchema(config);
  }

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "UP":
        // do nothing, hearing our own updates
        break;
      case "MODEL":
      case "MODEL-REF":
        log.info("Loading new model");
        PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);
        if (pmml == null) {
          return;
        }

        KMeansPMMLUtils.validatePMMLVsSchema(pmml, inputSchema);
        model = new KMeansSpeedModel(KMeansPMMLUtils.read(pmml));
        log.info("New model loaded: {}", model);
        break;
      default:
        throw new IllegalArgumentException("Bad key: " + key);
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String, String> newData) {
    if (model == null) {
      return Collections.emptyList();
    }

    // Use locals to avoid capturing a reference to the Manager class
    KMeansSpeedModel model = this.model;
    InputSchema inputSchema = this.inputSchema;
    return newData.values().map(MLFunctions.PARSE_FN).mapToPair(data -> {
      try {
        double[] featureVector = KMeansUtils.featuresFromTokens(data, inputSchema);
        int closestClusterID = model.closestCluster(featureVector).getID();
        return new Tuple2<>(closestClusterID, new Tuple2<>(featureVector, 1L));
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", Arrays.toString(data));
        throw e;
      }
    }).reduceByKey((t1, t2) -> {
      double[] vec1 = t1._1();
      double[] vec2 = t2._1();
      // going to modify 1 in place
      for (int i = 0; i < vec1.length; i++) {
        vec1[i] += vec2[i];
      }
      return new Tuple2<>(vec1, t1._2() + t2._2());
    }).collect().stream().map(pair -> {
      int clusterID = pair._1();
      double[] vectorSum = pair._2()._1();
      long count = pair._2()._2();

      for (int i = 0 ; i < vectorSum.length; i++) {
        vectorSum[i] /= count;
      }

      ClusterInfo clusterInfo = model.getCluster(clusterID);
      clusterInfo.update(vectorSum, count);
      // Note: this is updating the local model copy so can't happen in Spark
      model.setCluster(clusterID, clusterInfo);
      return TextUtils.joinJSON(Arrays.asList(clusterID, clusterInfo.getCenter(), clusterInfo.getCount()));
    }).collect(Collectors.toList());
  }

}
