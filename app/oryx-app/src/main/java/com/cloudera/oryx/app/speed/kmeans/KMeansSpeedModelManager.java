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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.speed.SpeedModelManager;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.kmeans.KMeansUtils;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * Implementation of {@link SpeedModelManager} that maintains and updates an k-means
 * clustering model in memory.
 */
public final class KMeansSpeedModelManager implements SpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(KMeansSpeedModelManager.class);

  private KMeansSpeedModel model;
  private final InputSchema inputSchema;

  public KMeansSpeedModelManager(Config config) {
    inputSchema = new InputSchema(config);
  }

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
          // do nothing, hearing our own updates
          break;
        case "MODEL":
        case "MODEL-REF":
          log.info("Loading new model");
          PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);
          KMeansPMMLUtils.validatePMMLVsSchema(pmml, inputSchema);
          model = new KMeansSpeedModel(KMeansPMMLUtils.read(pmml));
          log.info("New model loaded: {}", model);
          break;
        default:
          throw new IllegalArgumentException("Bad message: " + km);
      }
    }

  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String, String> newData) {
    if (model == null) {
      return Collections.emptyList();
    }

    List<Tuple2<Integer,Tuple2<double[],Long>>> updatedPoints =
        newData.values().map(MLFunctions.PARSE_FN)
            .mapToPair(new ToClusteredSums(inputSchema, model))
            .reduceByKey(new ReduceFeatureAndCountFn()).collect();

    List<String> updates = new ArrayList<>(updatedPoints.size());
    for (Tuple2<Integer,Tuple2<double[],Long>> pair : updatedPoints) {
      int clusterID = pair._1();
      double[] vectorSum = pair._2()._1();
      long count = pair._2()._2();

      for (int i = 0 ; i < vectorSum.length; i++) {
        vectorSum[i] /= count;
      }

      ClusterInfo clusterInfo = model.getCluster(clusterID);
      clusterInfo.update(vectorSum, count);
      model.setCluster(clusterID, clusterInfo);
      // add to updates
      updates.add(TextUtils.joinJSON(
          Arrays.asList(clusterID, clusterInfo.getCenter(), clusterInfo.getCount())));
    }

    return updates;
  }

  @Override
  public void close() {
    // do nothing
  }

  private static final class ToClusteredSums
      implements PairFunction<String[],Integer,Tuple2<double[],Long>> {

    private final InputSchema inputSchema;
    private final KMeansSpeedModel model;

    ToClusteredSums(InputSchema inputSchema, KMeansSpeedModel model) {
      this.inputSchema = inputSchema;
      // This assumes KMeansSpeedModel is smallish and trivial to serialize
      this.model = model;
    }

    @Override
    public Tuple2<Integer,Tuple2<double[],Long>> call(String[] data) {
      try {
        double[] featureVector = KMeansUtils.featuresFromTokens(data, inputSchema);
        int closestClusterID = model.closestCluster(featureVector).getID();
        return new Tuple2<>(closestClusterID, new Tuple2<>(featureVector, 1L));
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", Arrays.toString(data));
        throw e;
      }
    }
  }

  private static final class ReduceFeatureAndCountFn
      implements Function2<Tuple2<double[],Long>,Tuple2<double[],Long>,Tuple2<double[],Long>> {
    @Override
    public Tuple2<double[], Long> call(Tuple2<double[], Long> t1, Tuple2<double[], Long> t2) {
      double[] vec1 = t1._1();
      double[] vec2 = t2._1();
      // going to modify 1 in place
      for (int i = 0; i < vec1.length; i++) {
        vec1[i] += vec2[i];
      }
      return new Tuple2<>(vec1, t1._2() + t2._2());
    }
  }

}
