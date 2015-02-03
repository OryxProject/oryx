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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.kmeans.SquaredDistanceFn;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.text.TextUtils;
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
          // do nothing, hearing our own updates
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
  public Iterable<String> buildUpdates(JavaPairRDD<String, String> newData) {
    if (model == null) {
      return Collections.emptyList();
    }

    List<String> updates = new ArrayList<>();

    List<Tuple2<Integer,Pair<double[],Long>>> clusteredPointsList =
        newData.values().map(MLFunctions.PARSE_FN)
            .mapToPair(new ToClosestCluster(inputSchema, model.getClusters()))
            .reduceByKey(new ToAvgSumAndCount())
            .collect();

    for (Tuple2<Integer,Pair<double[],Long>> clusteredEntry : clusteredPointsList) {
      int clusterID = clusteredEntry._1();
      double[] vectorSum = clusteredEntry._2().getFirst();
      Long count = clusteredEntry._2().getSecond();

      for (int i = 0; i < vectorSum.length; i++) {
        vectorSum[i] /= count;
      }

      ClusterInfo clusterInfo = model.getClusters().get(clusterID);
      clusterInfo.update(vectorSum, count);
      model.update(clusterID, clusterInfo);

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

  private static class ToClosestCluster implements PairFunction<String[],Integer,Pair<double[],Long>> {
    private final InputSchema inputSchema;
    private final List<ClusterInfo> clusters;

    ToClosestCluster(InputSchema inputSchema, List<ClusterInfo> clusters) {
      this.inputSchema = inputSchema;
      this.clusters = clusters;
    }

    @Override
    public Tuple2<Integer,Pair<double[],Long>> call(String[] data) {
      double[] features = new double[inputSchema.getNumPredictors()];
      for (int featureIndex = 0; featureIndex < data.length; featureIndex++) {
        if (inputSchema.isActive(featureIndex)) {
          features[inputSchema.featureToPredictorIndex(featureIndex)] =
              Double.parseDouble(data[featureIndex]);
        }
      }

      double minDistance = Double.POSITIVE_INFINITY;
      int bestIndex = -1;

      SquaredDistanceFn sqDist = new SquaredDistanceFn();
      for (ClusterInfo cluster : clusters) {
        double distance = sqDist.distance(cluster.getCenter(), features);
        if (distance < minDistance) {
          minDistance = distance;
          bestIndex = cluster.getID();
        }
      }
      return new Tuple2<>(bestIndex, new Pair<>(features, 1L));
    }
  }

  private static class ToAvgSumAndCount implements Function2<Pair<double[],Long>,Pair<double[],Long>,
                                                             Pair<double[],Long>> {

    @Override
    public Pair<double[],Long> call(Pair<double[],Long> p1, Pair<double[],Long> p2) {
      double[] v1 = p1.getFirst();
      double[] v2 = p2.getFirst();

      for (int i = 0; i < v1.length; i++) {
        v1[i] += v2[i];
      }

      return new Pair<>(v1, p1.getSecond() + p2.getSecond());
    }
  }

}
