/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.speed.rdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.speed.AbstractSpeedModelManager;
import com.cloudera.oryx.app.classreg.example.CategoricalFeature;
import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.example.ExampleUtils;
import com.cloudera.oryx.app.classreg.example.Feature;
import com.cloudera.oryx.app.classreg.example.NumericFeature;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.rdf.RDFPMMLUtils;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.rdf.tree.DecisionTree;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * Implementation of {@link com.cloudera.oryx.api.speed.SpeedModelManager} that maintains and
 * updates a random decision forest model in memory.
 */
public final class RDFSpeedModelManager extends AbstractSpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(RDFSpeedModelManager.class);

  private final InputSchema inputSchema;
  private RDFSpeedModel model;

  public RDFSpeedModelManager(Config config) {
    inputSchema = new InputSchema(config);
  }

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "UP":
        // Nothing to do; just hearing our own updates
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
        model = new RDFSpeedModel(forestAndEncodings.getFirst(), forestAndEncodings.getSecond());
        log.info("New model loaded: {}", model);
        break;
      default:
        throw new IllegalArgumentException("Bad key: " + key);
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    if (model == null) {
      return Collections.emptyList();
    }

    InputSchema inputSchema = this.inputSchema;
    CategoricalValueEncodings valueEncodings = model.getEncodings();
    JavaRDD<Example> examplesRDD = newData.values().map(MLFunctions.PARSE_FN).
        map(data -> ExampleUtils.dataToExample(data, inputSchema, valueEncodings));

    DecisionForest forest = model.getForest();
    JavaPairRDD<Pair<Integer,String>,Iterable<Feature>> targetsByTreeAndID =
        examplesRDD.flatMapToPair(example -> {
          Feature target = example.getTarget();
          DecisionTree[] trees = forest.getTrees();
          List<Tuple2<Pair<Integer,String>,Feature>> results = new ArrayList<>(trees.length);
          for (int treeID = 0; treeID < trees.length; treeID++) {
            String id = trees[treeID].findTerminal(example).getID();
            results.add(new Tuple2<>(new Pair<>(treeID, id), target));
          }
          return results.iterator();
        }).groupByKey();


    if (inputSchema.isClassification()) {

      return targetsByTreeAndID.mapValues(categoricalTargets ->
        StreamSupport.stream(categoricalTargets.spliterator(), false)
            .collect(Collectors.groupingBy(f -> ((CategoricalFeature) f).getEncoding(), Collectors.counting()))
      ).collect().stream().map(p -> {
        // This happens on the driver since the call below uses Jackson, and we have a version
        // conflict with Spark. Or did.
        Integer treeID = p._1().getFirst();
        String nodeID = p._1().getSecond();
        return TextUtils.joinJSON(Arrays.asList(treeID, nodeID, p._2()));
      }).collect(Collectors.toList());

    } else {

      return targetsByTreeAndID.mapValues(numericTargets ->
        StreamSupport.stream(numericTargets.spliterator(), false)
            .collect(Collectors.summarizingDouble(f -> ((NumericFeature) f).getValue()))
      ).collect().stream().map(p -> {
        // This happens on the driver since the call below uses Jackson, and we have a version
        // conflict with Spark. Or did.
        Integer treeID = p._1().getFirst();
        String nodeID = p._1().getSecond();
        DoubleSummaryStatistics stats = p._2();
        return TextUtils.joinJSON(Arrays.asList(treeID, nodeID, stats.getAverage(), stats.getCount()));
      }).collect(Collectors.toList());

    }

  }

}
