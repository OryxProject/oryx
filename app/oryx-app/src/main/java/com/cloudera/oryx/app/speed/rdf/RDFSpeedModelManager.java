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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.util.concurrent.AtomicLongMap;
import com.typesafe.config.Config;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.speed.SpeedModelManager;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.rdf.RDFPMMLUtils;
import com.cloudera.oryx.app.rdf.ToExampleFn;
import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.Feature;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.rdf.tree.DecisionTree;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * Implementation of {@link SpeedModelManager} that maintains and updates a random decision
 * forest model in memory.
 */
public final class RDFSpeedModelManager implements SpeedModelManager<String,String,String> {

  private static final Logger log = LoggerFactory.getLogger(RDFSpeedModelManager.class);

  private final InputSchema inputSchema;
  private RDFSpeedModel model;

  public RDFSpeedModelManager(Config config) {
    inputSchema = new InputSchema(config);
  }

  @Override
  public void consume(Iterator<KeyMessage<String, String>> updateIterator, Configuration hadoopConf)
      throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      Objects.requireNonNull(key, "Bad message: " + km);
      switch (key) {
        case "UP":
          // Nothing to do; just hearing our own updates
          break;
        case "MODEL":
        case "MODEL-REF":
          log.info("Loading new model");
          PMML pmml = AppPMMLUtils.readPMMLFromUpdateKeyMessage(key, message, hadoopConf);
          RDFPMMLUtils.validatePMMLVsSchema(pmml, inputSchema);
          Pair<DecisionForest,CategoricalValueEncodings> forestAndEncodings =
              RDFPMMLUtils.read(pmml);
          model = new RDFSpeedModel(forestAndEncodings.getFirst(), forestAndEncodings.getSecond());
          log.info("New model loaded: {}", model);
          break;
        default:
          throw new IllegalArgumentException("Bad message: " + km);
      }
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    if (model == null) {
      return Collections.emptyList();
    }

    JavaRDD<Example> examplesRDD = newData.values().map(MLFunctions.PARSE_FN)
            .map(new ToExampleFn(inputSchema, model.getEncodings()));

    DecisionForest forest = model.getForest();
    JavaPairRDD<Pair<Integer,String>,Iterable<Feature>> targetsByTreeAndID =
        examplesRDD.flatMapToPair(new ToTreeNodeFeatureFn(forest)).groupByKey();

    List<String> updates = new ArrayList<>();

    if (inputSchema.isClassification()) {

      List<Tuple2<Pair<Integer,String>,Map<Integer,Long>>> countsByTreeAndID =
          targetsByTreeAndID.mapValues(new TargetCategoryCountFn()).collect();
      for (Tuple2<Pair<Integer,String>,Map<Integer,Long>> p : countsByTreeAndID) {
        Integer treeID = p._1().getFirst();
        String nodeID = p._1().getSecond();
        updates.add(TextUtils.joinJSON(Arrays.asList(treeID, nodeID, p._2())));
      }

    } else {

      List<Tuple2<Pair<Integer,String>,Mean>> meanTargetsByTreeAndID =
          targetsByTreeAndID.mapValues(new MeanNewTargetFn()).collect();
      for (Tuple2<Pair<Integer,String>,Mean> p : meanTargetsByTreeAndID) {
        Integer treeID = p._1().getFirst();
        String nodeID = p._1().getSecond();
        Mean mean = p._2();
        updates.add(TextUtils.joinJSON(Arrays.asList(
            treeID, nodeID, mean.getResult(), mean.getN())));
      }

    }

    return updates;
  }

  @Override
  public void close() {
    // do nothing
  }

  private static final class ToTreeNodeFeatureFn
      implements PairFlatMapFunction<Example,Pair<Integer,String>,Feature> {
    private final DecisionForest forest;
    ToTreeNodeFeatureFn(DecisionForest forest) {
      this.forest = forest;
    }
    @Override
    public Iterable<Tuple2<Pair<Integer,String>,Feature>> call(Example example) {
      Feature target = example.getTarget();
      DecisionTree[] trees = forest.getTrees();
      List<Tuple2<Pair<Integer,String>,Feature>> results = new ArrayList<>(trees.length);
      for (int treeID = 0; treeID < trees.length; treeID++) {
        String id = trees[treeID].findTerminal(example).getID();
        results.add(new Tuple2<>(new Pair<>(treeID, id), target));
      }
      return results;
    }
  }

  private static final class MeanNewTargetFn implements Function<Iterable<Feature>,Mean> {
    @Override
    public Mean call(Iterable<Feature> numericTargets) {
      Mean mean = new Mean();
      for (Feature f : numericTargets) {
        mean.increment(((NumericFeature) f).getValue());
      }
      return mean;
    }
  }

  private static final class TargetCategoryCountFn
      implements Function<Iterable<Feature>,Map<Integer,Long>> {
    @Override
    public Map<Integer,Long> call(Iterable<Feature> categoricalTargets) {
      AtomicLongMap<Integer> categoryCounts = AtomicLongMap.create();
      for (Feature f : categoricalTargets) {
        categoryCounts.incrementAndGet(((CategoricalFeature) f).getEncoding());
      }
      // Have to clone it as Kryo won't serialize the unmodifiable map
      return new HashMap<>(categoryCounts.asMap());
    }
  }

}
