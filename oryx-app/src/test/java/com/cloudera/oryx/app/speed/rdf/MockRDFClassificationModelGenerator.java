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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.math3.random.RandomGenerator;
import org.dmg.pmml.Array;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.MissingValueStrategyType;
import org.dmg.pmml.Node;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;
import org.dmg.pmml.Value;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.kafka.util.DatumGenerator;

public final class MockRDFClassificationModelGenerator implements DatumGenerator<String,String> {

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    if (id == 0) {
      PMML pmml = buildModel();
      return new Pair<>("MODEL", PMMLUtils.toString(pmml));
    } else {
      String nodeID = "r" + ((id % 2 == 0) ? '-' : '+');
      Map<Integer,Integer> counts = new HashMap<>();
      counts.put(0, 1);
      counts.put(1, 2);
      return new Pair<>("UP", TextUtils.joinJSON(Arrays.asList(0, nodeID, counts)));
    }
  }

  private static PMML buildModel() {
    PMML pmml = PMMLUtils.buildSkeletonPMML();

    DataDictionary dataDictionary = new DataDictionary();
    dataDictionary.setNumberOfFields(2);
    DataField predictor = new DataField(new FieldName("color"), OpType.CATEGORICAL, DataType.STRING);
    predictor.getValues().add(new Value("yellow"));
    predictor.getValues().add(new Value("red"));
    dataDictionary.getDataFields().add(predictor);
    DataField target = new DataField(new FieldName("fruit"), OpType.CATEGORICAL, DataType.STRING);
    target.getValues().add(new Value("banana"));
    target.getValues().add(new Value("apple"));
    dataDictionary.getDataFields().add(target);
    pmml.setDataDictionary(dataDictionary);

    MiningSchema miningSchema = new MiningSchema();
    MiningField predictorMF = new MiningField(new FieldName("color"));
    predictorMF.setOptype(OpType.CATEGORICAL);
    predictorMF.setUsageType(FieldUsageType.ACTIVE);
    predictorMF.setImportance(0.5);
    miningSchema.getMiningFields().add(predictorMF);
    MiningField targetMF = new MiningField(new FieldName("fruit"));
    targetMF.setOptype(OpType.CATEGORICAL);
    targetMF.setUsageType(FieldUsageType.PREDICTED);
    miningSchema.getMiningFields().add(targetMF);

    Node rootNode = new Node();
    rootNode.setId("r");
    double dummyCount = 2.0;
    rootNode.setRecordCount(dummyCount);
    rootNode.setPredicate(new True());

    double halfCount = dummyCount / 2;

    Node left = new Node();
    left.setId("r-");
    left.setRecordCount(halfCount);
    left.setPredicate(new True());
    left.getScoreDistributions().add(new ScoreDistribution("apple", halfCount));
    Node right = new Node();
    right.setId("r+");
    right.setRecordCount(halfCount);
    right.setPredicate(new SimpleSetPredicate(new Array("red", Array.Type.STRING),
                                              new FieldName("color"),
                                              SimpleSetPredicate.BooleanOperator.IS_NOT_IN));
    right.getScoreDistributions().add(new ScoreDistribution("banana", halfCount));

    rootNode.getNodes().add(right);
    rootNode.getNodes().add(left);

    TreeModel treeModel =
        new TreeModel(miningSchema, rootNode, MiningFunctionType.CLASSIFICATION);
    treeModel.setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT);
    treeModel.setMissingValueStrategy(MissingValueStrategyType.DEFAULT_CHILD);
    treeModel.setMiningSchema(miningSchema);

    pmml.getModels().add(treeModel);

    return pmml;
  }

}
