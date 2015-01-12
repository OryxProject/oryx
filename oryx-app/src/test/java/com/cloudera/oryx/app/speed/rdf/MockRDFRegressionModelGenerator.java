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

import org.apache.commons.math3.random.RandomGenerator;
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
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;

import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.kafka.util.DatumGenerator;

final class MockRDFRegressionModelGenerator implements DatumGenerator<String,String> {

  @Override
  public Pair<String,String> generate(int id, RandomGenerator random) {
    PMML pmml = PMMLUtils.buildSkeletonPMML();
    double dummyCount = 2.0 * (id + 1);

    DataDictionary dataDictionary = new DataDictionary();
    dataDictionary.setNumberOfFields(2);
    DataField predictor = new DataField(new FieldName("foo"), OpType.CONTINUOUS, DataType.DOUBLE);
    dataDictionary.getDataFields().add(predictor);
    DataField target = new DataField(new FieldName("bar"), OpType.CONTINUOUS, DataType.DOUBLE);
    dataDictionary.getDataFields().add(target);
    pmml.setDataDictionary(dataDictionary);

    MiningSchema miningSchema = new MiningSchema();
    MiningField predictorMF = new MiningField(new FieldName("foo"));
    predictorMF.setOptype(OpType.CONTINUOUS);
    predictorMF.setUsageType(FieldUsageType.ACTIVE);
    miningSchema.getMiningFields().add(predictorMF);
    MiningField targetMF = new MiningField(new FieldName("bar"));
    targetMF.setOptype(OpType.CONTINUOUS);
    targetMF.setUsageType(FieldUsageType.PREDICTED);
    miningSchema.getMiningFields().add(targetMF);

    Node rootNode = new Node();
    rootNode.setId("r");
    rootNode.setRecordCount(dummyCount);
    rootNode.setPredicate(new True());

    double halfCount = dummyCount / 2;

    Node left = new Node();
    left.setId("r-");
    left.setRecordCount(halfCount);
    left.setPredicate(new True());
    left.setScore("-2.0");
    Node right = new Node();
    right.setId("r+");
    right.setRecordCount(halfCount);
    SimplePredicate predicate =
        new SimplePredicate(new FieldName("foo"), SimplePredicate.Operator.GREATER_THAN);
    predicate.setValue("3.14");
    right.setPredicate(predicate);
    right.setScore("2.0");

    rootNode.getNodes().add(right);
    rootNode.getNodes().add(left);

    TreeModel treeModel =
        new TreeModel(miningSchema, rootNode, MiningFunctionType.REGRESSION);
    treeModel.setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT);
    treeModel.setMissingValueStrategy(MissingValueStrategyType.DEFAULT_CHILD);
    treeModel.setMiningSchema(miningSchema);

    pmml.getModels().add(treeModel);

    return new Pair<>("MODEL", PMMLUtils.toString(pmml));
  }

}
