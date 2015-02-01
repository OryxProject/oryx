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

package com.cloudera.oryx.app.rdf;

import java.util.HashMap;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.Array;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.MissingValueStrategyType;
import org.dmg.pmml.MultipleModelMethodType;
import org.dmg.pmml.Node;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.Segment;
import org.dmg.pmml.Segmentation;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;
import org.dmg.pmml.Value;
import org.junit.Test;

import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.OryxTest;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;

public final class RDFPMMLUtilsTest extends OryxTest {

  @Test
  public void testValidateClassification() {
    PMML pmml = buildDummyClassificationModel();
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.feature-names", "[\"color\",\"fruit\"]");
    overlayConfig.put("oryx.input-schema.numeric-features", "[]");
    overlayConfig.put("oryx.input-schema.target-feature", "fruit");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema schema = new InputSchema(config);
    RDFPMMLUtils.validatePMMLVsSchema(pmml, schema);
  }

  @Test
  public void testValidateRegression() {
    PMML pmml = buildDummyRegressionModel();
    Map<String,Object> overlayConfig = new HashMap<>();
    overlayConfig.put("oryx.input-schema.feature-names", "[\"foo\",\"bar\"]");
    overlayConfig.put("oryx.input-schema.categorical-features", "[]");
    overlayConfig.put("oryx.input-schema.target-feature", "bar");
    Config config = ConfigUtils.overlayOn(overlayConfig, ConfigUtils.getDefault());
    InputSchema schema = new InputSchema(config);
    RDFPMMLUtils.validatePMMLVsSchema(pmml, schema);
  }

  @Test
  public void testReadClassification() {
    PMML pmml = buildDummyClassificationModel();
    Pair<DecisionForest,CategoricalValueEncodings> forestAndEncodings = RDFPMMLUtils.read(pmml);
    DecisionForest forest = forestAndEncodings.getFirst();
    assertEquals(1, forest.getTrees().length);
    assertArrayEquals(new double[] { 1.0 }, forest.getWeights());
    assertArrayEquals(new double[] { 0.5, 0.0 }, forest.getFeatureImportances());
    CategoricalValueEncodings encodings = forestAndEncodings.getSecond();
    assertEquals(2, encodings.getValueCount(0));
    assertEquals(2, encodings.getValueCount(1));
  }

  @Test
  public void testReadRegression() {
    PMML pmml = buildDummyRegressionModel();
    Pair<DecisionForest,CategoricalValueEncodings> forestAndEncodings = RDFPMMLUtils.read(pmml);
    DecisionForest forest = forestAndEncodings.getFirst();
    assertEquals(1, forest.getTrees().length);
    assertArrayEquals(new double[] { 1.0 }, forest.getWeights());
    assertArrayEquals(new double[] { 0.5, 0.0 }, forest.getFeatureImportances());
    CategoricalValueEncodings encodings = forestAndEncodings.getSecond();
    assertTrue(encodings.getCategoryCounts().isEmpty());
  }

  @Test
  public void testReadClassificationForest() {
    PMML pmml = buildDummyClassificationModel(3);
    Pair<DecisionForest,CategoricalValueEncodings> forestAndEncodings = RDFPMMLUtils.read(pmml);
    DecisionForest forest = forestAndEncodings.getFirst();
    assertEquals(3, forest.getTrees().length);
  }

  public static PMML buildDummyClassificationModel() {
    return buildDummyClassificationModel(1);
  }

  public static PMML buildDummyClassificationModel(int numTrees) {
    PMML pmml = PMMLUtils.buildSkeletonPMML();

    DataDictionary dataDictionary = new DataDictionary();
    dataDictionary.setNumberOfFields(2);
    DataField predictor =
        new DataField(FieldName.create("color"), OpType.CATEGORICAL, DataType.STRING);
    predictor.getValues().add(new Value("yellow"));
    predictor.getValues().add(new Value("red"));
    dataDictionary.getDataFields().add(predictor);
    DataField target =
        new DataField(FieldName.create("fruit"), OpType.CATEGORICAL, DataType.STRING);
    target.getValues().add(new Value("banana"));
    target.getValues().add(new Value("apple"));
    dataDictionary.getDataFields().add(target);
    pmml.setDataDictionary(dataDictionary);

    MiningSchema miningSchema = new MiningSchema();
    MiningField predictorMF = new MiningField(FieldName.create("color"));
    predictorMF.setOptype(OpType.CATEGORICAL);
    predictorMF.setUsageType(FieldUsageType.ACTIVE);
    predictorMF.setImportance(0.5);
    miningSchema.getMiningFields().add(predictorMF);
    MiningField targetMF = new MiningField(FieldName.create("fruit"));
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
                                              FieldName.create("color"),
                                              SimpleSetPredicate.BooleanOperator.IS_NOT_IN));
    right.getScoreDistributions().add(new ScoreDistribution("banana", halfCount));

    rootNode.getNodes().add(right);
    rootNode.getNodes().add(left);

    TreeModel treeModel =
        new TreeModel(miningSchema, rootNode, MiningFunctionType.CLASSIFICATION);
    treeModel.setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT);
    treeModel.setMissingValueStrategy(MissingValueStrategyType.DEFAULT_CHILD);

    if (numTrees > 1) {
      MiningModel miningModel = new MiningModel(miningSchema, MiningFunctionType.CLASSIFICATION);
      Segmentation segmentation = new Segmentation(MultipleModelMethodType.WEIGHTED_MAJORITY_VOTE);
      for (int i = 0; i < numTrees; i++) {
        Segment segment = new Segment();
        segment.setId(Integer.toString(i));
        segment.setPredicate(new True());
        segment.setModel(treeModel);
        segment.setWeight(1.0);
        segmentation.getSegments().add(segment);
      }
      miningModel.setSegmentation(segmentation);
      pmml.getModels().add(miningModel);
    } else {
      pmml.getModels().add(treeModel);
    }

    return pmml;
  }

  public static PMML buildDummyRegressionModel() {
    PMML pmml = PMMLUtils.buildSkeletonPMML();

    DataDictionary dataDictionary = new DataDictionary();
    dataDictionary.setNumberOfFields(2);
    DataField predictor =
        new DataField(FieldName.create("foo"), OpType.CONTINUOUS, DataType.DOUBLE);
    dataDictionary.getDataFields().add(predictor);
    DataField target =
        new DataField(FieldName.create("bar"), OpType.CONTINUOUS, DataType.DOUBLE);
    dataDictionary.getDataFields().add(target);
    pmml.setDataDictionary(dataDictionary);

    MiningSchema miningSchema = new MiningSchema();
    MiningField predictorMF = new MiningField(FieldName.create("foo"));
    predictorMF.setOptype(OpType.CONTINUOUS);
    predictorMF.setUsageType(FieldUsageType.ACTIVE);
    predictorMF.setImportance(0.5);
    miningSchema.getMiningFields().add(predictorMF);
    MiningField targetMF = new MiningField(FieldName.create("bar"));
    targetMF.setOptype(OpType.CONTINUOUS);
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
    left.setScore("-2.0");
    Node right = new Node();
    right.setId("r+");
    right.setRecordCount(halfCount);
    SimplePredicate predicate =
        new SimplePredicate(FieldName.create("foo"), SimplePredicate.Operator.GREATER_THAN);
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

    return pmml;
  }

}
