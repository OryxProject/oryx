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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import org.dmg.pmml.Array;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunction;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.OpType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.True;
import org.dmg.pmml.Value;
import org.dmg.pmml.mining.MiningModel;
import org.dmg.pmml.mining.Segment;
import org.dmg.pmml.mining.Segmentation;
import org.dmg.pmml.tree.Node;
import org.dmg.pmml.tree.TreeModel;
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
    CategoricalValueEncodings encodings = forestAndEncodings.getSecond();
    assertEquals(0, encodings.getCategoryCounts().size());
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

  private static PMML buildDummyClassificationModel(int numTrees) {
    PMML pmml = PMMLUtils.buildSkeletonPMML();

    List<DataField> dataFields = new ArrayList<>();
    DataField predictor =
        new DataField(FieldName.create("color"), OpType.CATEGORICAL, DataType.STRING);
    predictor.addValues(new Value("yellow"), new Value("red"));
    dataFields.add(predictor);
    DataField target =
        new DataField(FieldName.create("fruit"), OpType.CATEGORICAL, DataType.STRING);
    target.addValues(new Value("banana"), new Value("apple"));
    dataFields.add(target);
    DataDictionary dataDictionary =
        new DataDictionary(dataFields).setNumberOfFields(dataFields.size());
    pmml.setDataDictionary(dataDictionary);

    List<MiningField> miningFields = new ArrayList<>();
    MiningField predictorMF = new MiningField(FieldName.create("color"))
        .setOpType(OpType.CATEGORICAL)
        .setUsageType(MiningField.UsageType.ACTIVE)
        .setImportance(0.5);
    miningFields.add(predictorMF);
    MiningField targetMF = new MiningField(FieldName.create("fruit"))
        .setOpType(OpType.CATEGORICAL)
        .setUsageType(MiningField.UsageType.PREDICTED);
    miningFields.add(targetMF);
    MiningSchema miningSchema = new MiningSchema(miningFields);

    double dummyCount = 2.0;
    Node rootNode = new Node().setId("r").setRecordCount(dummyCount).setPredicate(new True());

    double halfCount = dummyCount / 2;

    Node left = new Node().setId("r-").setRecordCount(halfCount).setPredicate(new True());
    left.addScoreDistributions(new ScoreDistribution("apple", halfCount));
    Node right = new Node().setId("r+").setRecordCount(halfCount)
        .setPredicate(new SimpleSetPredicate(FieldName.create("color"),
                                             SimpleSetPredicate.BooleanOperator.IS_NOT_IN,
                                             new Array(Array.Type.STRING, "red")));
    right.addScoreDistributions(new ScoreDistribution("banana", halfCount));

    rootNode.addNodes(right, left);

    TreeModel treeModel = new TreeModel(MiningFunction.CLASSIFICATION, miningSchema, rootNode)
        .setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT)
        .setMissingValueStrategy(TreeModel.MissingValueStrategy.DEFAULT_CHILD);

    if (numTrees > 1) {
      MiningModel miningModel = new MiningModel(MiningFunction.CLASSIFICATION, miningSchema);
      List<Segment> segments = new ArrayList<>();
      for (int i = 0; i < numTrees; i++) {
        segments.add(new Segment()
            .setId(Integer.toString(i))
            .setPredicate(new True())
            .setModel(treeModel)
            .setWeight(1.0));
      }
      miningModel.setSegmentation(
          new Segmentation(Segmentation.MultipleModelMethod.WEIGHTED_MAJORITY_VOTE, segments));
      pmml.addModels(miningModel);
    } else {
      pmml.addModels(treeModel);
    }

    return pmml;
  }

  public static PMML buildDummyRegressionModel() {
    PMML pmml = PMMLUtils.buildSkeletonPMML();

    List<DataField> dataFields = new ArrayList<>();
    dataFields.add(new DataField(FieldName.create("foo"), OpType.CONTINUOUS, DataType.DOUBLE));
    dataFields.add(new DataField(FieldName.create("bar"), OpType.CONTINUOUS, DataType.DOUBLE));
    DataDictionary dataDictionary =
        new DataDictionary(dataFields).setNumberOfFields(dataFields.size());
    pmml.setDataDictionary(dataDictionary);

    List<MiningField> miningFields = new ArrayList<>();
    MiningField predictorMF = new MiningField(FieldName.create("foo"))
        .setOpType(OpType.CONTINUOUS)
        .setUsageType(MiningField.UsageType.ACTIVE)
        .setImportance(0.5);
    miningFields.add(predictorMF);
    MiningField targetMF = new MiningField(FieldName.create("bar"))
        .setOpType(OpType.CONTINUOUS)
        .setUsageType(MiningField.UsageType.PREDICTED);
    miningFields.add(targetMF);
    MiningSchema miningSchema = new MiningSchema(miningFields);

    double dummyCount = 2.0;
    Node rootNode = new Node().setId("r").setRecordCount(dummyCount).setPredicate(new True());

    double halfCount = dummyCount / 2;

    Node left = new Node()
        .setId("r-")
        .setRecordCount(halfCount)
        .setPredicate(new True())
        .setScore("-2.0");
    Node right = new Node().setId("r+").setRecordCount(halfCount)
        .setPredicate(new SimplePredicate(FieldName.create("foo"),
                                          SimplePredicate.Operator.GREATER_THAN).setValue("3.14"))
        .setScore("2.0");

    rootNode.addNodes(right, left);

    TreeModel treeModel = new TreeModel(MiningFunction.REGRESSION, miningSchema, rootNode)
        .setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT)
        .setMissingValueStrategy(TreeModel.MissingValueStrategy.DEFAULT_CHILD)
        .setMiningSchema(miningSchema);

    pmml.addModels(treeModel);

    return pmml;
  }

}
