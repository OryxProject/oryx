/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunction;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.True;
import org.dmg.pmml.mining.MiningModel;
import org.dmg.pmml.mining.Segment;
import org.dmg.pmml.mining.Segmentation;
import org.dmg.pmml.tree.Node;
import org.dmg.pmml.tree.TreeModel;

import com.cloudera.oryx.app.classreg.predict.CategoricalPrediction;
import com.cloudera.oryx.app.classreg.predict.NumericPrediction;
import com.cloudera.oryx.app.classreg.predict.Prediction;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.rdf.decision.CategoricalDecision;
import com.cloudera.oryx.app.rdf.decision.Decision;
import com.cloudera.oryx.app.rdf.decision.NumericDecision;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.rdf.tree.DecisionNode;
import com.cloudera.oryx.app.rdf.tree.DecisionTree;
import com.cloudera.oryx.app.rdf.tree.TerminalNode;
import com.cloudera.oryx.app.rdf.tree.TreeNode;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.text.TextUtils;

/**
 * Utility methods related to PMML serialization of random decision forest classification
 * and regression models.
 */
public final class RDFPMMLUtils {

  private RDFPMMLUtils() {}

  /**
   * Validates that the encoded PMML model received matches expected schema.
   *
   * @param pmml {@link PMML} encoding of a decision forest
   * @param schema expected schema attributes of decision forest
   */
  public static void validatePMMLVsSchema(PMML pmml, InputSchema schema) {
    List<Model> models = pmml.getModels();
    Preconditions.checkArgument(models.size() == 1,
                                "Should have exactly one model, but had %s", models.size());

    Model model = models.get(0);
    MiningFunction function = model.getMiningFunction();
    if (schema.isClassification()) {
      Preconditions.checkArgument(function == MiningFunction.CLASSIFICATION,
                                  "Expected classification function type but got %s",
                                  function);
    } else {
      Preconditions.checkArgument(function == MiningFunction.REGRESSION,
                                  "Expected regression function type but got %s",
                                  function);
    }

    DataDictionary dictionary = pmml.getDataDictionary();
    Preconditions.checkArgument(
        schema.getFeatureNames().equals(AppPMMLUtils.getFeatureNames(dictionary)),
        "Feature names in schema don't match names in PMML");

    MiningSchema miningSchema = model.getMiningSchema();
    Preconditions.checkArgument(schema.getFeatureNames().equals(
        AppPMMLUtils.getFeatureNames(miningSchema)));

    Integer pmmlIndex = AppPMMLUtils.findTargetIndex(miningSchema);
    if (schema.hasTarget()) {
      int schemaIndex = schema.getTargetFeatureIndex();
      Preconditions.checkArgument(
          pmmlIndex != null && schemaIndex == pmmlIndex,
          "Configured schema expects target at index %s, but PMML has target at index %s",
          schemaIndex, pmmlIndex);
    } else {
      Preconditions.checkArgument(pmmlIndex == null);
    }
  }

  /**
   * @param pmml PMML representation of decision forest
   * @return a {@link DecisionForest} representation of the PMML encoded model
   */
  public static Pair<DecisionForest,CategoricalValueEncodings> read(PMML pmml) {

    DataDictionary dictionary = pmml.getDataDictionary();
    List<String> featureNames = AppPMMLUtils.getFeatureNames(dictionary);
    CategoricalValueEncodings categoricalValueEncodings =
        AppPMMLUtils.buildCategoricalValueEncodings(dictionary);

    List<Model> models = pmml.getModels();
    Model model = models.get(0);
    MiningSchema miningSchema = model.getMiningSchema();
    int targetIndex = Objects.requireNonNull(AppPMMLUtils.findTargetIndex(miningSchema));

    DecisionTree[] trees;
    double[] weights;
    if (model instanceof MiningModel) {
      MiningModel miningModel = (MiningModel) model;
      Segmentation segmentation = miningModel.getSegmentation();
      Preconditions.checkArgument(
          segmentation.getMultipleModelMethod() == Segmentation.MultipleModelMethod.WEIGHTED_AVERAGE ||
          segmentation.getMultipleModelMethod() == Segmentation.MultipleModelMethod.WEIGHTED_MAJORITY_VOTE);
      List<Segment> segments = segmentation.getSegments();
      Preconditions.checkArgument(!segments.isEmpty());

      trees = new DecisionTree[segments.size()];
      weights = new double[trees.length];
      for (int i = 0; i < trees.length; i++) {
        Segment segment = segments.get(i);
        Preconditions.checkArgument(segment.getPredicate() instanceof True);
        weights[i] = segment.getWeight();
        TreeModel treeModel = (TreeModel) segment.getModel();
        TreeNode root = translateFromPMML(treeModel.getNode(),
                                          categoricalValueEncodings,
                                          featureNames,
                                          targetIndex);
        trees[i] = new DecisionTree(root);
      }
    } else {
      // Single tree model
      TreeNode root = translateFromPMML(((TreeModel) model).getNode(),
                                        categoricalValueEncodings,
                                        featureNames,
                                        targetIndex);
      trees = new DecisionTree[] { new DecisionTree(root) };
      weights = new double[] { 1.0 };
    }

    List<MiningField> miningFields = miningSchema.getMiningFields();
    double[] featureImportances = new double[featureNames.size()];
    for (int i = 0; i < miningFields.size(); i++) {
      MiningField field = miningFields.get(i);
      Double importance = field.getImportance();
      if (importance != null) {
        featureImportances[i] = importance;
      }
    }

    return new Pair<>(new DecisionForest(trees, weights, featureImportances),
                      categoricalValueEncodings);
  }

  private static TreeNode translateFromPMML(Node root,
                                            CategoricalValueEncodings categoricalValueEncodings,
                                            List<String> featureNames,
                                            int targetIndex) {


    String id = root.getId();
    List<Node> children = root.getNodes();
    if (children.isEmpty()) {
      // Terminal
      Collection<ScoreDistribution> scoreDistributions = root.getScoreDistributions();
      Prediction prediction;
      if (scoreDistributions != null && !scoreDistributions.isEmpty()) {
        // Categorical target
        Map<String,Integer> targetEncoding =
            categoricalValueEncodings.getValueEncodingMap(targetIndex);
        double[] categoryCounts = new double[targetEncoding.size()];
        for (ScoreDistribution dist : scoreDistributions) {
          int encoding = targetEncoding.get(dist.getValue());
          categoryCounts[encoding] = dist.getRecordCount();
        }
        prediction = new CategoricalPrediction(categoryCounts);
      } else {
        prediction = new NumericPrediction(Double.parseDouble(root.getScore()),
                                           (int) Math.round(root.getRecordCount()));
      }
      return new TerminalNode(id, prediction);
    }

    Preconditions.checkArgument(children.size() == 2);
    // Decision
    Node child1 = children.get(0);
    Node child2 = children.get(1);
    Node negativeLeftChild;
    Node positiveRightChild;
    if (child1.getPredicate() instanceof True) {
      negativeLeftChild = child1;
      positiveRightChild = child2;
    } else {
      Preconditions.checkArgument(child2.getPredicate() instanceof True);
      negativeLeftChild = child2;
      positiveRightChild = child1;
    }

    Decision decision;
    Predicate predicate = positiveRightChild.getPredicate();
    boolean defaultDecision = positiveRightChild.getId().equals(root.getDefaultChild());

    if (predicate instanceof SimplePredicate) {
      // Numeric decision
      SimplePredicate simplePredicate = (SimplePredicate) predicate;
      SimplePredicate.Operator operator = simplePredicate.getOperator();
      Preconditions.checkArgument(
          operator == SimplePredicate.Operator.GREATER_OR_EQUAL ||
          operator == SimplePredicate.Operator.GREATER_THAN);
      double threshold = Double.parseDouble(simplePredicate.getValue());
      // NumericDecision uses >= criteria. Increase threshold by one ulp to implement
      // "> threshold" as ">= (threshold + ulp)"
      if (operator == SimplePredicate.Operator.GREATER_THAN) {
        threshold += Math.ulp(threshold);
      }
      int featureNumber = featureNames.indexOf(simplePredicate.getField().getValue());
      decision = new NumericDecision(featureNumber, threshold, defaultDecision);

    } else {
      // Categorical decision
      Preconditions.checkArgument(predicate instanceof SimpleSetPredicate);
      SimpleSetPredicate simpleSetPredicate = (SimpleSetPredicate) predicate;
      SimpleSetPredicate.BooleanOperator operator = simpleSetPredicate.getBooleanOperator();
      Preconditions.checkArgument(
          operator == SimpleSetPredicate.BooleanOperator.IS_IN ||
              operator == SimpleSetPredicate.BooleanOperator.IS_NOT_IN);
      int featureNumber = featureNames.indexOf(simpleSetPredicate.getField().getValue());
      Map<String,Integer> valueEncodingMap =
          categoricalValueEncodings.getValueEncodingMap(featureNumber);
      String[] categories = TextUtils.parseDelimited(simpleSetPredicate.getArray().getValue(), ' ');
      BitSet activeCategories = new BitSet(valueEncodingMap.size());
      if (operator == SimpleSetPredicate.BooleanOperator.IS_IN) {
        for (String category : categories) {
          activeCategories.set(valueEncodingMap.get(category));
        }
      } else {
        // "not in"
        valueEncodingMap.values().forEach(activeCategories::set);
        for (String category : categories) {
          activeCategories.clear(valueEncodingMap.get(category));
        }
      }
      decision = new CategoricalDecision(featureNumber, activeCategories, defaultDecision);
    }

    return new DecisionNode(
        id,
        decision,
        translateFromPMML(negativeLeftChild,
                          categoricalValueEncodings,
                          featureNames,
                          targetIndex),
        translateFromPMML(positiveRightChild,
                          categoricalValueEncodings,
                          featureNames,
                          targetIndex));
  }

}
