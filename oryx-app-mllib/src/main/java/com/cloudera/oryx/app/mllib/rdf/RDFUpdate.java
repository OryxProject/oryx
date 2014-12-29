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

package com.cloudera.oryx.app.mllib.rdf;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.configuration.Algo;
import org.apache.spark.mllib.tree.configuration.FeatureType;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.mllib.tree.model.Predict;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.tree.model.Split;
import org.dmg.pmml.Array;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.MissingValueStrategyType;
import org.dmg.pmml.Model;
import org.dmg.pmml.MultipleModelMethodType;
import org.dmg.pmml.Node;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.Segment;
import org.dmg.pmml.Segmentation;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;
import scala.collection.JavaConversions;

import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.param.HyperParamValues;
import com.cloudera.oryx.ml.param.HyperParams;

public final class RDFUpdate extends MLUpdate<String> {

  private final int numTrees;
  private final List<HyperParamValues<?>> hyperParamValues;
  private final InputSchema inputSchema;

  public RDFUpdate(Config config) {
    super(config);
    numTrees = config.getInt("oryx.rdf.num-trees");
    Preconditions.checkArgument(numTrees >= 1);
    hyperParamValues = Arrays.asList(
        HyperParams.fromConfig(config, "oryx.rdf.hyperparams.max-split-candidates"),
        HyperParams.fromConfig(config, "oryx.rdf.hyperparams.max-depth"),
        HyperParams.fromConfig(config, "oryx.rdf.hyperparams.impurity"));

    inputSchema = new InputSchema(config);
    Preconditions.checkArgument(inputSchema.hasTarget());
  }

  @Override
  public List<HyperParamValues<?>> getHyperParameterValues() {
    return hyperParamValues;
  }

  @Override
  public PMML buildModel(JavaSparkContext sparkContext,
                         JavaRDD<String> trainData,
                         List<?> hyperParameters,
                         Path candidatePath) {

    int maxSplitCandidates = (Integer) hyperParameters.get(0);
    int maxDepth = (Integer) hyperParameters.get(1);
    String impurity = (String) hyperParameters.get(2);
    Preconditions.checkArgument(maxSplitCandidates > 0);
    Preconditions.checkArgument(maxDepth > 0);

    JavaRDD<String[]> parsedRDD = trainData.map(MLFunctions.PARSE_FN);
    Map<Integer,BiMap<String,Double>> distinctValueMaps = getDistinctValueMap(parsedRDD);
    JavaRDD<LabeledPoint> trainPointData = parsedToRatingRDD(parsedRDD, distinctValueMaps);

    Map<Integer,Integer> categoryInfo = new HashMap<>();
    for (Map.Entry<Integer,BiMap<String,Double>> e : distinctValueMaps.entrySet()) {
      int featureIndex = e.getKey();
      if (!inputSchema.isTarget(featureIndex)) {
        categoryInfo.put(featureIndex, e.getValue().size());
      }
    }

    int seed = RandomManager.getRandom().nextInt();

    RandomForestModel model;
    if (inputSchema.isClassification()) {
      int numTargetClasses = distinctValueMaps.get(inputSchema.getTargetFeatureIndex()).size();
      model = RandomForest.trainClassifier(trainPointData,
                                           numTargetClasses,
                                           categoryInfo,
                                           numTrees,
                                           "auto",
                                           impurity,
                                           maxDepth,
                                           maxSplitCandidates,
                                           seed);
    } else {
      model = RandomForest.trainRegressor(trainPointData,
                                          categoryInfo,
                                          numTrees,
                                          "auto",
                                          impurity,
                                          maxDepth,
                                          maxSplitCandidates,
                                          seed);
    }
    return rdfModelToPMML(model, distinctValueMaps, maxDepth, maxSplitCandidates, impurity);
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData) {
    return 0;
  }

  private Map<Integer,BiMap<String,Double>> getDistinctValueMap(JavaRDD<String[]> parsedRDD) {
    Map<Integer,Set<String>> distinctValues = getDistinctValues(parsedRDD);
    Map<Integer,BiMap<String,Double>> distinctValueMaps = new HashMap<>(distinctValues.size());
    for (Map.Entry<Integer,Set<String>> e : distinctValues.entrySet()) {
      distinctValueMaps.put(e.getKey(), mapDistinctValues(e.getValue()));
    }
    return distinctValueMaps;
  }

  private Map<Integer,Set<String>> getDistinctValues(JavaRDD<String[]> parsedRDD) {
    final List<Integer> categoricalIndices = new ArrayList<>();
    for (int i = 0; i < inputSchema.getNumFeatures(); i++) {
      if (inputSchema.isCategorical(i)) {
        categoricalIndices.add(i);
      }
    }

    JavaRDD<Map<Integer,Set<String>>> distinctValuesByPartition = parsedRDD.mapPartitions(
        new FlatMapFunction<Iterator<String[]>, Map<Integer,Set<String>>>() {
          @Override
          public Iterable<Map<Integer,Set<String>>> call(Iterator<String[]> data) {
            Map<Integer,Set<String>> distinctCategoricalValues = new HashMap<>();
            for (int i : categoricalIndices) {
              distinctCategoricalValues.put(i, new HashSet<String>());
            }
            while (data.hasNext()) {
              String[] datum = data.next();
              for (Map.Entry<Integer,Set<String>> e : distinctCategoricalValues.entrySet()) {
                e.getValue().add(datum[e.getKey()]);
              }
            }
            return Collections.singletonList(distinctCategoricalValues);
          }
        });

    return distinctValuesByPartition.reduce(
        new Function2<Map<Integer,Set<String>>, Map<Integer,Set<String>>, Map<Integer,Set<String>>>() {
          @Override
          public Map<Integer,Set<String>> call(Map<Integer,Set<String>> v1,
                                               Map<Integer,Set<String>> v2) {
            for (Map.Entry<Integer,Set<String>> e : v1.entrySet()) {
              e.getValue().addAll(v2.get(e.getKey()));
            }
            return v1;
          }
        });
  }

  private static <T extends Comparable<T>> BiMap<T,Double> mapDistinctValues(Collection<T> distinct) {
    List<T> sortedDistinct = new ArrayList<>(distinct);
    Collections.sort(sortedDistinct);
    BiMap<T,Double> mapping = HashBiMap.create();
    for (int i = 0; i < sortedDistinct.size(); i++) {
      mapping.put(sortedDistinct.get(i), (double) i);
    }
    return mapping;
  }


  private JavaRDD<LabeledPoint> parsedToRatingRDD(
      JavaRDD<String[]> parsedRDD,
      final Map<Integer,BiMap<String,Double>> distinctValueMaps) {

    return parsedRDD.map(new Function<String[],LabeledPoint>() {
      @Override
      public LabeledPoint call(String[] data) {
        double[] features = new double[data.length - 1]; // - 1 because one of them is the target
        double target = Double.NaN;
        int offset = 0;
        for (int i = 0; i < data.length; i++) {
          Map<String,Double> mapping = distinctValueMaps.get(i);
          double encoded;
          if (mapping == null) { // Numeric feature
            encoded = Double.parseDouble(data[i]);
          } else { // Categorical feature
            encoded = mapping.get(data[i]);
          }
          if (inputSchema.isTarget(i)) {
            target = encoded;
            offset = 1; // cause the rest of vector to be moved up one relative to data
          } else {
            features[i - offset] = encoded;
          }
        }
        Preconditions.checkState(!Double.isNaN(target));
        return new LabeledPoint(target, Vectors.dense(features));
      }
    });
  }

  private PMML rdfModelToPMML(RandomForestModel rfModel,
                              Map<Integer,BiMap<String,Double>> distinctValueMaps,
                              int maxDepth,
                              int maxSplitCandidates,
                              String impurity) {

    boolean classificationTask = rfModel.algo().equals(Algo.Classification());
    Preconditions.checkState(classificationTask == inputSchema.isClassification());

    DecisionTreeModel[] trees = rfModel.trees();

    Model model;
    if (trees.length == 1) {
      model = toTreeModel(trees[0], distinctValueMaps);
    } else {
      MiningModel miningModel = new MiningModel();
      model = miningModel;
      // MLlib doesn't support confidences, otherwise these could be WEIGHTED_...
      MultipleModelMethodType multipleModelMethodType = classificationTask ?
          MultipleModelMethodType.MAJORITY_VOTE :
          MultipleModelMethodType.AVERAGE;
      Segmentation segmentation = new Segmentation(multipleModelMethodType);
      miningModel.setSegmentation(segmentation);
      for (int treeID = 0; treeID < trees.length; treeID++) {
        TreeModel treeModel = toTreeModel(trees[treeID], distinctValueMaps);
        Segment segment = new Segment();
        segment.setId(Integer.toString(treeID));
        segment.setPredicate(new True());
        segment.setModel(treeModel);
        //segment.setWeight(1.0); // No weights in MLlib impl now
        segmentation.getSegments().add(segment);
      }
    }

    model.setFunctionName(classificationTask ?
                          MiningFunctionType.CLASSIFICATION :
                          MiningFunctionType.REGRESSION);
    model.setMiningSchema(AppPMMLUtils.buildMiningSchema(inputSchema));
    DataDictionary dictionary = AppPMMLUtils.buildDataDictionary(inputSchema, distinctValueMaps);

    PMML pmml = PMMLUtils.buildSkeletonPMML();
    pmml.setDataDictionary(dictionary);
    pmml.getModels().add(model);

    AppPMMLUtils.addExtension(pmml, "maxDepth", maxDepth);
    AppPMMLUtils.addExtension(pmml, "maxSplitCandidates", maxSplitCandidates);
    AppPMMLUtils.addExtension(pmml, "impurity", impurity);

    return pmml;
  }

  private TreeModel toTreeModel(DecisionTreeModel dtModel,
                                Map<Integer,BiMap<String,Double>> distinctValueMaps) {

    List<String> featureNames = inputSchema.getFeatureNames();
    boolean classificationTask = dtModel.algo().equals(Algo.Classification());
    Preconditions.checkState(classificationTask == inputSchema.isClassification());

    Map<Double,String> targetEncodingToValue =
        distinctValueMaps.get(inputSchema.getTargetFeatureIndex()).inverse();

    Node root = new Node();
    root.setId("r");

    Queue<Node> modelNodes = new ArrayDeque<>();
    modelNodes.add(root);

    Queue<Pair<org.apache.spark.mllib.tree.model.Node,Split>> treeNodes = new ArrayDeque<>();
    treeNodes.add(new Pair<>(dtModel.topNode(), (Split) null));

    while (!treeNodes.isEmpty()) {

      Pair<org.apache.spark.mllib.tree.model.Node,Split> treeNodePredicate = treeNodes.remove();
      Node modelNode = modelNodes.remove();

      // This is the decision that got us here from the parent, if any;
      // not the predicate at this node
      Predicate predicate = buildPredicate(treeNodePredicate.getSecond(),
                                           featureNames,
                                           distinctValueMaps);
      modelNode.setPredicate(predicate);

      org.apache.spark.mllib.tree.model.Node treeNode = treeNodePredicate.getFirst();
      if (treeNode.isLeaf()) { // Hacky conversion from Scala

        // modelNode.setRecordCount(...); // Unsupported in MLlib
        Predict prediction = treeNode.predict();
        double targetEncodedValue = prediction.predict();
        if (classificationTask) {
          String predictedCategoricalValue = targetEncodingToValue.get(targetEncodedValue);
          double confidence = prediction.prob();
          // Not really correct to pass confidence as record count but it's required
          // and we have nothing else
          ScoreDistribution distribution =
              new ScoreDistribution(predictedCategoricalValue, confidence);
          distribution.setConfidence(confidence);
          modelNode.getScoreDistributions().add(distribution);
        } else {
          modelNode.setScore(Double.toString(targetEncodedValue));
        }

      } else {

        Split split = treeNode.split().get();

        Node positiveModelNode = new Node();
        positiveModelNode.setId(modelNode.getId() + '+');
        modelNode.getNodes().add(positiveModelNode);
        Node negativeModelNode = new Node();
        negativeModelNode.setId(modelNode.getId() + '-');
        modelNode.getNodes().add(negativeModelNode);
        // Unsupported in MLlib
        //modelNode.setDefaultChild(
        //  decision.getDefaultDecision() ? positiveModelNode.getId() : negativeModelNode.getId());

        // Right node is "positive", so carries the predicate. It must evaluate first
        // and therefore come first in the tree
        modelNodes.add(positiveModelNode);
        modelNodes.add(negativeModelNode);
        treeNodes.add(new Pair<>(treeNode.rightNode().get(), split));
        treeNodes.add(new Pair<>(treeNode.leftNode().get(), (Split) null));

      }

    }

    TreeModel treeModel = new TreeModel();
    treeModel.setNode(root);
    treeModel.setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT);
    // Unsupported in MLlib
    // treeModel.setMissingValueStrategy(MissingValueStrategyType.DEFAULT_CHILD);
    treeModel.setMissingValueStrategy(MissingValueStrategyType.NONE);
    return treeModel;
  }

  private static Predicate buildPredicate(Split split,
                                          List<String> featureNames,
                                          Map<Integer,BiMap<String,Double>> distinctValueMaps) {
    if (split == null) {
      // Left child always applies, but is evaluated second
      return new True();
    }

    int featureIndex = split.feature();
    FieldName fieldName = new FieldName(featureNames.get(featureIndex));

    if (split.featureType().equals(FeatureType.Categorical())) {
      // Note that categories in MLlib model select the *left* child but the
      // convention here will be that the predicate selects the *right* child
      // So the predicate will evaluate "not in" this set
      // More ugly casting
      @SuppressWarnings("unchecked")
      List<Double> javaCategories = (List<Double>) (List<?>)
          JavaConversions.seqAsJavaList(split.categories());
      Set<Double> negativeEncodings = new HashSet<>(javaCategories);

      Map<Double,String> encodingToValue = distinctValueMaps.get(featureIndex).inverse();
      List<String> negativeValues = new ArrayList<>();
      for (Double negativeEncoding : negativeEncodings) {
        negativeValues.add(encodingToValue.get(negativeEncoding));
      }

      String joinedValues = TextUtils.joinDelimited(negativeValues, ' ');
      return new SimpleSetPredicate(new Array(joinedValues, Array.Type.STRING),
                                    fieldName,
                                    SimpleSetPredicate.BooleanOperator.IS_NOT_IN);

    } else {
      // For MLlib, left means <= threshold, so right means >
      SimplePredicate numericPredicate =
          new SimplePredicate(fieldName, SimplePredicate.Operator.GREATER_THAN);
      numericPredicate.setValue(Double.toString(split.threshold()));
      return numericPredicate;
    }
  }

}