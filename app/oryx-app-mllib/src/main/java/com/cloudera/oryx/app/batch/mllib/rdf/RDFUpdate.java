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

package com.cloudera.oryx.app.batch.mllib.rdf;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Preconditions;
import com.koloboke.collect.map.IntLongMap;
import com.koloboke.collect.map.hash.HashIntLongMaps;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
import org.dmg.pmml.MiningFunction;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import com.cloudera.oryx.app.classreg.example.Example;
import com.cloudera.oryx.app.classreg.example.ExampleUtils;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.rdf.RDFPMMLUtils;
import com.cloudera.oryx.app.rdf.tree.DecisionForest;
import com.cloudera.oryx.app.schema.CategoricalValueEncodings;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.param.HyperParamValues;
import com.cloudera.oryx.ml.param.HyperParams;

/**
 * Update function that builds and evaluates random decision forest models in the Batch Layer.
 */
public final class RDFUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(RDFUpdate.class);

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
    Preconditions.checkArgument(maxSplitCandidates >= 2,
                                "max-split-candidates must be at least 2");
    Preconditions.checkArgument(maxDepth > 0,
                                "max-depth must be at least 1");

    JavaRDD<String[]> parsedRDD = trainData.map(MLFunctions.PARSE_FN);
    CategoricalValueEncodings categoricalValueEncodings =
        new CategoricalValueEncodings(getDistinctValues(parsedRDD));
    JavaRDD<LabeledPoint> trainPointData =
        parseToLabeledPointRDD(parsedRDD, categoricalValueEncodings);

    Map<Integer,Integer> categoryInfo = categoricalValueEncodings.getCategoryCounts();
    categoryInfo.remove(inputSchema.getTargetFeatureIndex()); // Don't specify target count
    // Need to translate indices to predictor indices
    Map<Integer,Integer> categoryInfoByPredictor = new HashMap<>(categoryInfo.size());
    categoryInfo.forEach((k, v) -> categoryInfoByPredictor.put(inputSchema.featureToPredictorIndex(k), v));

    int seed = RandomManager.getRandom().nextInt();

    RandomForestModel model;
    if (inputSchema.isClassification()) {
      int numTargetClasses =
          categoricalValueEncodings.getValueCount(inputSchema.getTargetFeatureIndex());
      model = RandomForest.trainClassifier(trainPointData,
                                           numTargetClasses,
                                           categoryInfoByPredictor,
                                           numTrees,
                                           "auto",
                                           impurity,
                                           maxDepth,
                                           maxSplitCandidates,
                                           seed);
    } else {
      model = RandomForest.trainRegressor(trainPointData,
                                          categoryInfoByPredictor,
                                          numTrees,
                                          "auto",
                                          impurity,
                                          maxDepth,
                                          maxSplitCandidates,
                                          seed);
    }

    List<Map<Integer,Long>> treeNodeIDCounts = treeNodeExampleCounts(trainPointData, model);
    Map<Integer,Long> predictorIndexCounts = predictorExampleCounts(trainPointData, model);

    return rdfModelToPMML(model,
                          categoricalValueEncodings,
                          maxDepth,
                          maxSplitCandidates,
                          impurity,
                          treeNodeIDCounts,
                          predictorIndexCounts);
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData,
                         JavaRDD<String> trainData) {
    RDFPMMLUtils.validatePMMLVsSchema(model, inputSchema);
    Pair<DecisionForest,CategoricalValueEncodings> forestAndEncoding = RDFPMMLUtils.read(model);
    DecisionForest forest = forestAndEncoding.getFirst();
    CategoricalValueEncodings valueEncodings = forestAndEncoding.getSecond();

    InputSchema inputSchema = this.inputSchema;
    JavaRDD<Example> examplesRDD = testData.map(MLFunctions.PARSE_FN).
        map(data -> ExampleUtils.dataToExample(data, inputSchema, valueEncodings));

    double eval;
    if (inputSchema.isClassification()) {
      double accuracy = Evaluation.accuracy(forest, examplesRDD);
      log.info("Accuracy: {}", accuracy);
      eval = accuracy;
    } else {
      double rmse = Evaluation.rmse(forest, examplesRDD);
      log.info("RMSE: {}", rmse);
      eval = -rmse;
    }
    return eval;
  }

  private Map<Integer,Collection<String>> getDistinctValues(JavaRDD<String[]> parsedRDD) {
    int[] categoricalIndices = IntStream.range(0, inputSchema.getNumFeatures()).
        filter(inputSchema::isCategorical).toArray();

    return parsedRDD.mapPartitions(data -> {
        Map<Integer,Collection<String>> categoryValues = new HashMap<>();
        for (int i : categoricalIndices) {
          categoryValues.put(i, new HashSet<>());
        }
        data.forEachRemaining(datum ->
          categoryValues.forEach((category, values) -> values.add(datum[category]))
        );
        return Collections.singleton(categoryValues).iterator();
      }).reduce((v1, v2) -> {
        // Assumes both have the same key set
        v1.forEach((category, values) -> values.addAll(v2.get(category)));
        return v1;
      });
  }


  private JavaRDD<LabeledPoint> parseToLabeledPointRDD(
      JavaRDD<String[]> parsedRDD,
      CategoricalValueEncodings categoricalValueEncodings) {

    return parsedRDD.map(data -> {
      try {
        double[] features = new double[inputSchema.getNumPredictors()];
        double target = Double.NaN;
        for (int featureIndex = 0; featureIndex < data.length; featureIndex++) {
          double encoded;
          if (inputSchema.isNumeric(featureIndex)) {
            encoded = Double.parseDouble(data[featureIndex]);
          } else if (inputSchema.isCategorical(featureIndex)) {
            Map<String,Integer> valueEncoding =
                categoricalValueEncodings.getValueEncodingMap(featureIndex);
            encoded = valueEncoding.get(data[featureIndex]);
          } else {
            continue;
          }
          if (inputSchema.isTarget(featureIndex)) {
            target = encoded;
          } else {
            features[inputSchema.featureToPredictorIndex(featureIndex)] = encoded;
          }
        }
        Preconditions.checkState(!Double.isNaN(target));
        return new LabeledPoint(target, Vectors.dense(features));
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", Arrays.toString(data));
        throw e;
      }
    });
  }

  /**
   * @param trainPointData data to run down trees
   * @param model random decision forest model to count on
   * @return maps of node IDs to the count of training examples that reached that node, one
   *  per tree in the model
   * @see #predictorExampleCounts(JavaRDD,RandomForestModel)
   */
  private static List<Map<Integer,Long>> treeNodeExampleCounts(JavaRDD<LabeledPoint> trainPointData,
                                                               RandomForestModel model) {
    return trainPointData.mapPartitions(data -> {
        DecisionTreeModel[] trees = model.trees();
        List<IntLongMap> treeNodeIDCounts = IntStream.range(0, trees.length).
            mapToObj(i -> HashIntLongMaps.newMutableMap()).collect(Collectors.toList());
        data.forEachRemaining(datum -> {
          double[] featureVector = datum.features().toArray();
          for (int i = 0; i < trees.length; i++) {
            DecisionTreeModel tree = trees[i];
            IntLongMap nodeIDCount = treeNodeIDCounts.get(i);
            org.apache.spark.mllib.tree.model.Node node = tree.topNode();
            // This logic cloned from Node.predict:
            while (!node.isLeaf()) {
              // Count node ID
              nodeIDCount.addValue(node.id(), 1);
              Split split = node.split().get();
              int featureIndex = split.feature();
              node = nextNode(featureVector, node, split, featureIndex);
            }
            nodeIDCount.addValue(node.id(), 1);
          }
        });
        return Collections.<List<Map<Integer,Long>>>singleton(
            treeNodeIDCounts.stream().map(HashMap::new).collect(Collectors.toList())).iterator();
      }
    ).reduce((a, b) -> {
        Preconditions.checkArgument(a.size() == b.size());
        for (int i = 0; i < a.size(); i++) {
          merge(a.get(i), b.get(i));
        }
        return a;
      });
  }

  /**
   * @param trainPointData data to run down trees
   * @param model random decision forest model to count on
   * @return map of predictor index to the number of training examples that reached a
   *  node whose decision is based on that feature. The index is among predictors, not all
   *  features, since there are fewer predictors than features. That is, the index will
   *  match the one used in the {@link RandomForestModel}.
   */
  private static Map<Integer,Long> predictorExampleCounts(JavaRDD<LabeledPoint> trainPointData,
                                                          RandomForestModel model) {
    return trainPointData.mapPartitions(data -> {
        IntLongMap featureIndexCount = HashIntLongMaps.newMutableMap();
        data.forEachRemaining(datum -> {
          double[] featureVector = datum.features().toArray();
          for (DecisionTreeModel tree : model.trees()) {
            org.apache.spark.mllib.tree.model.Node node = tree.topNode();
            // This logic cloned from Node.predict:
            while (!node.isLeaf()) {
              Split split = node.split().get();
              int featureIndex = split.feature();
              // Count feature
              featureIndexCount.addValue(featureIndex, 1);
              node = nextNode(featureVector, node, split, featureIndex);
            }
          }
        });
        // Clone to avoid problem with Kryo serializing Koloboke
        return Collections.<Map<Integer,Long>>singleton(
            new HashMap<>(featureIndexCount)).iterator();
    }).reduce(RDFUpdate::merge);
  }

  private static org.apache.spark.mllib.tree.model.Node nextNode(
      double[] featureVector,
      org.apache.spark.mllib.tree.model.Node node,
      Split split,
      int featureIndex) {
    double featureValue = featureVector[featureIndex];
    if (split.featureType().equals(FeatureType.Continuous())) {
      if (featureValue <= split.threshold()) {
        return node.leftNode().get();
      } else {
        return node.rightNode().get();
      }
    } else {
      if (split.categories().contains(featureValue)) {
        return node.leftNode().get();
      } else {
        return node.rightNode().get();
      }
    }
  }

  private static <T> Map<T,Long> merge(Map<T,Long> a, Map<T,Long> b) {
    if (b.size() > a.size()) {
      return merge(b, a);
    }
    b.forEach((key, value) -> a.merge(key, value, (x, y) -> x + y));
    return a;
  }

  private static <T> long get(Map<T,Long> map, T key) {
    Long count = map.get(key);
    return count == null ? 0L : count;
  }

  private PMML rdfModelToPMML(RandomForestModel rfModel,
                              CategoricalValueEncodings categoricalValueEncodings,
                              int maxDepth,
                              int maxSplitCandidates,
                              String impurity,
                              List<Map<Integer,Long>> nodeIDCounts,
                              Map<Integer,Long> predictorIndexCounts) {

    boolean classificationTask = rfModel.algo().equals(Algo.Classification());
    Preconditions.checkState(classificationTask == inputSchema.isClassification());

    DecisionTreeModel[] trees = rfModel.trees();

    Model model;
    if (trees.length == 1) {
      model = toTreeModel(trees[0], categoricalValueEncodings, nodeIDCounts.get(0));
    } else {
      MiningModel miningModel = new MiningModel();
      model = miningModel;
      Segmentation.MultipleModelMethod multipleModelMethodType = classificationTask ?
          Segmentation.MultipleModelMethod.WEIGHTED_MAJORITY_VOTE :
          Segmentation.MultipleModelMethod.WEIGHTED_AVERAGE;
      List<Segment> segments = new ArrayList<>(trees.length);
      for (int treeID = 0; treeID < trees.length; treeID++) {
        TreeModel treeModel =
            toTreeModel(trees[treeID], categoricalValueEncodings, nodeIDCounts.get(treeID));
        segments.add(new Segment()
             .setId(Integer.toString(treeID))
             .setPredicate(new True())
             .setModel(treeModel)
             .setWeight(1.0)); // No weights in MLlib impl now
      }
      miningModel.setSegmentation(new Segmentation(multipleModelMethodType, segments));
    }

    model.setMiningFunction(classificationTask ?
                            MiningFunction.CLASSIFICATION :
                            MiningFunction.REGRESSION);

    double[] importances = countsToImportances(predictorIndexCounts);
    model.setMiningSchema(AppPMMLUtils.buildMiningSchema(inputSchema, importances));
    DataDictionary dictionary =
        AppPMMLUtils.buildDataDictionary(inputSchema, categoricalValueEncodings);

    PMML pmml = PMMLUtils.buildSkeletonPMML();
    pmml.setDataDictionary(dictionary);
    pmml.addModels(model);

    AppPMMLUtils.addExtension(pmml, "maxDepth", maxDepth);
    AppPMMLUtils.addExtension(pmml, "maxSplitCandidates", maxSplitCandidates);
    AppPMMLUtils.addExtension(pmml, "impurity", impurity);

    return pmml;
  }

  private TreeModel toTreeModel(DecisionTreeModel dtModel,
                                CategoricalValueEncodings categoricalValueEncodings,
                                Map<Integer,Long> nodeIDCounts) {

    boolean classificationTask = dtModel.algo().equals(Algo.Classification());
    Preconditions.checkState(classificationTask == inputSchema.isClassification());

    Node root = new Node();
    root.setId("r");

    Queue<Node> modelNodes = new ArrayDeque<>();
    modelNodes.add(root);

    Queue<Pair<org.apache.spark.mllib.tree.model.Node,Split>> treeNodes = new ArrayDeque<>();
    treeNodes.add(new Pair<>(dtModel.topNode(), null));

    while (!treeNodes.isEmpty()) {

      Pair<org.apache.spark.mllib.tree.model.Node,Split> treeNodePredicate = treeNodes.remove();
      Node modelNode = modelNodes.remove();

      // This is the decision that got us here from the parent, if any;
      // not the predicate at this node
      Predicate predicate = buildPredicate(treeNodePredicate.getSecond(),
                                           categoricalValueEncodings);
      modelNode.setPredicate(predicate);

      org.apache.spark.mllib.tree.model.Node treeNode = treeNodePredicate.getFirst();
      long nodeCount = get(nodeIDCounts, treeNode.id());
      modelNode.setRecordCount((double) nodeCount);

      if (treeNode.isLeaf()) {

        Predict prediction = treeNode.predict();
        int targetEncodedValue = (int) prediction.predict();
        if (classificationTask) {
          Map<Integer,String> targetEncodingToValue =
              categoricalValueEncodings.getEncodingValueMap(inputSchema.getTargetFeatureIndex());
          double predictedProbability = prediction.prob();
          Preconditions.checkState(predictedProbability >= 0.0 && predictedProbability <= 1.0);
          // Not sure how nodeCount == 0 can happen but it does in the MLlib model
          long effectiveNodeCount = Math.max(1, nodeCount);
          // Problem: MLlib only gives a predicted class and its probability, and no distribution
          // over the rest. Infer that the rest of the probability is evenly distributed.
          double restProbability = (1.0 - predictedProbability) / (targetEncodingToValue.size() - 1);

          targetEncodingToValue.forEach((encodedValue, value) -> {
            double probability = encodedValue == targetEncodedValue ? predictedProbability : restProbability;
            // Yes, recordCount may be fractional; it's a relative indicator
            double recordCount = probability * effectiveNodeCount;
            if (recordCount > 0.0) {
              ScoreDistribution distribution = new ScoreDistribution(value, recordCount);
              // Not "confident" enough in the "probability" to call it one
              distribution.setConfidence(probability);
              modelNode.addScoreDistributions(distribution);
            }
          });
        } else {
          modelNode.setScore(Double.toString(targetEncodedValue));
        }

      } else {

        Split split = treeNode.split().get();

        Node positiveModelNode = new Node().setId(modelNode.getId() + '+');
        Node negativeModelNode = new Node().setId(modelNode.getId() + '-');
        modelNode.addNodes(positiveModelNode, negativeModelNode);

        org.apache.spark.mllib.tree.model.Node rightTreeNode = treeNode.rightNode().get();
        org.apache.spark.mllib.tree.model.Node leftTreeNode = treeNode.leftNode().get();

        boolean defaultRight = get(nodeIDCounts, rightTreeNode.id()) > get(nodeIDCounts, leftTreeNode.id());
        modelNode.setDefaultChild(defaultRight ? positiveModelNode.getId() : negativeModelNode.getId());

        // Right node is "positive", so carries the predicate. It must evaluate first
        // and therefore come first in the tree
        modelNodes.add(positiveModelNode);
        modelNodes.add(negativeModelNode);
        treeNodes.add(new Pair<>(rightTreeNode, split));
        treeNodes.add(new Pair<>(leftTreeNode, null));

      }

    }

    return new TreeModel()
        .setNode(root)
        .setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT)
        .setMissingValueStrategy(TreeModel.MissingValueStrategy.DEFAULT_CHILD);
  }

  private Predicate buildPredicate(Split split,
                                   CategoricalValueEncodings categoricalValueEncodings) {
    if (split == null) {
      // Left child always applies, but is evaluated second
      return new True();
    }

    int featureIndex = inputSchema.predictorToFeatureIndex(split.feature());
    FieldName fieldName = FieldName.create(inputSchema.getFeatureNames().get(featureIndex));

    if (split.featureType().equals(FeatureType.Categorical())) {
      // Note that categories in MLlib model select the *left* child but the
      // convention here will be that the predicate selects the *right* child
      // So the predicate will evaluate "not in" this set
      // More ugly casting
      @SuppressWarnings("unchecked")
      List<Double> javaCategories = (List<Double>) (List<?>)
          JavaConversions.seqAsJavaList(split.categories());
      Set<Integer> negativeEncodings = javaCategories.stream().map(Double::intValue).collect(Collectors.toSet());

      Map<Integer,String> encodingToValue =
          categoricalValueEncodings.getEncodingValueMap(featureIndex);
      List<String> negativeValues = negativeEncodings.stream().map(encodingToValue::get).collect(Collectors.toList());

      String joinedValues = TextUtils.joinPMMLDelimited(negativeValues);
      return new SimpleSetPredicate(fieldName,
                                    SimpleSetPredicate.BooleanOperator.IS_NOT_IN,
                                    new Array(Array.Type.STRING, joinedValues));

    } else {
      // For MLlib, left means <= threshold, so right means >
      return new SimplePredicate(fieldName, SimplePredicate.Operator.GREATER_THAN)
          .setValue(Double.toString(split.threshold()));
    }
  }

  private double[] countsToImportances(Map<Integer,Long> predictorIndexCounts) {
    double[] importances = new double[inputSchema.getNumPredictors()];
    long total = predictorIndexCounts.values().stream().collect(Collectors.summingLong(l -> l));
    Preconditions.checkArgument(total > 0);
    predictorIndexCounts.forEach((k, count) -> importances[k] = (double) count / total);
    return importances;
  }

}