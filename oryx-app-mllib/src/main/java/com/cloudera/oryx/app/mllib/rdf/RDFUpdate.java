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
import com.google.common.util.concurrent.AtomicLongMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.rdf.RDFPMMLUtils;
import com.cloudera.oryx.app.rdf.example.CategoricalFeature;
import com.cloudera.oryx.app.rdf.example.Example;
import com.cloudera.oryx.app.rdf.example.Feature;
import com.cloudera.oryx.app.rdf.example.NumericFeature;
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
    for (Map.Entry<Integer,Integer> e : categoryInfo.entrySet()) {
      categoryInfoByPredictor.put(inputSchema.featureToPredictorIndex(e.getKey()),
                                  e.getValue());
    }

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
                         JavaRDD<String> testData) {
    Pair<DecisionForest,CategoricalValueEncodings> forestAndEncoding =
        RDFPMMLUtils.read(model, inputSchema);
    DecisionForest forest = forestAndEncoding.getFirst();
    final CategoricalValueEncodings valueEncodings = forestAndEncoding.getSecond();

    JavaRDD<Example> examplesRDD = testData.map(MLFunctions.PARSE_FN).map(
        new Function<String[],Example>() {
          @Override
          public Example call(String[] data) {
            Feature[] features = new Feature[data.length];
            Feature target = null;
            for (int featureIndex = 0; featureIndex < data.length; featureIndex++) {
              Feature feature = null;
              if (inputSchema.isNumeric(featureIndex)) {
                feature = NumericFeature.forValue(Double.parseDouble(data[featureIndex]));
              } else if (inputSchema.isCategorical(featureIndex)) {
                int encoding = valueEncodings.getValueEncodingMap(featureIndex)
                    .get(data[featureIndex]);
                feature = CategoricalFeature.forEncoding(encoding);
              }
              features[featureIndex] = feature;
              if (inputSchema.isTarget(featureIndex)) {
                target = feature;
              }
            }
            return new Example(target, features);
          }
        });

    double eval;
    if (inputSchema.isClassification()) {
      double accuracy = Evaluation.accuracy(forest, examplesRDD);
      log.info("Accuracy: {}", accuracy);
      eval = accuracy;
    } else {
      double rmse = Evaluation.rmse(forest, examplesRDD);
      log.info("RMSE: {}", rmse);
      eval = 1.0 / rmse;
    }
    return eval;
  }

  private Map<Integer,Collection<String>> getDistinctValues(JavaRDD<String[]> parsedRDD) {
    final List<Integer> categoricalIndices = new ArrayList<>();
    for (int i = 0; i < inputSchema.getNumFeatures(); i++) {
      if (inputSchema.isCategorical(i)) {
        categoricalIndices.add(i);
      }
    }

    JavaRDD<Map<Integer,Collection<String>>> distinctValuesByPartition = parsedRDD.mapPartitions(
        new FlatMapFunction<Iterator<String[]>, Map<Integer,Collection<String>>>() {
          @Override
          public Iterable<Map<Integer,Collection<String>>> call(Iterator<String[]> data) {
            Map<Integer,Collection<String>> distinctCategoricalValues = new HashMap<>();
            for (int i : categoricalIndices) {
              distinctCategoricalValues.put(i, new HashSet<String>());
            }
            while (data.hasNext()) {
              String[] datum = data.next();
              for (Map.Entry<Integer,Collection<String>> e : distinctCategoricalValues.entrySet()) {
                e.getValue().add(datum[e.getKey()]);
              }
            }
            return Collections.singletonList(distinctCategoricalValues);
          }
        });

    return distinctValuesByPartition.reduce(
        new Function2<Map<Integer,Collection<String>>,
                      Map<Integer,Collection<String>>,
                      Map<Integer,Collection<String>>>() {
          @Override
          public Map<Integer,Collection<String>> call(Map<Integer,Collection<String>> v1,
                                                      Map<Integer,Collection<String>> v2) {
            for (Map.Entry<Integer,Collection<String>> e : v1.entrySet()) {
              e.getValue().addAll(v2.get(e.getKey()));
            }
            return v1;
          }
        });
  }


  private JavaRDD<LabeledPoint> parseToLabeledPointRDD(
      JavaRDD<String[]> parsedRDD,
      final CategoricalValueEncodings categoricalValueEncodings) {

    return parsedRDD.map(new Function<String[],LabeledPoint>() {
      @Override
      public LabeledPoint call(String[] data) {
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
                                                               final RandomForestModel model) {
    List<AtomicLongMap<Integer>> maps = trainPointData.mapPartitions(
        new FlatMapFunction<Iterator<LabeledPoint>,List<AtomicLongMap<Integer>>>() {
          @Override
          public Iterable<List<AtomicLongMap<Integer>>> call(Iterator<LabeledPoint> data) {
            DecisionTreeModel[] trees = model.trees();
            int numTrees = trees.length;
            List<AtomicLongMap<Integer>> treeNodeIDCounts = new ArrayList<>(numTrees);
            for (int i = 0; i < numTrees; i++) {
              treeNodeIDCounts.add(AtomicLongMap.<Integer>create());
            }
            while (data.hasNext()) {
              LabeledPoint datum = data.next();
              double[] featureVector = datum.features().toArray();
              for (int i = 0; i < trees.length; i++) {
                DecisionTreeModel tree = trees[i];
                AtomicLongMap<Integer> nodeIDCount = treeNodeIDCounts.get(i);
                org.apache.spark.mllib.tree.model.Node node = tree.topNode();
                // This logic cloned from Node.predict:
                while (!node.isLeaf()) {
                  // Count node ID
                  nodeIDCount.incrementAndGet(node.id());
                  Split split = node.split().get();
                  int featureIndex = split.feature();
                  double featureValue = featureVector[featureIndex];
                  if (split.featureType().equals(FeatureType.Continuous())) {
                    if (featureValue <= split.threshold()) {
                      node = node.leftNode().get();
                    } else {
                      node = node.rightNode().get();
                    }
                  } else {
                    if (split.categories().contains(featureValue)) {
                      node = node.leftNode().get();
                    } else {
                      node = node.rightNode().get();
                    }
                  }
                }
                nodeIDCount.incrementAndGet(node.id());
              }
            }
            return Collections.singleton(treeNodeIDCounts);
          }
        }
      ).reduce(
        new Function2<List<AtomicLongMap<Integer>>,
                      List<AtomicLongMap<Integer>>,
                      List<AtomicLongMap<Integer>>>() {
          @Override
          public List<AtomicLongMap<Integer>> call(List<AtomicLongMap<Integer>> a,
                                                   List<AtomicLongMap<Integer>> b) {
            Preconditions.checkArgument(a.size() == b.size());
            for (int i = 0; i < a.size(); i++) {
              merge(a.get(i), b.get(i));
            }
            return a;
          }
        }
      );

    List<Map<Integer,Long>> result = new ArrayList<>(maps.size());
    for (AtomicLongMap<Integer> map : maps) {
      result.add(map.asMap());
    }
    return result;
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
                                                          final RandomForestModel model) {
    return trainPointData.mapPartitions(
        new FlatMapFunction<Iterator<LabeledPoint>,AtomicLongMap<Integer>>() {
          @Override
          public Iterable<AtomicLongMap<Integer>> call(Iterator<LabeledPoint> data) {
            AtomicLongMap<Integer> featureIndexCount = AtomicLongMap.create();
            while (data.hasNext()) {
              LabeledPoint datum = data.next();
              double[] featureVector = datum.features().toArray();
              for (DecisionTreeModel tree : model.trees()) {
                org.apache.spark.mllib.tree.model.Node node = tree.topNode();
                // This logic cloned from Node.predict:
                while (!node.isLeaf()) {
                  Split split = node.split().get();
                  int featureIndex = split.feature();
                  // Count feature
                  featureIndexCount.incrementAndGet(featureIndex);
                  double featureValue = featureVector[featureIndex];
                  if (split.featureType().equals(FeatureType.Continuous())) {
                    if (featureValue <= split.threshold()) {
                      node = node.leftNode().get();
                    } else {
                      node = node.rightNode().get();
                    }
                  } else {
                    if (split.categories().contains(featureValue)) {
                      node = node.leftNode().get();
                    } else {
                      node = node.rightNode().get();
                    }
                  }
                }
              }
            }
            return Collections.singleton(featureIndexCount);
          }
        }
      ).reduce(
        new Function2<AtomicLongMap<Integer>,AtomicLongMap<Integer>,AtomicLongMap<Integer>>() {
          @Override
          public AtomicLongMap<Integer> call(AtomicLongMap<Integer> a, AtomicLongMap<Integer> b) {
            return merge(a, b);
          }
        }
      ).asMap();
  }

  private static AtomicLongMap<Integer> merge(AtomicLongMap<Integer> a, AtomicLongMap<Integer> b) {
    for (Map.Entry<Integer,Long> e : b.asMap().entrySet()) {
      a.addAndGet(e.getKey(), e.getValue());
    }
    return a;
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
      MultipleModelMethodType multipleModelMethodType = classificationTask ?
          MultipleModelMethodType.WEIGHTED_MAJORITY_VOTE :
          MultipleModelMethodType.WEIGHTED_AVERAGE;
      Segmentation segmentation = new Segmentation(multipleModelMethodType);
      miningModel.setSegmentation(segmentation);
      for (int treeID = 0; treeID < trees.length; treeID++) {
        TreeModel treeModel =
            toTreeModel(trees[treeID], categoricalValueEncodings, nodeIDCounts.get(treeID));
        Segment segment = new Segment();
        segment.setId(Integer.toString(treeID));
        segment.setPredicate(new True());
        segment.setModel(treeModel);
        segment.setWeight(1.0); // No weights in MLlib impl now
        segmentation.getSegments().add(segment);
      }
    }

    model.setFunctionName(classificationTask ?
                          MiningFunctionType.CLASSIFICATION :
                          MiningFunctionType.REGRESSION);

    double[] importances = countsToImportances(predictorIndexCounts);
    model.setMiningSchema(AppPMMLUtils.buildMiningSchema(inputSchema, importances));
    DataDictionary dictionary =
        AppPMMLUtils.buildDataDictionary(inputSchema, categoricalValueEncodings);

    PMML pmml = PMMLUtils.buildSkeletonPMML();
    pmml.setDataDictionary(dictionary);
    pmml.getModels().add(model);

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
    treeNodes.add(new Pair<>(dtModel.topNode(), (Split) null));

    while (!treeNodes.isEmpty()) {

      Pair<org.apache.spark.mllib.tree.model.Node,Split> treeNodePredicate = treeNodes.remove();
      Node modelNode = modelNodes.remove();

      // This is the decision that got us here from the parent, if any;
      // not the predicate at this node
      Predicate predicate = buildPredicate(treeNodePredicate.getSecond(),
                                           categoricalValueEncodings);
      modelNode.setPredicate(predicate);

      org.apache.spark.mllib.tree.model.Node treeNode = treeNodePredicate.getFirst();
      long nodeCount = nodeIDCounts.get(treeNode.id());
      modelNode.setRecordCount((double) nodeCount);

      if (treeNode.isLeaf()) {

        Predict prediction = treeNode.predict();
        int targetEncodedValue = (int) prediction.predict();
        if (classificationTask) {
          Map<Integer,String> targetEncodingToValue =
              categoricalValueEncodings.getEncodingValueMap(inputSchema.getTargetFeatureIndex());
          String predictedCategoricalValue = targetEncodingToValue.get(targetEncodedValue);
          double confidence = prediction.prob();
          Preconditions.checkState(confidence >= 0.0 && confidence <= 1.0);
          // Slightly faked 'record' count; taken as the probability of the positive class
          // times record count at the node
          long pseudoSDRecordCount = Math.round(confidence * nodeCount);
          ScoreDistribution distribution =
              new ScoreDistribution(predictedCategoricalValue, pseudoSDRecordCount);
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

        org.apache.spark.mllib.tree.model.Node rightTreeNode = treeNode.rightNode().get();
        org.apache.spark.mllib.tree.model.Node leftTreeNode = treeNode.leftNode().get();

        boolean defaultRight =
            nodeIDCounts.get(rightTreeNode.id()) > nodeIDCounts.get(leftTreeNode.id());
        modelNode.setDefaultChild(
            defaultRight ? positiveModelNode.getId() : negativeModelNode.getId());

        // Right node is "positive", so carries the predicate. It must evaluate first
        // and therefore come first in the tree
        modelNodes.add(positiveModelNode);
        modelNodes.add(negativeModelNode);
        treeNodes.add(new Pair<>(rightTreeNode, split));
        treeNodes.add(new Pair<>(leftTreeNode, (Split) null));

      }

    }

    TreeModel treeModel = new TreeModel();
    treeModel.setNode(root);
    treeModel.setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT);
    treeModel.setMissingValueStrategy(MissingValueStrategyType.DEFAULT_CHILD);
    return treeModel;
  }

  private Predicate buildPredicate(Split split,
                                   CategoricalValueEncodings categoricalValueEncodings) {
    if (split == null) {
      // Left child always applies, but is evaluated second
      return new True();
    }

    int featureIndex = inputSchema.predictorToFeatureIndex(split.feature());
    FieldName fieldName = new FieldName(inputSchema.getFeatureNames().get(featureIndex));

    if (split.featureType().equals(FeatureType.Categorical())) {
      // Note that categories in MLlib model select the *left* child but the
      // convention here will be that the predicate selects the *right* child
      // So the predicate will evaluate "not in" this set
      // More ugly casting
      @SuppressWarnings("unchecked")
      List<Double> javaCategories = (List<Double>) (List<?>)
          JavaConversions.seqAsJavaList(split.categories());
      Set<Integer> negativeEncodings = new HashSet<>(javaCategories.size());
      for (double category : javaCategories) {
        negativeEncodings.add((int) category);
      }

      Map<Integer,String> encodingToValue =
          categoricalValueEncodings.getEncodingValueMap(featureIndex);
      List<String> negativeValues = new ArrayList<>();
      for (int negativeEncoding : negativeEncodings) {
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

  private double[] countsToImportances(Map<Integer,Long> predictorIndexCounts) {
    double[] importances = new double[inputSchema.getNumPredictors()];
    long total = 0L;
    for (long count : predictorIndexCounts.values()) {
      total += count;
    }
    for (Map.Entry<Integer,Long> e : predictorIndexCounts.entrySet()) {
      importances[e.getKey()] = (double) e.getValue() / total;
    }
    return importances;
  }

}