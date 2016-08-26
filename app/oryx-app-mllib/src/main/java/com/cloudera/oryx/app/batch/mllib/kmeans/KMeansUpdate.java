/*
 * Copyright (c) 2014, Cloudera and Intel, Inc. All Rights Reserved.
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

package com.cloudera.oryx.app.batch.mllib.kmeans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningFunction;
import org.dmg.pmml.PMML;
import org.dmg.pmml.SquaredEuclidean;
import org.dmg.pmml.clustering.Cluster;
import org.dmg.pmml.clustering.ClusteringField;
import org.dmg.pmml.clustering.ClusteringModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.kmeans.ClusterInfo;
import com.cloudera.oryx.app.kmeans.KMeansPMMLUtils;
import com.cloudera.oryx.app.kmeans.KMeansUtils;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.app.schema.InputSchema;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.param.HyperParamValues;
import com.cloudera.oryx.ml.param.HyperParams;

/**
 * Update function that builds and evaluates k-means models in the Batch Layer.
 */
public final class KMeansUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(KMeansUpdate.class);

  private final String initializationStrategy;
  private final int maxIterations;
  private final int numberOfRuns;
  private final List<HyperParamValues<?>> hyperParamValues;
  private final InputSchema inputSchema;
  private final KMeansEvalStrategy evaluationStrategy;

  public KMeansUpdate(Config config) {
    super(config);
    initializationStrategy = config.getString("oryx.kmeans.initialization-strategy");
    evaluationStrategy = Enum.valueOf(KMeansEvalStrategy.class, config.getString("oryx.kmeans.evaluation-strategy"));
    numberOfRuns = config.getInt("oryx.kmeans.runs");
    maxIterations = config.getInt("oryx.kmeans.iterations");
    hyperParamValues = new ArrayList<>();
    hyperParamValues.add(HyperParams.fromConfig(config, "oryx.kmeans.hyperparams.k"));
    inputSchema = new InputSchema(config);
    Preconditions.checkArgument(maxIterations > 0);
    Preconditions.checkArgument(numberOfRuns > 0);
    Preconditions.checkArgument(
        initializationStrategy.equals(KMeans.K_MEANS_PARALLEL()) ||
            initializationStrategy.equals(KMeans.RANDOM()));
    // Should be an unsupervised problem. This impl only supports numeric features.
    Preconditions.checkArgument(!inputSchema.hasTarget());
    for (int i = 0; i < inputSchema.getNumFeatures(); i++) {
      Preconditions.checkArgument(!inputSchema.isCategorical(i));
    }
  }

  /**
   * @return a list of hyperparameter value ranges to try, one {@link HyperParamValues} per
   *  hyperparameter. Different combinations of the values derived from the list will be
   *  passed back into {@link #buildModel(JavaSparkContext,JavaRDD,List,Path)}
   */
  @Override
  public List<HyperParamValues<?>> getHyperParameterValues() {
    return hyperParamValues;
  }

  /**
   * @param sparkContext    active Spark Context
   * @param trainData       training data on which to build a model
   * @param hyperParameters ordered list of hyper parameter values to use in building model
   * @param candidatePath   directory where additional model files can be written
   * @return a {@link PMML} representation of a model trained on the given data
   */
  @Override
  public PMML buildModel(JavaSparkContext sparkContext,
                         JavaRDD<String> trainData,
                         List<?> hyperParameters,
                         Path candidatePath) {
    int numClusters = (Integer) hyperParameters.get(0);
    Preconditions.checkArgument(numClusters > 1);
    log.info("Building KMeans Model with {} clusters", numClusters);

    JavaRDD<Vector> trainingData = parsedToVectorRDD(trainData.map(MLFunctions.PARSE_FN));
    KMeansModel kMeansModel = KMeans.train(trainingData.rdd(), numClusters, maxIterations,
                                           numberOfRuns, initializationStrategy);

    return kMeansModelToPMML(kMeansModel, fetchClusterCountsFromModel(trainingData, kMeansModel));
  }

  /**
   * @param trainPointData data to cluster
   * @param model trained KMeans Model
   * @return map of ClusterId, count of points associated with the clusterId
   */
  private static Map<Integer,Long> fetchClusterCountsFromModel(JavaRDD<Vector> trainPointData,
                                                               KMeansModel model) {
     return trainPointData.map(model::predict).countByValue();
  }

  /**
   * @param sparkContext    active Spark Context
   * @param model           model to evaluate
   * @param modelParentPath directory containing model files, if applicable
   * @param testData        data on which to test the model performance
   * @return an evaluation of the model on the test data. Higher should mean "better"
   */
  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData,
                         JavaRDD<String> trainData) {
    KMeansPMMLUtils.validatePMMLVsSchema(model, inputSchema);
    JavaRDD<Vector> evalData =
        parsedToVectorRDD(trainData.union(testData).map(MLFunctions.PARSE_FN));
    List<ClusterInfo> clusterInfoList = KMeansPMMLUtils.read(model);

    log.info("Evaluation Strategy is {}", evaluationStrategy);
    double eval;
    switch (evaluationStrategy) {
      case DAVIES_BOULDIN:
        double dbIndex = new DaviesBouldinIndex(clusterInfoList).evaluate(evalData);
        log.info("Davies-Bouldin index: {}", dbIndex);
        eval = -dbIndex;
        break;
      case DUNN:
        double dunnIndex = new DunnIndex(clusterInfoList).evaluate(evalData);
        log.info("Dunn index: {}", dunnIndex);
        eval = dunnIndex;
        break;
      case SILHOUETTE:
        double silhouette = new SilhouetteCoefficient(clusterInfoList).evaluate(evalData);
        log.info("Silhouette Coefficient: {}", silhouette);
        eval = silhouette;
        break;
      case SSE :
        double sse = new SumSquaredError(clusterInfoList).evaluate(evalData);
        log.info("Sum squared error: {}", sse);
        eval = -sse;
        break;
      default:
        throw new IllegalArgumentException("Unknown evaluation strategy " + evaluationStrategy);
    }

    return eval;
  }

  /**
   * @param model {@link KMeansModel} to translate to PMML
   * @return PMML representation of a KMeans cluster model
   */
  private PMML kMeansModelToPMML(KMeansModel model, Map<Integer,Long> clusterSizesMap) {
    ClusteringModel clusteringModel = pmmlClusteringModel(model, clusterSizesMap);
    PMML pmml = PMMLUtils.buildSkeletonPMML();
    pmml.setDataDictionary(AppPMMLUtils.buildDataDictionary(inputSchema, null));
    pmml.addModels(clusteringModel);
    return pmml;
  }

  private ClusteringModel pmmlClusteringModel(KMeansModel model,
                                              Map<Integer,Long> clusterSizesMap) {
    Vector[] clusterCenters = model.clusterCenters();

    List<ClusteringField> clusteringFields = new ArrayList<>();
    for (int i = 0; i < inputSchema.getNumFeatures(); i++) {
      if (inputSchema.isActive(i)) {
        FieldName fieldName = FieldName.create(inputSchema.getFeatureNames().get(i));
        ClusteringField clusteringField =
            new ClusteringField(fieldName).setCenterField(ClusteringField.CenterField.TRUE);
        clusteringFields.add(clusteringField);
      }
    }

    List<Cluster> clusters = new ArrayList<>(clusterCenters.length);
    for (int i = 0; i < clusterCenters.length; i++) {
      clusters.add(new Cluster().setId(Integer.toString(i))
                       .setSize(clusterSizesMap.get(i).intValue())
                       .setArray(AppPMMLUtils.toArray(clusterCenters[i].toArray())));
    }

    return new ClusteringModel(
        MiningFunction.CLUSTERING,
        ClusteringModel.ModelClass.CENTER_BASED,
        clusters.size(),
        AppPMMLUtils.buildMiningSchema(inputSchema),
        new ComparisonMeasure(ComparisonMeasure.Kind.DISTANCE).setMeasure(new SquaredEuclidean()),
        clusteringFields,
        clusters);
  }

  private JavaRDD<Vector> parsedToVectorRDD(JavaRDD<String[]> parsedRDD) {
    return parsedRDD.map(data -> {
      try {
        return Vectors.dense(KMeansUtils.featuresFromTokens(data, inputSchema));
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", Arrays.toString(data));
        throw e;
      }
    });
  }

}
