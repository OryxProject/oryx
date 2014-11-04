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

package com.cloudera.oryx.ml.mllib.kmeans;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.dmg.pmml.Array;
import org.dmg.pmml.Cluster;
import org.dmg.pmml.ClusteringModel;
import org.dmg.pmml.ComparisonMeasure;
import org.dmg.pmml.PMML;
import org.dmg.pmml.SquaredEuclidean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.mllib.common.fn.MLFunctions;
import com.cloudera.oryx.ml.param.HyperParamRange;
import com.cloudera.oryx.ml.param.HyperParamRanges;

public class KMeansUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(KMeansUpdate.class);
  private static final Pattern COMMA = Pattern.compile(",");

  private final String initializatioStrategy;
  private final int maxIterations;
  private final int numberOfRuns;
  private final List<HyperParamRange> hyperParamRanges;

  protected KMeansUpdate(Config config) {
    super(config);
    initializatioStrategy = config.getString("kmeans.initialization-strategy");
    numberOfRuns = config.getInt("kmeans.runs");
    maxIterations = config.getInt("kmeans.iterations");
    hyperParamRanges = Arrays.asList(
        HyperParamRanges.fromConfig(config, "kmeans.hyperparams.k"));

    Preconditions.checkArgument(maxIterations > 0);
    Preconditions.checkArgument(numberOfRuns > 0);
    Preconditions.checkArgument(
        initializatioStrategy.equals(KMeans.K_MEANS_PARALLEL()) ||
            initializatioStrategy.equals(KMeans.RANDOM()));
  }

  /**
   * @return a list of hyperparameter value ranges to try, one {@link HyperParamRange} per
   *  hyperparameter. Different combinations of the values derived from the list will be
   *  passed back into {@link #buildModel(JavaSparkContext,JavaRDD,List,Path)}
   */
  @Override
  public List<HyperParamRange> getHyperParameterRanges() {
    return hyperParamRanges;
  }

  /**
   * @param sparkContext    active Spark Context
   * @param trainData       training data on which to build a model
   * @param hyperParams ordered list of hyper parameter values to use in building model
   * @param candidatePath   directory where additional model files can be written
   * @return a {@link org.dmg.pmml.PMML} representation of a model trained on the given data
   */
  @Override
  public PMML buildModel(JavaSparkContext sparkContext,
                         JavaRDD<String> trainData,
                         List<Number> hyperParams,
                         Path candidatePath) {
    log.info("Building model with params {}", hyperParams);

    int numClusters = hyperParams.get(0).intValue();
    Preconditions.checkArgument(numClusters > 1);

    JavaRDD<Vector> trainingData = parsedToVectorRDD(trainData.map(MLFunctions.PARSE_FN));
    KMeansModel kMeansModel = KMeans.train(trainingData.rdd(), numClusters, maxIterations,
                                           numberOfRuns, initializatioStrategy);

    return kMeansModelToPMML(kMeansModel);
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
                         JavaRDD<String> testData) {
    // TODO: A Strategy pattern to be able to invoke different evaluation metrics
    JavaRDD<Vector> testingData = parsedToVectorRDD(testData.map(MLFunctions.PARSE_FN));
    return pmmlToKMeansModel(model).computeCost(testingData.rdd());
  }

  /**
   * Returns a PMML representation of a KMeans cluster model
   * @param model - {@link org.apache.spark.mllib.clustering.KMeansModel}
   * @return {@link org.dmg.pmml.PMML}
   */
  private static PMML kMeansModelToPMML(KMeansModel model) {
    PMML pmml = PMMLUtils.buildSkeletonPMML();
    ClusteringModel clusteringModel = new ClusteringModel();
    clusteringModel.setAlgorithmName("K-Means||");
    clusteringModel.setComparisonMeasure(
        new ComparisonMeasure(ComparisonMeasure.Kind.DISTANCE).withMeasure(new SquaredEuclidean()));
    clusteringModel.setNumberOfClusters(model.k());

    Vector[] clusterCenters = model.clusterCenters();
    for (int i = 0; i < clusterCenters.length; i++) {
      clusteringModel.getClusters().add(toCluster(clusterCenters[i], i));
    }
    // TODO: Add MiningSchema, Dictionary, Clustering Fields, Transformations to the PMML
    pmml.getModels().add(clusteringModel);
    return pmml;
  }

  /**
   * Retrieves {@link KMeansModel} from PMML
   * @param pmml - PMML model to retrieve the original {@link KMeansModel} from
   * @return {@link KMeansModel}
   */
  private static KMeansModel pmmlToKMeansModel(PMML pmml) {
    ClusteringModel clusteringModel = (ClusteringModel) pmml.getModels().get(0);
    List<Cluster> clusters = clusteringModel.getClusters();
    Vector[] clusterCenters = new Vector[clusters.size()];
    for (Cluster cluster : clusters) {
      clusterCenters[Integer.valueOf(cluster.getId())] =
          toVectorFromPMMLArray(cluster.getArray());
    }
    return new KMeansModel(clusterCenters);
  }

  private static JavaRDD<Vector> parsedToVectorRDD(JavaRDD<String[]> parsedRDD) {
    return parsedRDD.map(new Function<String[], Vector>() {
        @Override
        public Vector call(String[] tokens) {
          double[] values = new double[tokens.length];
          for (int i = 0; i < tokens.length; i++) {
            values[i] = Double.parseDouble(tokens[i]);
          }
          return Vectors.dense(values);
        }
    });
  }

  private static Cluster toCluster(Vector center, int clusterId) {
    Cluster cluster = new Cluster();
    cluster.setId(String.valueOf(clusterId));
    String s = Arrays.toString(center.toArray());
    // Strip the beginning '[' and end ']'
    Array array = new Array(s.substring(1, s.length() - 1), Array.Type.REAL);
    // TODO: figure out how to set this when its a sparse vector
    array.setN(center.size());
    cluster.setArray(array);
    return cluster;
  }

  /**
   * Retrieve a {@link org.apache.spark.mllib.linalg.Vector} from PMML
   * @param array - {@link org.dmg.pmml.Array}
   * @return {@link org.apache.spark.mllib.linalg.Vector}
   */
  // TODO: figure out how to regenerate a sparse vector from org.dmg.pmml.Array
  private static Vector toVectorFromPMMLArray(Array array) {
    String[] values = COMMA.split(array.getValue());
    double[] doubles = new double[values.length];
    for (int i = 0; i < values.length; i++) {
      doubles[i] = Double.valueOf(values[i]);
    }
    return Vectors.dense(doubles);
  }

}
