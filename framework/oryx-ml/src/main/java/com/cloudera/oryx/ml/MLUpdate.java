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

package com.cloudera.oryx.ml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.batch.BatchLayerUpdate;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.lang.ExecUtils;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.ml.param.HyperParamValues;
import com.cloudera.oryx.ml.param.HyperParams;

/**
 * A specialization of {@link BatchLayerUpdate} for machine learning-oriented
 * update processes. This implementation contains the framework for test/train split
 * for example, parameter optimization, and so on. Subclasses instead implement
 * methods like {@link #buildModel(JavaSparkContext,JavaRDD,List,Path)} to create a PMML model and
 * {@link #evaluate(JavaSparkContext,PMML,Path,JavaRDD,JavaRDD)} to evaluate a model from
 * held-out test data.
 *
 * @param <M> type of message to read from the input topic
 */
public abstract class MLUpdate<M> implements BatchLayerUpdate<Object,M,String> {

  private static final Logger log = LoggerFactory.getLogger(MLUpdate.class);

  public static final String MODEL_FILE_NAME = "model.pmml";

  private final double testFraction;
  private final int candidates;
  private final int evalParallelism;
  private final Double threshold;
  private final int maxMessageSize;

  protected MLUpdate(Config config) {
    this.testFraction = config.getDouble("oryx.ml.eval.test-fraction");
    int candidates = config.getInt("oryx.ml.eval.candidates");
    this.evalParallelism = config.getInt("oryx.ml.eval.parallelism");
    this.threshold = ConfigUtils.getOptionalDouble(config, "oryx.ml.eval.threshold");
    this.maxMessageSize = config.getInt("oryx.update-topic.message.max-size");
    Preconditions.checkArgument(testFraction >= 0.0 && testFraction <= 1.0);
    Preconditions.checkArgument(candidates > 0);
    Preconditions.checkArgument(evalParallelism > 0);
    Preconditions.checkArgument(maxMessageSize > 0);
    if (testFraction == 0.0) {
      if (candidates > 1) {
        log.info("Eval is disabled (test fraction = 0) so candidates is overridden to 1");
        candidates = 1;
      }
    }
    this.candidates = candidates;
  }

  protected final double getTestFraction() {
    return testFraction;
  }

  /**
   * @return a list of hyperparameter value ranges to try, one {@link HyperParamValues} per
   *  hyperparameter. Different combinations of the values derived from the list will be
   *  passed back into {@link #buildModel(JavaSparkContext,JavaRDD,List,Path)}
   */
  public List<HyperParamValues<?>> getHyperParameterValues() {
    return Collections.emptyList();
  }

  /**
   * @param sparkContext active Spark Context
   * @param trainData training data on which to build a model
   * @param hyperParameters ordered list of hyper parameter values to use in building model
   * @param candidatePath directory where additional model files can be written
   * @return a {@link PMML} representation of a model trained on the given data
   */
  public abstract PMML buildModel(JavaSparkContext sparkContext,
                                  JavaRDD<M> trainData,
                                  List<?> hyperParameters,
                                  Path candidatePath);

  /**
   * @return {@code true} iff additional updates must be published along with the model; if
   *  {@link #publishAdditionalModelData(JavaSparkContext, PMML, JavaRDD, JavaRDD, Path, TopicProducer)} must
   *  be called. This is only applicable for special model types.
   */
  public boolean canPublishAdditionalModelData() {
    return false;
  }

  /**
   * Optionally, publish additional model-related information to the update topic,
   * after the model has been written. This is needed only in specific cases, like the
   * ALS algorithm, where the model serialization in PMML can't contain all of the info.
   *
   * @param sparkContext active Spark Context
   * @param pmml model for which extra data should be written
   * @param newData data that has arrived in current interval
   * @param pastData all previously-known data (may be {@code null})
   * @param modelParentPath directory containing model files, if applicable
   * @param modelUpdateTopic message topic to write to
   */
  public void publishAdditionalModelData(JavaSparkContext sparkContext,
                                         PMML pmml,
                                         JavaRDD<M> newData,
                                         JavaRDD<M> pastData,
                                         Path modelParentPath,
                                         TopicProducer<String, String> modelUpdateTopic) {
    // Do nothing by default
  }

  /**
   * @param sparkContext active Spark Context
   * @param model model to evaluate
   * @param modelParentPath directory containing model files, if applicable
   * @param testData data on which to test the model performance
   * @param trainData data on which model was trained, which can also be useful in evaluating
   *  unsupervised learning problems
   * @return an evaluation of the model on the test data. Higher should mean "better"
   */
  public abstract double evaluate(JavaSparkContext sparkContext,
                                  PMML model,
                                  Path modelParentPath,
                                  JavaRDD<M> testData,
                                  JavaRDD<M> trainData);

  @Override
  public void runUpdate(JavaSparkContext sparkContext,
                        long timestamp,
                        JavaPairRDD<Object,M> newKeyMessageData,
                        JavaPairRDD<Object,M> pastKeyMessageData,
                        String modelDirString,
                        TopicProducer<String,String> modelUpdateTopic)
      throws IOException, InterruptedException {

    Objects.requireNonNull(newKeyMessageData);

    JavaRDD<M> newData = newKeyMessageData.values();
    JavaRDD<M> pastData = pastKeyMessageData == null ? null : pastKeyMessageData.values();

    if (newData != null) {
      newData.cache();
      // This forces caching of the RDD. This shouldn't be necessary but we see some freezes
      // when many workers try to materialize the RDDs at once. Hence the workaround.
      newData.foreachPartition(p -> {});
    }
    if (pastData != null) {
      pastData.cache();
      pastData.foreachPartition(p -> {});
    }

    List<HyperParamValues<?>> hyperParamValues = getHyperParameterValues();
    int valuesPerHyperParam =
        HyperParams.chooseValuesPerHyperParam(hyperParamValues.size(), candidates);
    List<List<?>> hyperParameterCombos =
        HyperParams.chooseHyperParameterCombos(hyperParamValues,
                                               candidates,
                                               valuesPerHyperParam);


    Path modelDir = new Path(modelDirString);
    Path tempModelPath = new Path(modelDir, ".temporary");
    Path candidatesPath = new Path(tempModelPath, Long.toString(System.currentTimeMillis()));

    FileSystem fs = FileSystem.get(modelDir.toUri(), sparkContext.hadoopConfiguration());
    fs.mkdirs(candidatesPath);

    Path bestCandidatePath = findBestCandidatePath(
        sparkContext, newData, pastData, hyperParameterCombos, candidatesPath);

    Path finalPath = new Path(modelDir, Long.toString(System.currentTimeMillis()));
    if (bestCandidatePath == null) {
      log.info("Unable to build any model");
    } else {
      // Move best model into place
      fs.rename(bestCandidatePath, finalPath);
    }
    // Then delete everything else
    fs.delete(candidatesPath, true);

    if (modelUpdateTopic == null) {
      log.info("No update topic configured, not publishing models to a topic");
    } else {
      // Push PMML model onto update topic, if it exists
      Path bestModelPath = new Path(finalPath, MODEL_FILE_NAME);
      if (fs.exists(bestModelPath)) {
        FileStatus bestModelPathFS = fs.getFileStatus(bestModelPath);
        PMML bestModel = null;
        boolean modelNeededForUpdates = canPublishAdditionalModelData();
        boolean modelNotTooLarge = bestModelPathFS.getLen() <= maxMessageSize;
        if (modelNeededForUpdates || modelNotTooLarge) {
          // Either the model is required for publishAdditionalModelData, or required because it's going to
          // be serialized to Kafka
          try (InputStream in = fs.open(bestModelPath)) {
            bestModel = PMMLUtils.read(in);
          }
        }

        if (modelNotTooLarge) {
          modelUpdateTopic.send("MODEL", PMMLUtils.toString(bestModel));
        } else {
          modelUpdateTopic.send("MODEL-REF", fs.makeQualified(bestModelPath).toString());
        }

        if (modelNeededForUpdates) {
          publishAdditionalModelData(
              sparkContext, bestModel, newData, pastData, finalPath, modelUpdateTopic);
        }
      }
    }

    if (newData != null) {
      newData.unpersist();
    }
    if (pastData != null) {
      pastData.unpersist();
    }
  }

  private Path findBestCandidatePath(JavaSparkContext sparkContext,
                                     JavaRDD<M> newData,
                                     JavaRDD<M> pastData,
                                     List<List<?>> hyperParameterCombos,
                                     Path candidatesPath) throws IOException {
    Map<Path,Double> pathToEval = ExecUtils.collectInParallel(
        candidates,
        Math.min(evalParallelism, candidates),
        true,
        i -> buildAndEval(i, hyperParameterCombos, sparkContext, newData, pastData, candidatesPath),
        Collectors.toMap(Pair::getFirst, Pair::getSecond));

    FileSystem fs = null;
    Path bestCandidatePath = null;
    double bestEval = Double.NEGATIVE_INFINITY;
    for (Map.Entry<Path,Double> pathEval : pathToEval.entrySet()) {
      Path path = pathEval.getKey();
      if (fs == null) {
        fs = FileSystem.get(path.toUri(), sparkContext.hadoopConfiguration());
      }
      if (path != null && fs.exists(path)) {
        Double eval = pathEval.getValue();
        if (!Double.isNaN(eval)) {
          // Valid evaluation; if it's the best so far, keep it
          if (eval > bestEval) {
            log.info("Best eval / model path is now {} / {}", eval, path);
            bestEval = eval;
            bestCandidatePath = path;
          }
        } else if (bestCandidatePath == null && testFraction == 0.0) {
          // Normal case when eval is disabled; no eval is possible, but keep the one model
          // that was built
          bestCandidatePath = path;
        }
      } // else can't do anything; no model at all
    }
    if (threshold != null && bestEval < threshold) {
      log.info("Best model at {} had eval {}, but did not exceed threshold {}; discarding model",
               bestCandidatePath, bestEval, threshold);
      bestCandidatePath = null;
    }
    return bestCandidatePath;
  }


  private Pair<Path,Double> buildAndEval(int i,
                                         List<List<?>> hyperParameterCombos,
                                         JavaSparkContext sparkContext,
                                         JavaRDD<M> newData,
                                         JavaRDD<M> pastData,
                                         Path candidatesPath) {
    // % = cycle through combinations if needed
    List<?> hyperParameters = hyperParameterCombos.get(i % hyperParameterCombos.size());
    Path candidatePath = new Path(candidatesPath, Integer.toString(i));
    log.info("Building candidate {} with params {}", i, hyperParameters);

    Pair<JavaRDD<M>,JavaRDD<M>> trainTestData = splitTrainTest(newData, pastData);
    JavaRDD<M> allTrainData = trainTestData.getFirst();
    JavaRDD<M> testData = trainTestData.getSecond();

    Double eval = Double.NaN;
    if (empty(allTrainData)) {
      log.info("No train data to build a model");
    } else {
      PMML model = buildModel(sparkContext, allTrainData, hyperParameters, candidatePath);
      if (model == null) {
        log.info("Unable to build a model");
      } else {
        Path modelPath = new Path(candidatePath, MODEL_FILE_NAME);
        log.info("Writing model to {}", modelPath);
        try {
          FileSystem fs = FileSystem.get(candidatePath.toUri(), sparkContext.hadoopConfiguration());
          fs.mkdirs(candidatePath);
          try (OutputStream out = fs.create(modelPath)) {
            PMMLUtils.write(model, out);
          }
        } catch (IOException ioe) {
          throw new IllegalStateException(ioe);
        }
        if (empty(testData)) {
          log.info("No test data available to evaluate model");
        } else {
          log.info("Evaluating model");
          eval = evaluate(sparkContext, model, candidatePath, testData, allTrainData);
        }
      }
    }

    log.info("Model eval for params {}: {} ({})", hyperParameters, eval, candidatePath);
    return new Pair<>(candidatePath, eval);
  }

  private Pair<JavaRDD<M>,JavaRDD<M>> splitTrainTest(JavaRDD<M> newData, JavaRDD<M> pastData) {
    Objects.requireNonNull(newData);
    if (testFraction <= 0.0) {
      return new Pair<>(pastData == null ? newData : newData.union(pastData), null);
    }
    if (testFraction >= 1.0) {
      return new Pair<>(pastData, newData);
    }
    if (empty(newData)) {
      return new Pair<>(pastData, null);
    }
    Pair<JavaRDD<M>,JavaRDD<M>> newTrainTest = splitNewDataToTrainTest(newData);
    JavaRDD<M> newTrainData = newTrainTest.getFirst();
    return new Pair<>(pastData == null ? newTrainData : newTrainData.union(pastData),
                      newTrainTest.getSecond());
  }

  private static boolean empty(JavaRDD<?> rdd) {
    return rdd == null || rdd.isEmpty();
  }

  /**
   * Default implementation which randomly splits new data into train/test sets.
   * This handles the case where {@link #getTestFraction()} is not 0 or 1.
   *
   * @param newData data that has arrived in the current input batch
   * @return a {@link Pair} of train, test {@link RDD}s.
   */
  protected Pair<JavaRDD<M>,JavaRDD<M>> splitNewDataToTrainTest(JavaRDD<M> newData) {
    RDD<M>[] testTrainRDDs = newData.rdd().randomSplit(
        new double[]{1.0 - testFraction, testFraction},
        RandomManager.getRandom().nextLong());
    return new Pair<>(newData.wrapRDD(testTrainRDDs[0]),
                      newData.wrapRDD(testTrainRDDs[1]));
  }

}
