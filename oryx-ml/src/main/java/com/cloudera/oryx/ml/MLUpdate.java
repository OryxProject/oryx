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

package com.cloudera.oryx.ml;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.lambda.QueueProducer;
import com.cloudera.oryx.lambda.update.BatchLayerUpdate;
import com.cloudera.oryx.ml.param.HyperParamRange;
import com.cloudera.oryx.ml.param.HyperParamRanges;
import com.cloudera.oryx.common.pmml.PMMLUtils;

/**
 * A specialization of {@link com.cloudera.oryx.lambda.update.BatchLayerUpdate} for machine learning-oriented
 * update processes. This implementation contains the framework for test/train split
 * for example, parameter optimization, and so on. Subclasses instead implement
 * methods like {@link #buildModel(JavaSparkContext,JavaRDD,List,Path)} to create a PMML model and
 * {@link #evaluate(JavaSparkContext,PMML,Path,JavaRDD)} to evaluate a model from
 * held-out test data.
 */
public abstract class MLUpdate<M> implements BatchLayerUpdate<Object,M,String> {

  private static final Logger log = LoggerFactory.getLogger(MLUpdate.class);

  public static final String MODEL_FILE_NAME = "model.pmml.gz";

  private final double testFraction;
  private final int candidates;
  private final int evalParallelism;

  protected MLUpdate(Config config) {
    this.testFraction = config.getDouble("ml.eval.test-fraction");
    this.candidates = config.getInt("ml.eval.candidates");
    this.evalParallelism = config.getInt("ml.eval.parallelism");
    Preconditions.checkArgument(testFraction >= 0.0 && testFraction <= 1.0);
    Preconditions.checkArgument(candidates > 0);
    Preconditions.checkArgument(evalParallelism > 0);
  }

  /**
   * @return a list of hyperparameter value ranges to try, one {@link HyperParamRange} per
   *  hyperparameter. Different combinations of the values derived from the list will be
   *  passed back into {@link #buildModel(JavaSparkContext,JavaRDD,List,Path)}
   */
  public List<HyperParamRange> getHyperParameterRanges() {
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
                                  List<Number> hyperParameters,
                                  Path candidatePath);

  /**
   * Optionally, publish additional model-related information to the update queue,
   * after the model has been written. This is needed only in specific cases, like the
   * ALS algorithm, where the model serialization in PMML can't contain all of the info.
   *
   * @param sparkContext active Spark Context
   * @param pmml model for which extra data should be written
   * @param modelParentPath directory containing model files, if applicable
   * @param modelUpdateQueue message queue to write to
   */
  public void addModelData(JavaSparkContext sparkContext,
                           PMML pmml,
                           Path modelParentPath,
                           QueueProducer<String,String> modelUpdateQueue) {
    // Do nothing by default
  }

  /**
   * @param sparkContext active Spark Context
   * @param model model to evaluate
   * @param modelParentPath directory containing model files, if applicable
   * @param testData data on which to test the model performance
   * @return an evaluation of the model on the test data. Higher should mean "better"
   */
  public abstract double evaluate(JavaSparkContext sparkContext,
                                  PMML model,
                                  Path modelParentPath,
                                  JavaRDD<M> testData);

  @Override
  public void configureUpdate(JavaSparkContext sparkContext,
                              long timestamp,
                              JavaPairRDD<Object,M> newKeyMessageData,
                              JavaPairRDD<Object,M> pastKeyMessageData,
                              String modelDirString,
                              QueueProducer<String,String> modelUpdateQueue)
      throws IOException, InterruptedException {

    Preconditions.checkNotNull(newKeyMessageData);

    JavaRDD<M> newData = newKeyMessageData.values();
    JavaRDD<M> pastData = pastKeyMessageData == null ? null : pastKeyMessageData.values();

    List<HyperParamRange> hyperParamRanges = getHyperParameterRanges();
    int valuesPerHyperParam = chooseValuesPerHyperParam(hyperParamRanges.size());
    List<List<Number>> hyperParameterCombos =
        HyperParamRanges.chooseHyperParameterCombos(hyperParamRanges,
                                                    candidates,
                                                    valuesPerHyperParam);

    FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration());

    Path modelDir = new Path(modelDirString);
    Path tempModelPath = new Path(modelDir, ".temporary");
    Path candidatesPath = new Path(tempModelPath, Long.toString(System.currentTimeMillis()));
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

    // Push PMML model onto model queue, if it exists
    Path bestModelPath = new Path(finalPath, MODEL_FILE_NAME);
    if (fs.exists(bestModelPath)) {
      PMML bestModel;
      try (InputStream in = new GZIPInputStream(fs.open(bestModelPath), 1 << 16)) {
        bestModel = PMMLUtils.read(in);
      }
      modelUpdateQueue.send("MODEL", PMMLUtils.toString(bestModel));
      addModelData(sparkContext, bestModel, finalPath, modelUpdateQueue);
    }
  }

  /**
   * @return smallest value such that pow(value, numParams) is at least the number of candidates
   *  requested to build. Returns 0 if numParams is less than 1.
   */
  private int chooseValuesPerHyperParam(int numParams) {
    if (numParams < 1) {
      return 0;
    }
    int valuesPerHyperParam = 0;
    int total;
    do {
      valuesPerHyperParam++;
      total = 1;
      for (int i = 0; i < numParams; i++) {
        total *= valuesPerHyperParam;
      }
    } while (total < candidates);
    return valuesPerHyperParam;
  }

  private Path findBestCandidatePath(JavaSparkContext sparkContext,
                                     JavaRDD<M> newData,
                                     JavaRDD<M> pastData,
                                     List<List<Number>> hyperParameterCombos,
                                     Path candidatesPath) throws InterruptedException, IOException {
    Map<Path,Double> pathToEval = new HashMap<>(candidates);
    if (evalParallelism > 1) {
      Collection<Future<Tuple2<Path,Double>>> futures = new ArrayList<>(candidates);
      ExecutorService executor = Executors.newFixedThreadPool(evalParallelism);
      try {
        for (int i = 0; i < candidates; i++) {
          futures.add(executor.submit(new BuildAndEvalWorker(
              i, hyperParameterCombos, sparkContext, newData, pastData, candidatesPath)));
        }
      } finally {
        executor.shutdown();
      }
      for (Future<Tuple2<Path,Double>> future : futures) {
        Tuple2<Path,Double> pathEval;
        try {
          pathEval = future.get();
        } catch (ExecutionException e) {
          throw new IllegalStateException(e);
        }
        pathToEval.put(pathEval._1(), pathEval._2());
      }
    } else {
      // Execute serially
      for (int i = 0; i < candidates; i++) {
        Tuple2<Path,Double> pathEval = new BuildAndEvalWorker(
            i, hyperParameterCombos, sparkContext, newData, pastData, candidatesPath).call();
        pathToEval.put(pathEval._1(), pathEval._2());
      }
    }

    FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration());

    Path bestCandidatePath = null;
    double bestEval = Double.NEGATIVE_INFINITY;
    for (Map.Entry<Path,Double> pathEval : pathToEval.entrySet()) {
      Path path = pathEval.getKey();
      Double eval = pathEval.getValue();
      if ((bestCandidatePath == null || (eval != null && eval > bestEval)) &&
          fs.exists(path)) {
        log.info("Best eval / path is now {} {}", eval ,path);
        if (eval != null) {
          bestEval = eval;
        }
        bestCandidatePath = path;
      }
    }
    return bestCandidatePath;
  }


  final class BuildAndEvalWorker implements Callable<Tuple2<Path,Double>> {

    private final int i;
    private final List<List<Number>> hyperParameterCombos;
    private final JavaSparkContext sparkContext;
    private final JavaRDD<M> newData;
    private final JavaRDD<M> pastData;
    private final Path candidatesPath;

    BuildAndEvalWorker(int i,
                       List<List<Number>> hyperParameterCombos,
                       JavaSparkContext sparkContext,
                       JavaRDD<M> newData,
                       JavaRDD<M> pastData,
                       Path candidatesPath) {
      this.i = i;
      this.hyperParameterCombos = hyperParameterCombos;
      this.sparkContext = sparkContext;
      this.newData = newData;
      this.pastData = pastData;
      this.candidatesPath = candidatesPath;
    }

    @Override
    public Tuple2<Path,Double> call() throws IOException {
      // % = cycle through combinations if needed
      List<Number> hyperParameters = hyperParameterCombos.get(i % hyperParameterCombos.size());
      Path candidatePath = new Path(candidatesPath, Integer.toString(i));
      log.info("Building candidate {} with params {}", i, hyperParameters);

      JavaRDD<M> allTrainData;
      JavaRDD<M> testData;
      if (testFraction <= 0.0) {

        allTrainData = pastData == null ? newData : newData.union(pastData);
        testData = null;

      } else if (testFraction >= 1.0) {

        allTrainData = pastData;
        testData = newData;

      } else {

        RDD<M>[] testTrainRDDs = newData.rdd().randomSplit(
            new double[]{1.0 - testFraction, testFraction},
            RandomManager.getRandom().nextLong());
        JavaRDD<M> newTrainData = newData.wrapRDD(testTrainRDDs[0]);
        allTrainData = pastData == null ? newTrainData : newTrainData.union(pastData);
        testData = newData.wrapRDD(testTrainRDDs[1]);

      }

      long trainDataSize = allTrainData == null ? 0 : allTrainData.count();
      long testDataSize = testData == null ? 0 : testData.count();
      log.info("Train set size: {} Test set size: {}", trainDataSize, testDataSize);

      Double eval = null;
      if (trainDataSize <= 0) {
        log.info("No train data to build a model");
      } else {
        PMML model = buildModel(sparkContext, allTrainData, hyperParameters, candidatePath);
        if (model == null) {
          log.info("Unable to build a model");
        } else {
          Path modelPath = new Path(candidatePath, MODEL_FILE_NAME);
          log.info("Writing model to {}", modelPath);
          FileSystem fs = FileSystem.get(sparkContext.hadoopConfiguration());
          fs.mkdirs(candidatePath);
          try (OutputStream out = new GZIPOutputStream(fs.create(modelPath), 1 << 16)) {
            PMMLUtils.write(model, out);
          }
          if (testDataSize == 0) {
            log.info("No test data available to evaluate model");
          } else {
            double thisEval = evaluate(sparkContext, model, candidatePath, testData);
            eval = Double.isNaN(thisEval) ? null : thisEval;
          }
        }
      }
      log.info("Model eval for params {}: {} ({})", hyperParameters, eval, candidatePath);
      return new Tuple2<>(candidatePath, eval);
    }
  }

}
