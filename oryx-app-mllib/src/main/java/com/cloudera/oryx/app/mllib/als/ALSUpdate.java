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

package com.cloudera.oryx.app.mllib.als;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.StatCounter;
import org.dmg.pmml.PMML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassTag$;

import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.lambda.TopicProducer;
import com.cloudera.oryx.lambda.fn.Functions;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.param.HyperParamValues;
import com.cloudera.oryx.ml.param.HyperParams;
import com.cloudera.oryx.common.pmml.PMMLUtils;

/**
 * A specialization of {@link MLUpdate} that creates a matrix factorization model of its
 * input, using the Alternating Least Squares algorithm.
 */
public final class ALSUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSUpdate.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final int iterations;
  private final boolean implicit;
  private final List<HyperParamValues<?>> hyperParamValues;
  private final boolean noKnownItems;

  public ALSUpdate(Config config) {
    super(config);
    iterations = config.getInt("oryx.als.iterations");
    implicit = config.getBoolean("oryx.als.implicit");
    Preconditions.checkArgument(iterations > 0);
    hyperParamValues = Arrays.asList(
        HyperParams.fromConfig(config, "oryx.als.hyperparams.features"),
        HyperParams.fromConfig(config, "oryx.als.hyperparams.lambda"),
        HyperParams.fromConfig(config, "oryx.als.hyperparams.alpha"));
    noKnownItems = config.getBoolean("oryx.als.no-known-items");
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
    int features = (Integer) hyperParameters.get(0);
    double lambda = (Double) hyperParameters.get(1);
    double alpha = (Double) hyperParameters.get(2);
    Preconditions.checkArgument(features > 0);
    Preconditions.checkArgument(lambda >= 0.0);
    Preconditions.checkArgument(alpha > 0.0);

    JavaRDD<Rating> trainRatingData = parsedToRatingRDD(trainData.map(MLFunctions.PARSE_FN));
    trainRatingData = aggregateScores(trainRatingData);
    MatrixFactorizationModel model;
    if (implicit) {
      model = ALS.trainImplicit(trainRatingData.rdd(), features, iterations, lambda, alpha);
    } else {
      model = ALS.train(trainRatingData.rdd(), features, iterations, lambda);
    }

    PMML pmml = mfModelToPMML(model, features, lambda, alpha, implicit, candidatePath);
    unpersist(model);
    return pmml;
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData) {
    JavaRDD<Rating> testRatingData = parsedToRatingRDD(testData.map(MLFunctions.PARSE_FN));
    testRatingData = aggregateScores(testRatingData);
    MatrixFactorizationModel mfModel = pmmlToMFModel(sparkContext, model, modelParentPath);
    double eval;
    if (implicit) {
      double auc = AUC.areaUnderCurve(sparkContext, mfModel, testRatingData);
      log.info("AUC: {}", auc);
      eval = auc;
    } else {
      double rmse = RMSE.rmse(mfModel, testRatingData);
      log.info("RMSE: {}", rmse);
      eval = 1.0 / rmse;
    }
    unpersist(mfModel);
    return eval;
  }

  /**
   * Manually unpersists the RDDs that are persisted inside a model.
   *
   * @param model model whose RDDs were persisted
   */
  private static void unpersist(MatrixFactorizationModel model) {
    model.userFeatures().unpersist(false);
    model.productFeatures().unpersist(false);
  }

  @Override
  public void publishAdditionalModelData(JavaSparkContext sparkContext,
                                         PMML pmml,
                                         JavaRDD<String> newData,
                                         JavaRDD<String> pastData,
                                         Path modelParentPath,
                                         TopicProducer<String, String> modelUpdateTopic) {

    JavaRDD<String[]> allData =
        (pastData == null ? newData : newData.union(pastData)).map(MLFunctions.PARSE_FN);

    log.info("Sending user / X data as model updates");
    String xPathString = AppPMMLUtils.getExtensionValue(pmml, "X");
    JavaPairRDD<Integer,double[]> userRDD =
        fromRDD(readFeaturesRDD(sparkContext, new Path(modelParentPath, xPathString)));

    if (noKnownItems) {
      userRDD.foreach(new EnqueueFeatureVecsFn("X", modelUpdateTopic));
    } else {
      log.info("Sending known item data with model updates");
      JavaPairRDD<Integer,Collection<Integer>> knownItems = knownsRDD(allData, true);
      userRDD.join(knownItems).foreach(
          new EnqueueFeatureVecsAndKnownItemsFn("X", modelUpdateTopic));
    }

    log.info("Sending item / Y data as model updates");
    String yPathString = AppPMMLUtils.getExtensionValue(pmml, "Y");
    JavaPairRDD<Integer,double[]> productRDD =
        fromRDD(readFeaturesRDD(sparkContext, new Path(modelParentPath, yPathString)));

    // For now, there is no use in sending known users for each item
    //if (noKnownItems) {
    productRDD.foreach(new EnqueueFeatureVecsFn("Y", modelUpdateTopic));
    //} else {
    //  log.info("Sending known user data with model updates");
    //  JavaPairRDD<Integer,Collection<Integer>> knownUsers = knownsRDD(allData, false);
    //  productRDD.join(knownUsers).foreach(
    //      new EnqueueFeatureVecsAndKnownItemsFn("Y", modelUpdateTopic));
    //}
  }

  /**
   * Implementation which splits based solely on time. It will return approximately
   * the earliest {@link #getTestFraction()} of input, ordered by timestamp, as new training
   * data and the rest as test data.
   */
  @Override
  protected Pair<JavaRDD<String>,JavaRDD<String>> splitNewDataToTrainTest(JavaRDD<String> newData) {
    // Rough approximation; assumes timestamps are fairly evenly distributed
    StatCounter maxMin = newData.mapToDouble(new DoubleFunction<String>() {
      @Override
      public double call(String line) throws Exception {
        return MLFunctions.TO_TIMESTAMP_FN.call(line).doubleValue();
      }
    }).stats();

    long minTime = (long) maxMin.min();
    long maxTime = (long) maxMin.max();
    log.info("New data timestamp range: {} - {}", minTime, maxTime);
    final long approxTestTrainBoundary = minTime + (long) (getTestFraction() * (maxTime - minTime));
    log.info("Splitting at timestamp {}", approxTestTrainBoundary);

    JavaRDD<String> newTrainData = newData.filter(new Function<String,Boolean>() {
      @Override
      public Boolean call(String line) throws Exception {
        return MLFunctions.TO_TIMESTAMP_FN.call(line) < approxTestTrainBoundary;
      }
    });
    JavaRDD<String> testData = newData.filter(new Function<String,Boolean>() {
      @Override
      public Boolean call(String line) throws Exception {
        return MLFunctions.TO_TIMESTAMP_FN.call(line) >= approxTestTrainBoundary;
      }
    });

    return new Pair<>(newTrainData, testData);
  }

  /**
   * @param parsedRDD parsed input as {@code String[]}
   * @return {@link Rating}s ordered by timestamp
   */
  private static JavaRDD<Rating> parsedToRatingRDD(JavaRDD<String[]> parsedRDD) {
    return parsedRDD.mapToPair(new PairFunction<String[],Long,Rating>() {
      @Override
      public Tuple2<Long,Rating> call(String[] tokens) {
        return new Tuple2<>(
            Long.valueOf(tokens[3]),
            new Rating(Integer.parseInt(tokens[0]),
                       Integer.parseInt(tokens[1]),
                       // Empty value means 'delete'; propagate as NaN
                       tokens[2].isEmpty() ? Double.NaN : Double.parseDouble(tokens[2])));
      }
    }).sortByKey().values();
  }

  /**
   * Combines {@link Rating}s with the same user/item into one, with score as the sum of
   * all of the scores.
   */
  private JavaRDD<Rating> aggregateScores(JavaRDD<Rating> original) {
    JavaPairRDD<Tuple2<Integer,Integer>,Double> tuples =
        original.mapToPair(new RatingToTupleDouble());

    JavaPairRDD<Tuple2<Integer,Integer>,Double> aggregated;
    if (implicit) {
      // TODO can we avoid groupByKey? reduce, combine, fold don't seem viable since
      // they don't guarantee the delete elements are properly handled
      aggregated = tuples.groupByKey().mapValues(MLFunctions.SUM_WITH_NAN);
    } else {
      // For non-implicit, last wins.
      aggregated = tuples.foldByKey(Double.NaN, Functions.<Double>last());
    }

    return aggregated
        .filter(MLFunctions.<Tuple2<Integer,Integer>>notNaNValue())
        .map(new Function<Tuple2<Tuple2<Integer,Integer>,Double>,Rating>() {
               @Override
               public Rating call(Tuple2<Tuple2<Integer,Integer>,Double> userProductScore) {
                 Tuple2<Integer,Integer> userProduct = userProductScore._1();
                 return new Rating(userProduct._1(), userProduct._2(), userProductScore._2());
               }
             });
      }

  /**
   * There is no actual serialization of a massive factored matrix model into PMML.
   * Instead, we create an ad-hoc serialization where the model just contains pointers
   * to files that contain the matrix data, as Extensions.
   */
  private static PMML mfModelToPMML(MatrixFactorizationModel model,
                                    int features,
                                    double lambda,
                                    double alpha,
                                    boolean implicit,
                                    Path candidatePath) {
    saveFeaturesRDD(model.userFeatures(), new Path(candidatePath, "X"));
    saveFeaturesRDD(model.productFeatures(), new Path(candidatePath, "Y"));

    PMML pmml = PMMLUtils.buildSkeletonPMML();
    AppPMMLUtils.addExtension(pmml, "X", "X/");
    AppPMMLUtils.addExtension(pmml, "Y", "Y/");
    AppPMMLUtils.addExtension(pmml, "features", features);
    AppPMMLUtils.addExtension(pmml, "lambda", lambda);
    AppPMMLUtils.addExtension(pmml, "implicit", implicit);
    if (implicit) {
      AppPMMLUtils.addExtension(pmml, "alpha", alpha);
    }
    addIDsExtension(pmml, "XIDs", model.userFeatures());
    addIDsExtension(pmml, "YIDs", model.productFeatures());
    return pmml;
  }

  private static void addIDsExtension(PMML pmml,
                                      String key,
                                      RDD<Tuple2<Object,double[]>> features) {
    List<String> ids = fromRDD(features).keys().map(Functions.toStringValue()).collect();
    AppPMMLUtils.addExtensionContent(pmml, key, ids);
  }

  private static void saveFeaturesRDD(RDD<Tuple2<Object,double[]>> features, Path path) {
    log.info("Saving features RDD to {}", path);
    fromRDD(features).map(new Function<Tuple2<Object,double[]>, String>() {
      @Override
      public String call(Tuple2<Object, double[]> keyAndVector) throws IOException {
        Object key = keyAndVector._1();
        double[] vector = keyAndVector._2();
        return MAPPER.writeValueAsString(Arrays.asList(key, vector));
      }
    }).saveAsTextFile(path.toString(), GzipCodec.class);
  }

  private static MatrixFactorizationModel pmmlToMFModel(JavaSparkContext sparkContext,
                                                        PMML pmml,
                                                        Path modelParentPath) {
    String xPathString = AppPMMLUtils.getExtensionValue(pmml, "X");
    String yPathString = AppPMMLUtils.getExtensionValue(pmml, "Y");

    RDD<Tuple2<Integer,double[]>> userRDD =
        readFeaturesRDD(sparkContext, new Path(modelParentPath, xPathString));
    RDD<Tuple2<Integer,double[]>> productRDD =
        readFeaturesRDD(sparkContext, new Path(modelParentPath, yPathString));
    // This mimics the persistence level establish by ALS training methods
    userRDD.persist(StorageLevel.MEMORY_AND_DISK());
    productRDD.persist(StorageLevel.MEMORY_AND_DISK());

    // Cast is needed for some reason with this Scala API returning array
    @SuppressWarnings("unchecked")
    Tuple2<?,double[]> first = (Tuple2<?,double[]>) ((Object[]) userRDD.take(1))[0];
    int rank = first._2().length;
    return new MatrixFactorizationModel(
        rank, massageToObjectKey(userRDD), massageToObjectKey(productRDD));
  }

  private static RDD<Tuple2<Integer,double[]>> readFeaturesRDD(JavaSparkContext sparkContext,
                                                              Path path) {
    log.info("Loading features RDD from {}", path);
    JavaRDD<String> featureLines = sparkContext.textFile(path.toString());
    return featureLines.map(new Function<String,Tuple2<Integer,double[]>>() {
      @Override
      public Tuple2<Integer,double[]> call(String line) throws IOException {
        List<?> update = MAPPER.readValue(line, List.class);
        Integer key = Integer.valueOf(update.get(0).toString());
        double[] vector = MAPPER.convertValue(update.get(1), double[].class);
        return new Tuple2<>(key, vector);
      }
    }).rdd();
  }

  private static <A,B> RDD<Tuple2<Object,B>> massageToObjectKey(RDD<Tuple2<A,B>> in) {
    // More horrible hacks to get around Scala-Java unfriendliness
    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,B>> result = (RDD<Tuple2<Object,B>>) (RDD<?>) in;
    return result;
  }

  private static JavaPairRDD<Integer,Collection<Integer>> knownsRDD(JavaRDD<String[]> allData,
                                                                    final boolean knownItems) {
    JavaRDD<String[]> sorted = allData.sortBy(
        new Function<String[], Long>() {
          @Override
          public Long call(String[] datum) {
            return Long.valueOf(datum[3]);
          }
        }, true, allData.partitions().size());

    JavaPairRDD<Integer,Tuple2<Integer,Boolean>> tuples = sorted.mapToPair(
        new PairFunction<String[],Integer,Tuple2<Integer,Boolean>>() {
          @Override
          public Tuple2<Integer,Tuple2<Integer,Boolean>> call(String[] datum) {
            Integer user = Integer.valueOf(datum[0]);
            Integer item = Integer.valueOf(datum[1]);
            Boolean delete = datum[2].isEmpty();
            return knownItems ?
                new Tuple2<>(user, new Tuple2<>(item, delete)) :
                new Tuple2<>(item, new Tuple2<>(user, delete));
          }
        });

    // TODO likely need to figure out a way to avoid groupByKey but collectByKey
    // won't work here -- doesn't guarantee enough about ordering
    return tuples.groupByKey().mapValues(
        new Function<Iterable<Tuple2<Integer, Boolean>>, Collection<Integer>>() {
          @Override
          public Collection<Integer> call(Iterable<Tuple2<Integer, Boolean>> idDeletes) {
            Collection<Integer> ids = new HashSet<>();
            for (Tuple2<Integer, Boolean> idDelete : idDeletes) {
              if (idDelete._2()) {
                ids.remove(idDelete._1());
              } else {
                ids.add(idDelete._1());
              }
            }
            return ids;
          }
        });
  }

  private static <K,V> JavaPairRDD<K,V> fromRDD(RDD<Tuple2<K,V>> rdd) {
    return JavaPairRDD.fromRDD(rdd,
                               ClassTag$.MODULE$.<K>apply(Object.class),
                               ClassTag$.MODULE$.<V>apply(Object.class));
  }

}
