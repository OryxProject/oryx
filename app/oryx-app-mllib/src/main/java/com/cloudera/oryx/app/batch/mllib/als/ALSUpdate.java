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

package com.cloudera.oryx.app.batch.mllib.als;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
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

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.param.HyperParamValues;
import com.cloudera.oryx.ml.param.HyperParams;

/**
 * <p>A specialization of {@link MLUpdate} that creates a matrix factorization model of its
 * input, using the Alternating Least Squares algorithm.</p>
 *
 * <p>The implementation is built on Spark MLlib's implementation of ALS, which is in turn
 * based on the paper
 * <a href="http://yifanhu.net/PUB/cf.pdf">Collaborative Filtering for Implicit Feedback Datasets</a>.
 * The parameters used below and in the configuration follow this paper as given.</p>
 *
 * <p>Note that this also adds support for log transformation of strength values, as suggested
 * in Equation 6 of the paper.</p>
 */
public final class ALSUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSUpdate.class);

  private final int iterations;
  private final boolean implicit;
  private final boolean logStrength;
  private final List<HyperParamValues<?>> hyperParamValues;
  private final boolean noKnownItems;
  private final double decayFactor;
  private final double decayZeroThreshold;

  public ALSUpdate(Config config) {
    super(config);
    iterations = config.getInt("oryx.als.iterations");
    implicit = config.getBoolean("oryx.als.implicit");
    logStrength = config.getBoolean("oryx.als.logStrength");
    Preconditions.checkArgument(iterations > 0);
    hyperParamValues = new ArrayList<>(Arrays.asList(
        HyperParams.fromConfig(config, "oryx.als.hyperparams.features"),
        HyperParams.fromConfig(config, "oryx.als.hyperparams.lambda"),
        HyperParams.fromConfig(config, "oryx.als.hyperparams.alpha")));
    if (logStrength) {
      hyperParamValues.add(HyperParams.fromConfig(config, "oryx.als.hyperparams.epsilon"));
    }
    noKnownItems = config.getBoolean("oryx.als.no-known-items");
    decayFactor = config.getDouble("oryx.als.decay.factor");
    decayZeroThreshold = config.getDouble("oryx.als.decay.zero-threshold");
    Preconditions.checkArgument(iterations > 0);
    Preconditions.checkArgument(decayFactor > 0.0 && decayFactor <= 1.0);
    Preconditions.checkArgument(decayZeroThreshold >= 0.0);
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
    double epsilon = Double.NaN;
    if (logStrength) {
      epsilon = (Double) hyperParameters.get(3);
    }
    Preconditions.checkArgument(features > 0);
    Preconditions.checkArgument(lambda >= 0.0);
    Preconditions.checkArgument(alpha > 0.0);
    if (logStrength) {
      Preconditions.checkArgument(epsilon > 0.0);
    }

    JavaRDD<String[]> parsedRDD = trainData.map(MLFunctions.PARSE_FN);
    parsedRDD.cache();

    Map<String,Integer> userIDIndexMap = buildIDIndexMapping(parsedRDD, true);
    Map<String,Integer> itemIDIndexMap = buildIDIndexMapping(parsedRDD, false);

    log.info("Broadcasting ID-index mappings for {} users, {} items",
             userIDIndexMap.size(), itemIDIndexMap.size());

    Broadcast<Map<String,Integer>> bUserIDToIndex = sparkContext.broadcast(userIDIndexMap);
    Broadcast<Map<String,Integer>> bItemIDToIndex = sparkContext.broadcast(itemIDIndexMap);

    JavaRDD<Rating> trainRatingData = parsedToRatingRDD(parsedRDD, bUserIDToIndex, bItemIDToIndex);
    trainRatingData = aggregateScores(trainRatingData, epsilon);
    ALS als = new ALS()
        .setRank(features)
        .setIterations(iterations)
        .setLambda(lambda)
        .setCheckpointInterval(5);
    if (implicit) {
      als = als.setImplicitPrefs(true).setAlpha(alpha);
    }

    RDD<Rating> trainingRatingDataRDD = trainRatingData.rdd();
    trainingRatingDataRDD.cache();
    MatrixFactorizationModel model = als.run(trainingRatingDataRDD);
    trainingRatingDataRDD.unpersist(false);

    bUserIDToIndex.unpersist();
    bItemIDToIndex.unpersist();

    parsedRDD.unpersist();

    Broadcast<Map<Integer,String>> bUserIndexToID = sparkContext.broadcast(invertMap(userIDIndexMap));
    Broadcast<Map<Integer,String>> bItemIndexToID = sparkContext.broadcast(invertMap(itemIDIndexMap));

    PMML pmml = mfModelToPMML(model,
                              features,
                              lambda,
                              alpha,
                              epsilon,
                              implicit,
                              logStrength,
                              candidatePath,
                              bUserIndexToID,
                              bItemIndexToID);
    unpersist(model);

    bUserIndexToID.unpersist();
    bItemIndexToID.unpersist();

    return pmml;
  }

  private static Map<String,Integer> buildIDIndexMapping(JavaRDD<String[]> parsedRDD,
                                                         boolean user) {
    int offset = user ? 0 : 1;
    Map<String,Integer> reverseIDLookup = parsedRDD.map(tokens -> tokens[offset])
        .distinct().sortBy(s -> s, true, parsedRDD.getNumPartitions())
        .zipWithIndex().mapValues(Long::intValue)
        .collectAsMap();
    // Clone, due to some serialization problems with the result of collectAsMap?
    return new HashMap<>(reverseIDLookup);
  }

  private static <K,V> Map<V,K> invertMap(Map<K,V> map) {
    Map<V,K> inverse = new HashMap<>(map.size());
    for (Map.Entry<K,V> entry : map.entrySet()) {
      inverse.put(entry.getValue(), entry.getKey());
    }
    return inverse;
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData,
                         JavaRDD<String> trainData) {

    JavaRDD<String[]> parsedTestRDD = testData.map(MLFunctions.PARSE_FN);
    parsedTestRDD.cache();

    Map<String,Integer> userIDToIndex = buildIDIndexOneWayMap(model, parsedTestRDD, true);
    Map<String,Integer> itemIDToIndex = buildIDIndexOneWayMap(model, parsedTestRDD, false);

    log.info("Broadcasting ID-index mappings for {} users, {} items",
             userIDToIndex.size(), itemIDToIndex.size());

    Broadcast<Map<String,Integer>> bUserIDToIndex = sparkContext.broadcast(userIDToIndex);
    Broadcast<Map<String,Integer>> bItemIDToIndex = sparkContext.broadcast(itemIDToIndex);

    JavaRDD<Rating> testRatingData = parsedToRatingRDD(parsedTestRDD, bUserIDToIndex, bItemIDToIndex);
    double epsilon = Double.NaN;
    if (logStrength) {
      epsilon = Double.parseDouble(AppPMMLUtils.getExtensionValue(model, "epsilon"));
    }
    testRatingData = aggregateScores(testRatingData, epsilon);

    MatrixFactorizationModel mfModel =
        pmmlToMFModel(sparkContext, model, modelParentPath, bUserIDToIndex, bItemIDToIndex);

    parsedTestRDD.unpersist();

    double eval;
    if (implicit) {
      double auc = Evaluation.areaUnderCurve(sparkContext, mfModel, testRatingData);
      log.info("AUC: {}", auc);
      eval = auc;
    } else {
      double rmse = Evaluation.rmse(mfModel, testRatingData);
      log.info("RMSE: {}", rmse);
      eval = -rmse;
    }
    unpersist(mfModel);

    bUserIDToIndex.unpersist();
    bItemIDToIndex.unpersist();

    return eval;
  }

  private static Map<String,Integer> buildIDIndexOneWayMap(PMML model,
                                                           JavaRDD<String[]> parsedTestRDD,
                                                           boolean user) {
    // Add to mapping everything from the model
    List<String> ids = AppPMMLUtils.getExtensionContent(model, user ? "XIDs" : "YIDs");
    Map<String,Integer> idIndex = new HashMap<>(ids.size());
    int index = 0;
    for (String id : ids) {
      idIndex.put(id, index++);
    }

    // And from the test set, which may have a few more IDs
    int offset = user ? 0 : 1;
    for (String id : parsedTestRDD.map(tokens -> tokens[offset]).distinct().collect()) {
      if (!idIndex.containsKey(id)) {
        idIndex.put(id, index++);
      }
    }

    return idIndex;
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
  public boolean canPublishAdditionalModelData() {
    return true;
  }

  @Override
  public void publishAdditionalModelData(JavaSparkContext sparkContext,
                                         PMML pmml,
                                         JavaRDD<String> newData,
                                         JavaRDD<String> pastData,
                                         Path modelParentPath,
                                         TopicProducer<String, String> modelUpdateTopic) {
    // Send item updates first, before users. That way, user-based endpoints like /recommend
    // may take longer to not return 404, but when they do, the result will be more complete.
    log.info("Sending item / Y data as model updates");
    String yPathString = AppPMMLUtils.getExtensionValue(pmml, "Y");
    JavaPairRDD<String,float[]> productRDD = readFeaturesRDD(sparkContext, new Path(modelParentPath, yPathString));

    String updateBroker = modelUpdateTopic.getUpdateBroker();
    String topic = modelUpdateTopic.getTopic();

    // For now, there is no use in sending known users for each item
    productRDD.foreachPartition(new EnqueueFeatureVecsFn("Y", updateBroker, topic));

    log.info("Sending user / X data as model updates");
    String xPathString = AppPMMLUtils.getExtensionValue(pmml, "X");
    JavaPairRDD<String,float[]> userRDD = readFeaturesRDD(sparkContext, new Path(modelParentPath, xPathString));

    if (noKnownItems) {
      userRDD.foreachPartition(new EnqueueFeatureVecsFn("X", updateBroker, topic));
    } else {
      log.info("Sending known item data with model updates");
      JavaRDD<String[]> allData =
          (pastData == null ? newData : newData.union(pastData)).map(MLFunctions.PARSE_FN);
      JavaPairRDD<String,Collection<String>> knownItems = knownsRDD(allData, true);
      userRDD.join(knownItems).foreachPartition(
          new EnqueueFeatureVecsAndKnownItemsFn("X", updateBroker, topic));
    }
  }

  /**
   * Implementation which splits based solely on time. It will return approximately
   * the earliest {@link #getTestFraction()} of input, ordered by timestamp, as new training
   * data and the rest as test data.
   */
  @Override
  protected Pair<JavaRDD<String>,JavaRDD<String>> splitNewDataToTrainTest(JavaRDD<String> newData) {
    // Rough approximation; assumes timestamps are fairly evenly distributed
    StatCounter maxMin = newData.mapToDouble(line -> MLFunctions.TO_TIMESTAMP_FN.call(line).doubleValue()).stats();

    long minTime = (long) maxMin.min();
    long maxTime = (long) maxMin.max();
    log.info("New data timestamp range: {} - {}", minTime, maxTime);
    long approxTestTrainBoundary = (long) (maxTime - getTestFraction() * (maxTime - minTime));
    log.info("Splitting at timestamp {}", approxTestTrainBoundary);

    JavaRDD<String> newTrainData = newData.filter(
        line -> MLFunctions.TO_TIMESTAMP_FN.call(line) < approxTestTrainBoundary);
    JavaRDD<String> testData = newData.filter(
        line -> MLFunctions.TO_TIMESTAMP_FN.call(line) >= approxTestTrainBoundary);

    return new Pair<>(newTrainData, testData);
  }

  /**
   * @param parsedRDD parsed input as {@code String[]}
   * @return {@link Rating}s ordered by timestamp
   */
  private JavaRDD<Rating> parsedToRatingRDD(JavaRDD<String[]> parsedRDD,
                                            Broadcast<Map<String,Integer>> bUserIDToIndex,
                                            Broadcast<Map<String,Integer>> bItemIDToIndex) {
    JavaPairRDD<Long,Rating> timestampRatingRDD = parsedRDD.mapToPair(tokens -> {
      try {
        return new Tuple2<>(
            Long.valueOf(tokens[3]),
            new Rating(bUserIDToIndex.value().get(tokens[0]),
                       bItemIDToIndex.value().get(tokens[1]),
                       // Empty value means 'delete'; propagate as NaN
                       tokens[2].isEmpty() ? Double.NaN : Double.parseDouble(tokens[2])));
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", Arrays.toString(tokens));
        throw e;
      }
    });

    if (decayFactor < 1.0) {
      double factor = decayFactor;
      long now = System.currentTimeMillis();
      timestampRatingRDD = timestampRatingRDD.mapToPair(timestampRating -> {
          long timestamp = timestampRating._1();
          return new Tuple2<>(timestamp, decayRating(timestampRating._2(), timestamp, now, factor));
        });
    }

    if (decayZeroThreshold > 0.0) {
      double theThreshold = decayZeroThreshold;
      timestampRatingRDD = timestampRatingRDD.filter(timestampRating -> timestampRating._2().rating() > theThreshold);
    }

    return timestampRatingRDD.sortByKey().values();
  }

  static Rating decayRating(Rating rating, long timestamp, long now, double factor) {
    if (timestamp >= now) {
      return rating;
    }
    double days = (now - timestamp) / 86400000.0;
    return new Rating(rating.user(), rating.product(), rating.rating() * Math.pow(factor, days));
  }

  /**
   * Combines {@link Rating}s with the same user/item into one, with score as the sum of
   * all of the scores.
   */
  private JavaRDD<Rating> aggregateScores(JavaRDD<Rating> original, double epsilon) {
    JavaPairRDD<Tuple2<Integer,Integer>,Double> tuples =
        original.mapToPair(rating -> new Tuple2<>(new Tuple2<>(rating.user(), rating.product()), rating.rating()));

    JavaPairRDD<Tuple2<Integer,Integer>,Double> aggregated;
    if (implicit) {
      // TODO can we avoid groupByKey? reduce, combine, fold don't seem viable since
      // they don't guarantee the delete elements are properly handled
      aggregated = tuples.groupByKey().mapValues(MLFunctions.SUM_WITH_NAN);
    } else {
      // For non-implicit, last wins.
      aggregated = tuples.foldByKey(Double.NaN, (current, next) -> next);
    }

    JavaPairRDD<Tuple2<Integer,Integer>,Double> noNaN =
        aggregated.filter(kv -> !Double.isNaN(kv._2()));

    if (logStrength) {
      return noNaN.map(userProductScore -> new Rating(
          userProductScore._1()._1(),
          userProductScore._1()._2(),
          Math.log1p(userProductScore._2() / epsilon)));
    } else {
      return noNaN.map(userProductScore -> new Rating(
          userProductScore._1()._1(),
          userProductScore._1()._2(),
          userProductScore._2()));
    }
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
                                    double epsilon,
                                    boolean implicit,
                                    boolean logStrength,
                                    Path candidatePath,
                                    Broadcast<Map<Integer,String>> bUserIndexToID,
                                    Broadcast<Map<Integer,String>> bItemIndexToID) {

    Function<double[],float[]> doubleArrayToFloats = d -> {
      float[] f = new float[d.length];
      for (int i = 0; i < f.length; i++) {
        f[i] = (float) d[i];
      }
      return f;
    };

    JavaPairRDD<Integer,float[]> userFeaturesRDD =
        massageToIntKey(model.userFeatures()).mapValues(doubleArrayToFloats);
    JavaPairRDD<Integer,float[]> itemFeaturesRDD =
        massageToIntKey(model.productFeatures()).mapValues(doubleArrayToFloats);

    saveFeaturesRDD(userFeaturesRDD, new Path(candidatePath, "X"), bUserIndexToID);
    saveFeaturesRDD(itemFeaturesRDD, new Path(candidatePath, "Y"), bItemIndexToID);

    PMML pmml = PMMLUtils.buildSkeletonPMML();
    AppPMMLUtils.addExtension(pmml, "X", "X/");
    AppPMMLUtils.addExtension(pmml, "Y", "Y/");
    AppPMMLUtils.addExtension(pmml, "features", features);
    AppPMMLUtils.addExtension(pmml, "lambda", lambda);
    AppPMMLUtils.addExtension(pmml, "implicit", implicit);
    if (implicit) {
      AppPMMLUtils.addExtension(pmml, "alpha", alpha);
    }
    AppPMMLUtils.addExtension(pmml, "logStrength", logStrength);
    if (logStrength) {
      AppPMMLUtils.addExtension(pmml, "epsilon", epsilon);
    }
    addIDsExtension(pmml, "XIDs", userFeaturesRDD, bUserIndexToID.value());
    addIDsExtension(pmml, "YIDs", itemFeaturesRDD, bItemIndexToID.value());
    return pmml;
  }

  private static <A,B> JavaPairRDD<Integer,B> massageToIntKey(RDD<Tuple2<A,B>> in) {
    // More horrible hacks to get around Scala-Java unfriendliness
    @SuppressWarnings("unchecked")
    JavaPairRDD<Integer,B> javaRDD = fromRDD((RDD<Tuple2<Integer,B>>) (RDD<?>) in);
    return javaRDD;
  }

  private static void addIDsExtension(PMML pmml,
                                      String key,
                                      JavaPairRDD<Integer,?> features,
                                      Map<Integer,String> indexToID) {
    List<String> ids = features.keys().collect().stream().map(indexToID::get).collect(Collectors.toList());
    AppPMMLUtils.addExtensionContent(pmml, key, ids);
  }

  private static void saveFeaturesRDD(JavaPairRDD<Integer,float[]> features,
                                      Path path,
                                      Broadcast<Map<Integer,String>> bIndexToID) {
    log.info("Saving features RDD to {}", path);
    features.map(keyAndVector -> {
      String id = bIndexToID.value().get(keyAndVector._1());
      float[] vector = keyAndVector._2();
      return TextUtils.joinJSON(Arrays.asList(id, vector));
    }).saveAsTextFile(path.toString(), GzipCodec.class);
  }

  private static MatrixFactorizationModel pmmlToMFModel(JavaSparkContext sparkContext,
                                                        PMML pmml,
                                                        Path modelParentPath,
                                                        Broadcast<Map<String,Integer>> bUserIDToIndex,
                                                        Broadcast<Map<String,Integer>> bItemIDToIndex) {
    String xPathString = AppPMMLUtils.getExtensionValue(pmml, "X");
    String yPathString = AppPMMLUtils.getExtensionValue(pmml, "Y");
    JavaPairRDD<String,float[]> userRDD = readFeaturesRDD(sparkContext, new Path(modelParentPath, xPathString));
    JavaPairRDD<String,float[]> productRDD = readFeaturesRDD(sparkContext, new Path(modelParentPath, yPathString));
    int rank = userRDD.first()._2().length;
    return new MatrixFactorizationModel(
        rank,
        readAndConvertFeatureRDD(userRDD, bUserIDToIndex),
        readAndConvertFeatureRDD(productRDD, bItemIDToIndex));
  }

  private static RDD<Tuple2<Object,double[]>> readAndConvertFeatureRDD(
      JavaPairRDD<String,float[]> javaRDD,
      Broadcast<Map<String,Integer>> bIdToIndex) {

    RDD<Tuple2<Integer,double[]>> scalaRDD = javaRDD.mapToPair(t ->
        new Tuple2<>(bIdToIndex.value().get(t._1()), t._2())
    ).mapValues(f -> {
        double[] d = new double[f.length];
        for (int i = 0; i < d.length; i++) {
          d[i] = f[i];
        }
        return d;
      }
    ).rdd();

    // This mimics the persistence level establish by ALS training methods
    scalaRDD.persist(StorageLevel.MEMORY_AND_DISK());

    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,double[]>> objKeyRDD = (RDD<Tuple2<Object,double[]>>) (RDD<?>) scalaRDD;
    return objKeyRDD;
  }

  private static JavaPairRDD<String,float[]> readFeaturesRDD(JavaSparkContext sparkContext, Path path) {
    log.info("Loading features RDD from {}", path);
    JavaRDD<String> featureLines = sparkContext.textFile(path.toString());
    return featureLines.mapToPair(line -> {
      List<?> update = TextUtils.readJSON(line, List.class);
      String key = update.get(0).toString();
      float[] vector = TextUtils.convertViaJSON(update.get(1), float[].class);
      return new Tuple2<>(key, vector);
    });
  }

  private static JavaPairRDD<String,Collection<String>> knownsRDD(JavaRDD<String[]> allData,
                                                                  boolean knownItems) {
    JavaRDD<String[]> sorted = allData.sortBy(datum -> Long.valueOf(datum[3]), true, allData.partitions().size());

    JavaPairRDD<String,Tuple2<String,Boolean>> tuples = sorted.mapToPair(datum -> {
        String user = datum[0];
        String item = datum[1];
        Boolean delete = datum[2].isEmpty();
        return knownItems ?
            new Tuple2<>(user, new Tuple2<>(item, delete)) :
            new Tuple2<>(item, new Tuple2<>(user, delete));
      });

    // TODO likely need to figure out a way to avoid groupByKey but collectByKey
    // won't work here -- doesn't guarantee enough about ordering
    return tuples.groupByKey().mapValues(idDeletes -> {
        Collection<String> ids = new HashSet<>();
        for (Tuple2<String,Boolean> idDelete : idDeletes) {
          if (idDelete._2()) {
            ids.remove(idDelete._1());
          } else {
            ids.add(idDelete._1());
          }
        }
        return ids;
      });
  }

  private static <K,V> JavaPairRDD<K,V> fromRDD(RDD<Tuple2<K,V>> rdd) {
    return JavaPairRDD.fromRDD(rdd,
                               ClassTag$.MODULE$.apply(Object.class),
                               ClassTag$.MODULE$.apply(Object.class));
  }

}
