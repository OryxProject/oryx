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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.app.common.fn.MLFunctions;
import com.cloudera.oryx.app.pmml.AppPMMLUtils;
import com.cloudera.oryx.common.collection.Pair;
import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.lambda.Functions;
import com.cloudera.oryx.ml.MLUpdate;
import com.cloudera.oryx.ml.param.HyperParamValues;
import com.cloudera.oryx.ml.param.HyperParams;

/**
 * A specialization of {@link MLUpdate} that creates a matrix factorization model of its
 * input, using the Alternating Least Squares algorithm.
 */
public final class ALSUpdate extends MLUpdate<String> {

  private static final Logger log = LoggerFactory.getLogger(ALSUpdate.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HashFunction HASH = Hashing.md5();
  private static final Pattern MOST_INTS_PATTERN = Pattern.compile("(0|-?[1-9][0-9]{0,9})");

  private final int iterations;
  private final boolean implicit;
  private final List<HyperParamValues<?>> hyperParamValues;
  private final boolean noKnownItems;
  private final double decayFactor;
  private final double decayZeroThreshold;

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
    Preconditions.checkArgument(features > 0);
    Preconditions.checkArgument(lambda >= 0.0);
    Preconditions.checkArgument(alpha > 0.0);

    JavaRDD<String[]> parsedRDD = trainData.map(MLFunctions.PARSE_FN);

    JavaRDD<Rating> trainRatingData = parsedToRatingRDD(parsedRDD);
    trainRatingData = aggregateScores(trainRatingData);
    MatrixFactorizationModel model;
    if (implicit) {
      model = ALS.trainImplicit(trainRatingData.rdd(), features, iterations, lambda, alpha);
    } else {
      model = ALS.train(trainRatingData.rdd(), features, iterations, lambda);
    }

    Map<Integer,String> reverseIDLookup = parsedRDD.
        flatMapToPair(new ToReverseLookupFn()).
        reduceByKey(Functions.<String>last()).collectAsMap();
    // Clone, due to some serialization problems with the result of collectAsMap?
    reverseIDLookup = new HashMap<>(reverseIDLookup);

    PMML pmml = mfModelToPMML(model,
                              features,
                              lambda,
                              alpha,
                              implicit,
                              candidatePath,
                              reverseIDLookup);
    unpersist(model);
    return pmml;
  }

  @Override
  public double evaluate(JavaSparkContext sparkContext,
                         PMML model,
                         Path modelParentPath,
                         JavaRDD<String> testData,
                         JavaRDD<String> trainData) {
    JavaRDD<Rating> testRatingData = parsedToRatingRDD(testData.map(MLFunctions.PARSE_FN));
    testRatingData = aggregateScores(testRatingData);
    MatrixFactorizationModel mfModel = pmmlToMFModel(sparkContext, model, modelParentPath);
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

    JavaRDD<String[]> allData =
        (pastData == null ? newData : newData.union(pastData)).map(MLFunctions.PARSE_FN);

    log.info("Sending user / X data as model updates");
    String xPathString = AppPMMLUtils.getExtensionValue(pmml, "X");
    JavaPairRDD<String,double[]> userRDD =
        readFeaturesRDD(sparkContext, new Path(modelParentPath, xPathString));

    String updateBroker = modelUpdateTopic.getUpdateBroker();
    String topic = modelUpdateTopic.getTopic();
    if (noKnownItems) {
      userRDD.foreachPartition(new EnqueueFeatureVecsFn("X", updateBroker, topic));
    } else {
      log.info("Sending known item data with model updates");
      JavaPairRDD<String,Collection<String>> knownItems = knownsRDD(allData, true);
      userRDD.join(knownItems).foreachPartition(
          new EnqueueFeatureVecsAndKnownItemsFn("X", updateBroker, topic));
    }

    log.info("Sending item / Y data as model updates");
    String yPathString = AppPMMLUtils.getExtensionValue(pmml, "Y");
    JavaPairRDD<String,double[]> productRDD =
        readFeaturesRDD(sparkContext, new Path(modelParentPath, yPathString));

    // For now, there is no use in sending known users for each item
    productRDD.foreachPartition(new EnqueueFeatureVecsFn("Y", updateBroker, topic));
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
    final long approxTestTrainBoundary = (long) (maxTime - getTestFraction() * (maxTime - minTime));
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
  private JavaRDD<Rating> parsedToRatingRDD(JavaRDD<String[]> parsedRDD) {
    JavaPairRDD<Long,Rating> timestampRatingRDD = parsedRDD.mapToPair(new ParseRatingFn());

    if (decayFactor < 1.0) {
      final double factor = decayFactor;
      final long now = System.currentTimeMillis();
      timestampRatingRDD = timestampRatingRDD.mapToPair(
          new PairFunction<Tuple2<Long,Rating>,Long,Rating>() {
            @Override
            public Tuple2<Long,Rating> call(Tuple2<Long,Rating> timestampRating) {
              long timestamp = timestampRating._1();
              Rating rating = timestampRating._2();
              double newRating;
              if (timestamp >= now) {
                newRating = rating.rating();
              } else {
                double days = (now - timestamp) / 86400000.0;
                newRating = rating.rating() * Math.pow(factor, days);
              }
              return new Tuple2<>(timestamp,
                                  new Rating(rating.user(), rating.product(), newRating));
            }
          });
    }

    if (decayZeroThreshold > 0.0) {
      final double theThreshold = decayZeroThreshold;
      timestampRatingRDD = timestampRatingRDD.filter(new Function<Tuple2<Long,Rating>,Boolean>() {
        @Override
        public Boolean call(Tuple2<Long,Rating> timestampRating) {
          return timestampRating._2().rating() > theThreshold;
        }
      });
    }

    return timestampRatingRDD.sortByKey().values();
  }

  private static final class ToReverseLookupFn
      implements PairFlatMapFunction<String[],Integer,String> {
    @Override
    public Iterable<Tuple2<Integer,String>> call(String[] tokens) {
      List<Tuple2<Integer,String>> results = new ArrayList<>(2);
      for (int i = 0; i < 2; i++) {
        String s = tokens[i];
        if (MOST_INTS_PATTERN.matcher(s).matches()) {
          try {
            Integer.parseInt(s);
            continue;
          } catch (NumberFormatException nfe) {
            // continue
          }
        }
        results.add(new Tuple2<>(hash(s), s));
      }
      return results;
    }
  }

  /**
   * @param s string to hash
   * @return top 32 bits of MD5 hash of UTF-8 encoding of the string,
   *  with 0 top bit (so result is nonnegative)
   */
  private static int hash(String s) {
    return HASH.hashString(s, StandardCharsets.UTF_8).asInt() & 0x7FFFFFFF;
  }

  private static final class ParseRatingFn implements PairFunction<String[],Long,Rating> {
    @Override
    public Tuple2<Long,Rating> call(String[] tokens) {
      try {
        return new Tuple2<>(
            Long.valueOf(tokens[3]),
            new Rating(parseOrHashInt(tokens[0]),
                       parseOrHashInt(tokens[1]),
                       // Empty value means 'delete'; propagate as NaN
                       tokens[2].isEmpty() ? Double.NaN : Double.parseDouble(tokens[2])));
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        log.warn("Bad input: {}", Arrays.toString(tokens));
        throw e;
      }
    }
  }

  /**
   * @param s value to parse or hash as an {@link Integer}
   * @return {@link Integer} value of argument if it parses as an {@link Integer}, or else
   *  the hash according to {@link #hash(String)}
   */
  private static int parseOrHashInt(String s) {
    // Optimization: matches all ints (and then some) and is cheaper than the frequent exceptions
    // generated by trying to parse everything
    if (MOST_INTS_PATTERN.matcher(s).matches()) {
      // may or may not parseable as int
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException nfe) {
        // number a bit too large/small for an int, so hash anyway
      }
    }
    return hash(s);
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
                                    Path candidatePath,
                                    Map<Integer,String> reverseIDMapping) {

    JavaPairRDD<Integer,double[]> userFeaturesRDD = massageToIntKey(model.userFeatures());
    JavaPairRDD<Integer,double[]> itemFeaturesRDD = massageToIntKey(model.productFeatures());

    saveFeaturesRDD(userFeaturesRDD, new Path(candidatePath, "X"), reverseIDMapping);
    saveFeaturesRDD(itemFeaturesRDD, new Path(candidatePath, "Y"), reverseIDMapping);

    PMML pmml = PMMLUtils.buildSkeletonPMML();
    AppPMMLUtils.addExtension(pmml, "X", "X/");
    AppPMMLUtils.addExtension(pmml, "Y", "Y/");
    AppPMMLUtils.addExtension(pmml, "features", features);
    AppPMMLUtils.addExtension(pmml, "lambda", lambda);
    AppPMMLUtils.addExtension(pmml, "implicit", implicit);
    if (implicit) {
      AppPMMLUtils.addExtension(pmml, "alpha", alpha);
    }
    addIDsExtension(pmml, "XIDs", userFeaturesRDD, reverseIDMapping);
    addIDsExtension(pmml, "YIDs", itemFeaturesRDD, reverseIDMapping);
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
                                      JavaPairRDD<Integer,double[]> features,
                                      Map<Integer,String> reverseIDMapping) {
    List<Integer> hashedIDs = features.keys().collect();
    List<String> ids = new ArrayList<>(hashedIDs.size());
    for (Integer hashedID : hashedIDs) {
      String originalID = reverseIDMapping.get(hashedID);
      ids.add(originalID == null ? hashedID.toString() : originalID);
    }
    AppPMMLUtils.addExtensionContent(pmml, key, ids);
  }

  private static void saveFeaturesRDD(JavaPairRDD<Integer,double[]> features,
                                      Path path,
                                      final Map<Integer,String> reverseIDMapping) {
    log.info("Saving features RDD to {}", path);
    features.map(new Function<Tuple2<Integer, double[]>, String>() {
      @Override
      public String call(Tuple2<Integer, double[]> keyAndVector) {
        Integer id = keyAndVector._1();
        String originalKey = reverseIDMapping.get(id);
        Object key = originalKey == null ? id : originalKey;
        double[] vector = keyAndVector._2();
        return TextUtils.joinJSON(Arrays.asList(key, vector));
      }
    }).saveAsTextFile(path.toString(), GzipCodec.class);
  }

  private static MatrixFactorizationModel pmmlToMFModel(JavaSparkContext sparkContext,
                                                        PMML pmml,
                                                        Path modelParentPath) {
    String xPathString = AppPMMLUtils.getExtensionValue(pmml, "X");
    String yPathString = AppPMMLUtils.getExtensionValue(pmml, "Y");
    JavaPairRDD<String,double[]> userRDD =
        readFeaturesRDD(sparkContext, new Path(modelParentPath, xPathString));
    JavaPairRDD<String,double[]> productRDD =
        readFeaturesRDD(sparkContext, new Path(modelParentPath, yPathString));
    int rank = userRDD.first()._2().length;
    return new MatrixFactorizationModel(
        rank, readAndConvertFeatureRDD(userRDD), readAndConvertFeatureRDD(productRDD));
  }

  private static RDD<Tuple2<Object,double[]>> readAndConvertFeatureRDD(
      JavaPairRDD<String,double[]> javaRDD) {

    RDD<Tuple2<Integer,double[]>> scalaRDD = javaRDD.mapToPair(
        new PairFunction<Tuple2<String,double[]>,Integer,double[]>() {
          @Override
          public Tuple2<Integer,double[]> call(Tuple2<String, double[]> t) {
            return new Tuple2<>(parseOrHashInt(t._1()), t._2());
          }
        }).rdd();

    // This mimics the persistence level establish by ALS training methods
    scalaRDD.persist(StorageLevel.MEMORY_AND_DISK());

    @SuppressWarnings("unchecked")
    RDD<Tuple2<Object,double[]>> objKeyRDD = (RDD<Tuple2<Object,double[]>>) (RDD<?>) scalaRDD;
    return objKeyRDD;
  }

  private static JavaPairRDD<String,double[]> readFeaturesRDD(JavaSparkContext sparkContext,
                                                              Path path) {
    log.info("Loading features RDD from {}", path);
    JavaRDD<String> featureLines = sparkContext.textFile(path.toString());
    return featureLines.mapToPair(new PairFunction<String,String,double[]>() {
      @Override
      public Tuple2<String, double[]> call(String line) throws IOException {
        List<?> update = MAPPER.readValue(line, List.class);
        String key = update.get(0).toString();
        double[] vector = MAPPER.convertValue(update.get(1), double[].class);
        return new Tuple2<>(key, vector);
      }
    });
  }

  private static JavaPairRDD<String,Collection<String>> knownsRDD(JavaRDD<String[]> allData,
                                                                  final boolean knownItems) {
    JavaRDD<String[]> sorted = allData.sortBy(
        new Function<String[], Long>() {
          @Override
          public Long call(String[] datum) {
            return Long.valueOf(datum[3]);
          }
        }, true, allData.partitions().size());

    JavaPairRDD<String,Tuple2<String,Boolean>> tuples = sorted.mapToPair(
        new PairFunction<String[],String,Tuple2<String,Boolean>>() {
          @Override
          public Tuple2<String,Tuple2<String,Boolean>> call(String[] datum) {
            String user = datum[0];
            String item = datum[1];
            Boolean delete = datum[2].isEmpty();
            return knownItems ?
                new Tuple2<>(user, new Tuple2<>(item, delete)) :
                new Tuple2<>(item, new Tuple2<>(user, delete));
          }
        });

    // TODO likely need to figure out a way to avoid groupByKey but collectByKey
    // won't work here -- doesn't guarantee enough about ordering
    return tuples.groupByKey().mapValues(
        new Function<Iterable<Tuple2<String,Boolean>>,Collection<String>>() {
          @Override
          public Collection<String> call(Iterable<Tuple2<String,Boolean>> idDeletes) {
            Collection<String> ids = new HashSet<>();
            for (Tuple2<String,Boolean> idDelete : idDeletes) {
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
