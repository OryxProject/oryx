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

package com.cloudera.oryx.example.batch;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.api.batch.BatchLayerUpdate;

/**
 * Input keys are ignored. Values are treated as lines of space-separated text. The job
 * counts, for each word, the number of distinct other words that co-occur in some line
 * of text in the input. These are written as a "MODEL" update, where the word-count mapping
 * is written as a JSON string.
 */
public final class ExampleBatchLayerUpdate implements BatchLayerUpdate<String,String,String> {

  @Override
  public void runUpdate(JavaSparkContext sparkContext,
                        long timestamp,
                        JavaPairRDD<String,String> newData,
                        JavaPairRDD<String,String> pastData,
                        String modelDirString,
                        TopicProducer<String,String> modelUpdateTopic) throws IOException {
    JavaPairRDD<String,String> allData = pastData == null ? newData : newData.union(pastData);
    String modelString;
    try {
      modelString = new ObjectMapper().writeValueAsString(countDistinctOtherWords(allData));
    } catch (JsonProcessingException jpe) {
      throw new IOException(jpe);
    }
    modelUpdateTopic.send("MODEL", modelString);
  }

  public static Map<String,Integer> countDistinctOtherWords(JavaPairRDD<String,String> data) {
    return data.values().flatMapToPair(line -> {
      Set<String> distinctTokens = new HashSet<>(Arrays.asList(line.split(" ")));
      return distinctTokens.stream().flatMap(a ->
        distinctTokens.stream().filter(b -> !a.equals(b)).map(b -> new Tuple2<>(a, b))
      ).iterator();
    }).distinct().mapValues(a -> 1).reduceByKey((c1, c2) -> c1 + c2).collectAsMap();
  }

}