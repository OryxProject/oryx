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

package com.cloudera.oryx.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
    String modelString;
    try {
      modelString = new ObjectMapper().writeValueAsString(countDistinctOtherWords(newData.union(pastData)));
    } catch (JsonProcessingException jpe) {
      throw new IOException(jpe);
    }
    modelUpdateTopic.send("MODEL", modelString);
  }

  static Map<String,Integer> countDistinctOtherWords(JavaPairRDD<String,String> data) {
    return data.values().flatMapToPair(new PairFlatMapFunction<String, String, String>() {
      @Override
      public Iterable<Tuple2<String, String>> call(String line) {
        List<Tuple2<String, String>> result = new ArrayList<>();
        Set<String> distinctTokens = new HashSet<>(Arrays.asList(line.split(" ")));
        for (String a : distinctTokens) {
          for (String b : distinctTokens) {
            if (!a.equals(b)) {
              result.add(new Tuple2<>(a, b));
            }
          }
        }
        return result;
      }
    }).distinct().groupByKey().mapValues(new Function<Iterable<String>,Integer>() {
      @Override
      public Integer call(Iterable<String> values) {
        int count = 0;
        for (String v : values) {
          count++;
        }
        return count;
      }
    }).collectAsMap();
  }

}