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

package com.cloudera.oryx.example.speed;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;

import com.cloudera.oryx.api.speed.AbstractSpeedModelManager;
import com.cloudera.oryx.example.batch.ExampleBatchLayerUpdate;

/**
 * Also counts and emits counts of number of distinct words that occur with words.
 * Listens for updates from the Batch Layer, which give the current correct count at its
 * last run. Updates these counts approximately in response to the same data stream
 * that the Batch Layer sees, but assumes all words seen are new and distinct, which is only
 * approximately true. Emits updates of the form "word,count".
 */
public final class ExampleSpeedModelManager extends AbstractSpeedModelManager<String,String,String> {

  private final Map<String,Integer> distinctOtherWords = new HashMap<>();

  @Override
  public void consumeKeyMessage(String key, String message, Configuration hadoopConf) throws IOException {
    switch (key) {
      case "MODEL":
        @SuppressWarnings("unchecked")
        Map<String,Integer> model = (Map<String,Integer>) new ObjectMapper().readValue(message, Map.class);
        synchronized (distinctOtherWords) {
          distinctOtherWords.clear();
          model.forEach(distinctOtherWords::put);
        }
        break;
      case "UP":
        // ignore
        break;
      default:
        throw new IllegalArgumentException("Bad key " + key);
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    return ExampleBatchLayerUpdate.countDistinctOtherWords(newData).entrySet().stream().map(entry -> {
      String word = entry.getKey();
      int count = entry.getValue();
      int newCount;
      synchronized (distinctOtherWords) {
        Integer oldCount = distinctOtherWords.get(word);
        newCount = oldCount == null ? count : oldCount + count;
        distinctOtherWords.put(word, newCount);
      }
      return word + "," + newCount;
    }).collect(Collectors.toList());
  }

}