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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.speed.SpeedModelManager;

/**
 * Also counts and emits counts of number of distinct words that occur with words.
 * Listens for updates from the Batch Layer, which give the current correct count at its
 * last run. Updates these counts approximately in response to the same data stream
 * that the Batch Layer sees, but assumes all words seen are new and distinct, which is only
 * approximately true. Emits updates of the form "word,count".
 */
public final class ExampleSpeedModelManager implements SpeedModelManager<String,String,String> {

  private final Map<String,Integer> distinctOtherWords = new HashMap<>();

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator,
                      Configuration hadoopConf) throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      switch (km.getKey()) {
        case "MODEL":
          @SuppressWarnings("unchecked")
          Map<String,String> model = (Map<String,String>) new ObjectMapper().readValue(km.getMessage(), Map.class);
          synchronized (distinctOtherWords) {
            distinctOtherWords.clear();
          }
          for (Map.Entry<String,String> entry : model.entrySet()) {
            synchronized (distinctOtherWords) {
              distinctOtherWords.put(entry.getKey(), Integer.valueOf(entry.getValue()));
            }
          }
          break;
        default:
          // ignore
      }
    }
  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    List<String> updates = new ArrayList<>();
    for (Map.Entry<String,Integer> entry :
         ExampleBatchLayerUpdate.countDistinctOtherWords(newData).entrySet()) {
      String word = entry.getKey();
      int newCount;
      synchronized (distinctOtherWords) {
        newCount = distinctOtherWords.get(word) + 1;
        distinctOtherWords.put(word, newCount);
      }
      updates.add(word + "," + newCount);
    }
    return updates;
  }

  @Override
  public void close() {
    // do nothing
  }

}