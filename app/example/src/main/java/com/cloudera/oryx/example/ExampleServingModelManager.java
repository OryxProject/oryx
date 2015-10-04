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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.oryx.api.KeyMessage;
import com.cloudera.oryx.api.serving.AbstractServingModelManager;
import com.cloudera.oryx.api.serving.ServingModel;

/**
 * Reads models and updates produced by the Batch Layer and Speed Layer. Models are maps, encoded as JSON
 * strings, mapping words to count of distinct other words that appear with that word in an input line.
 * Updates are "word,count" pairs representing new counts for a word. This class manages and exposes the
 * mapping to the Serving Layer applications.
 */
public final class ExampleServingModelManager extends AbstractServingModelManager<String> {

  private final Map<String,Integer> distinctOtherWords =
      Collections.synchronizedMap(new HashMap<String,Integer>());

  public ExampleServingModelManager(Config config) {
    super(config);
  }

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator, Configuration hadoopConf) throws IOException {
    while (updateIterator.hasNext()) {
      KeyMessage<String,String> km = updateIterator.next();
      String key = km.getKey();
      String message = km.getMessage();
      switch (key) {
        case "MODEL":
          @SuppressWarnings("unchecked")
          Map<String,String> model = (Map<String,String>) new ObjectMapper().readValue(message, Map.class);
          distinctOtherWords.keySet().retainAll(model.keySet());
          for (Map.Entry<String,String> entry : model.entrySet()) {
            distinctOtherWords.put(entry.getKey(), Integer.valueOf(entry.getValue()));
          }
          break;
        case "UP":
          String[] wordCount = message.split(",");
          distinctOtherWords.put(wordCount[0], Integer.valueOf(wordCount[1]));
          break;
        default:
          throw new IllegalArgumentException("Unknown key " + key);
      }
    }
  }

  @Override
  public ServingModel getModel() {
    return new ServingModel() {
      @Override
      public float getFractionLoaded() {
        return 1.0f;
      }
      public Map<String,Integer> getWords() {
        return distinctOtherWords;
      }
    };
  }

}