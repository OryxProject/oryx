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

package com.cloudera.oryx.app.batch.mllib.als;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.lambda.TopicProducerImpl;

final class EnqueueFeatureVecsAndKnownItemsFn
    implements VoidFunction<Iterator<Tuple2<String,Tuple2<float[],Collection<String>>>>> {

  private final String whichMatrix;
  private final String updateBroker;
  private final String topic;

  EnqueueFeatureVecsAndKnownItemsFn(String whichMatrix, String updateBroker, String topic) {
    this.whichMatrix = whichMatrix;
    this.updateBroker = updateBroker;
    this.topic = topic;
  }

  @Override
  public void call(Iterator<Tuple2<String,Tuple2<float[],Collection<String>>>> it) {
    if (it.hasNext()) {
      try (TopicProducer<String, String> producer = new TopicProducerImpl<>(updateBroker, topic, true)) {
        it.forEachRemaining(keyAndVectorAndItems -> {
          String id = keyAndVectorAndItems._1();
          Tuple2<float[],Collection<String>> vectorAndItems = keyAndVectorAndItems._2();
          float[] vector = vectorAndItems._1();
          Collection<String> knowItemIDs = vectorAndItems._2();
          Collection<?> data = knowItemIDs.isEmpty() ?
              Arrays.asList(whichMatrix, id, vector) :
              Arrays.asList(whichMatrix, id, vector, knowItemIDs);
          producer.send("UP", TextUtils.joinJSON(data));
        });
      }
    }
  }

}
