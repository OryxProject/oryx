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
import java.util.Iterator;

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.common.text.TextUtils;
import com.cloudera.oryx.lambda.TopicProducerImpl;

final class EnqueueFeatureVecsFn implements VoidFunction<Iterator<Tuple2<String,float[]>>> {

  private final String whichMatrix;
  private final String updateBroker;
  private final String topic;

  EnqueueFeatureVecsFn(String whichMatrix, String updateBroker, String topic) {
    this.whichMatrix = whichMatrix;
    this.updateBroker = updateBroker;
    this.topic = topic;
  }

  @Override
  public void call(Iterator<Tuple2<String,float[]>> it) {
    if (it.hasNext()) {
      try (TopicProducer<String,String> producer = new TopicProducerImpl<>(updateBroker, topic, true)) {
        while (it.hasNext()) {
          Tuple2<String,float[]> keyAndVector = it.next();
          String id = keyAndVector._1();
          float[] vector = keyAndVector._2();
          producer.send("UP", TextUtils.joinJSON(Arrays.asList(whichMatrix, id, vector)));
        }
      }
    }
  }

}
