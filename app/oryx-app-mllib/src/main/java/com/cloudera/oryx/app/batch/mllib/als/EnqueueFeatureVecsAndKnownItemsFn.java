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

import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import com.cloudera.oryx.api.TopicProducer;
import com.cloudera.oryx.common.text.TextUtils;

final class EnqueueFeatureVecsAndKnownItemsFn
    implements VoidFunction<Tuple2<String,Tuple2<double[],Collection<String>>>> {

  private final String whichMatrix;
  private final TopicProducer<String, String> modelUpdateTopic;

  EnqueueFeatureVecsAndKnownItemsFn(String whichMatrix,
                                    TopicProducer<String,String> modelUpdateTopic) {
    this.whichMatrix = whichMatrix;
    this.modelUpdateTopic = modelUpdateTopic;
  }

  @Override
  public void call(Tuple2<String,Tuple2<double[],Collection<String>>> keyAndVectorAndItems) {
    String id = keyAndVectorAndItems._1();
    Tuple2<double[],Collection<String>> vectorAndItems = keyAndVectorAndItems._2();
    double[] vector = vectorAndItems._1();
    Collection<String> knowItemIDs = vectorAndItems._2();
    modelUpdateTopic.send("UP", TextUtils.joinJSON(
        Arrays.asList(whichMatrix, id, vector, knowItemIDs)));
  }

}
