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

package com.cloudera.oryx.ml.mllib.als;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import com.cloudera.oryx.lambda.QueueProducer;

final class EnqueueFeatureVecsFn implements VoidFunction<Tuple2<Integer,double[]>> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String whichMatrix;
  private final QueueProducer<String, String> modelUpdateQueue;

  EnqueueFeatureVecsFn(String whichMatrix, QueueProducer<String, String> modelUpdateQueue) {
    this.whichMatrix = whichMatrix;
    this.modelUpdateQueue = modelUpdateQueue;
  }

  @Override
  public void call(Tuple2<Integer,double[]> keyAndVector) throws IOException {
    Integer id = keyAndVector._1();
    double[] vector = keyAndVector._2();
    modelUpdateQueue.send("UP", MAPPER.writeValueAsString(Arrays.asList(whichMatrix, id, vector)));
  }

}
