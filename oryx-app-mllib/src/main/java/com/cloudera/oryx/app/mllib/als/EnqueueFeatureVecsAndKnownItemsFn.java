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

package com.cloudera.oryx.app.mllib.als;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import com.cloudera.oryx.lambda.TopicProducer;

final class EnqueueFeatureVecsAndKnownItemsFn
    implements VoidFunction<Tuple2<Integer,Tuple2<double[],Collection<Integer>>>> {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String whichMatrix;
  private final TopicProducer<String, String> modelUpdateTopic;

  EnqueueFeatureVecsAndKnownItemsFn(String whichMatrix,
                                    TopicProducer<String,String> modelUpdateTopic) {
    this.whichMatrix = whichMatrix;
    this.modelUpdateTopic = modelUpdateTopic;
  }

  @Override
  public void call(Tuple2<Integer,Tuple2<double[],Collection<Integer>>> keyAndVectorAndItems)
      throws IOException {
    Integer id = keyAndVectorAndItems._1();
    Tuple2<double[],Collection<Integer>> vectorAndItems = keyAndVectorAndItems._2();
    double[] vector = vectorAndItems._1();
    Collection<Integer> knowItemIDs = vectorAndItems._2();
    Collection<String> knownItemIDsStrings = new ArrayList<>(knowItemIDs.size());
    for (Integer i : knowItemIDs) {
      knownItemIDsStrings.add(i.toString());
    }
    modelUpdateTopic.send("UP", MAPPER.writeValueAsString(
        Arrays.asList(whichMatrix, id, vector, knownItemIDsStrings)));
  }

}
