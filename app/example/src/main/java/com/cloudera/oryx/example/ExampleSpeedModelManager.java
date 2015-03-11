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
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;

import com.cloudera.oryx.lambda.KeyMessage;
import com.cloudera.oryx.lambda.speed.SpeedModelManager;

public final class ExampleSpeedModelManager implements SpeedModelManager<String,String,String> {

  @Override
  public void consume(Iterator<KeyMessage<String,String>> updateIterator) throws IOException {

  }

  @Override
  public void close() {

  }

  @Override
  public Iterable<String> buildUpdates(JavaPairRDD<String,String> newData) {
    return null;
  }

}