/*
 * Copyright (c) 2014, Cloudera, Inc. and Intel Corp. All Rights Reserved.
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

package com.cloudera.oryx.ml.speed.als;

import kafka.consumer.ConsumerIterator;
import kafka.javaapi.producer.Producer;

import com.cloudera.oryx.lambda.model.SpeedModelManager;

public final class ALSSpeedModelManager implements SpeedModelManager<String,String> {

  @Override
  public void start(ConsumerIterator<String,String> inputIterator,
                    ConsumerIterator<String,String> updateIterator,
                    Producer<String,String> updateProducer) {

  }

  @Override
  public void close() {

  }

}
